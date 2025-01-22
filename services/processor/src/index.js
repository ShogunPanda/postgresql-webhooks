import createConnectionPool from '@databases/pg'
import { scheduler } from 'node:timers/promises'
import {
  becomeLeaderQuery,
  deletePendingMessageQuery,
  getNextPendingMessageQuery,
  markMessageAsCompletedQuery,
  markMessageAsFailedQuery,
  markMessageAsProcessingQuery,
  markMessageAsToRetryQuery,
  resignAsLeaderQuery
} from './data.js'

const logger = globalThis.platformatic.logger.child({}, { level: globalThis.platformatic.logLevel })

function ensureLoggableError (error) {
  Reflect.defineProperty(error, 'message', { enumerable: true })

  if ('code' in error) {
    Reflect.defineProperty(error, 'code', { enumerable: true })
  }

  if ('stack' in error) {
    Reflect.defineProperty(error, 'stack', { enumerable: true })
  }

  return error
}

function exponentialBackoff (attempt) {
  return Math.pow(2, attempt) * 1000
}

async function runAsLeader (db, task) {
  return db.task(async leaderConnection => {
    const [result] = await leaderConnection.query(becomeLeaderQuery())

    if (!result.lock) {
      return
    }

    try {
      await task()
    } catch (e) {
      logger.error({ error: ensureLoggableError(e) }, 'AA')
    } finally {
      await leaderConnection.query(resignAsLeaderQuery())
    }
  })
}

async function handleMessageSuccess (db, message, response) {
  try {
    await db.tx(async db => {
      await db.query(markMessageAsCompletedQuery(message, response))
      await db.query(deletePendingMessageQuery(message.id))
    })
  } catch (e) {
    logger.fatal({ error: ensureLoggableError(e), id: message.id }, 'Error while marking a message as completed.')
  }
}

async function handleMessageFailure (db, message, error) {
  if (message.retries < message.max_retries) {
    const timestamp = new Date(Date.now() + exponentialBackoff(message.retries + 1)).toISOString()
    try {
      await db.query(markMessageAsToRetryQuery(message, timestamp))
    } catch (e) {
      logger.fatal({ error: ensureLoggableError(e), id: message.id }, 'Error while marking a message as to retry.')
    }
  } else {
    try {
      await db.tx(async db => {
        await db.query(markMessageAsFailedQuery(message, error))
        await db.query(deletePendingMessageQuery(message.id))
      })
    } catch (e) {
      logger.fatal({ error: ensureLoggableError(e), id: message.id }, 'Error while marking a message as failed.')
    }
  }
}

async function invokeHook (message, db) {
  try {
    const response = await fetch(message.url, {
      method: message.method,
      headers: { 'content-type': 'application/octet-stream', ...message.queue_headers, ...message.message_headers },
      body: message.payload
    })

    const responsePayload = {
      status: response.status,
      headers: Object.fromEntries(response.headers.entries()),
      body: await response.text()
    }

    if (response.ok) {
      await handleMessageSuccess(db, message, responsePayload)
    } else {
      await handleMessageFailure(db, message, responsePayload)
    }
  } catch (e) {
    await handleMessageFailure(db, message, e)
  }
}

async function processJobs (db, abortSignal) {
  while (!abortSignal.aborted) {
    let message
    try {
      const pending = await db.query(getNextPendingMessageQuery())
      message = pending[0]

      if (message) {
        await db.query(markMessageAsProcessingQuery(message.id))
      }
    } catch (e) {
      logger.error({ error: ensureLoggableError(e) }, 'Error while fetching pending messages. Giving up leadership.')
      return
    }

    // No messages, wait for a while then continue
    if (!message) {
      await scheduler.wait(100)
      continue
    }

    // Call the target, a non 2xx will be considered a failure
    await invokeHook(db, message)
  }
}

async function main () {
  // This accesses process.env.DATABASE_URL by default
  const db = createConnectionPool({ bigIntMode: 'bigint' })

  globalThis.platformatic.events.on('stop', async () => {
    logger.info('Received stop event. Stopping processing jobs...')
    abortController.abort()
  })

  const abortController = new AbortController()

  while (!abortController.signal.aborted) {
    await runAsLeader(db, () => {
      logger.info('Successfully elected as leader. Starting processing jobs...')
      return processJobs(db, abortController.signal)
    })

    await scheduler.wait(1000)
  }

  await db.dispose()
}

export function build () {
  main().catch(error => {
    logger.error({ error: ensureLoggableError(error) }, 'Error while electing the leader')
  })

  return {}
}
