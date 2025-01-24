import sql from '@databases/sql'
import parser from 'cron-parser'

export function ensureLoggableError (error) {
  Reflect.defineProperty(error, 'message', { enumerable: true })

  if ('code' in error) {
    Reflect.defineProperty(error, 'code', { enumerable: true })
  }

  if ('stack' in error) {
    Reflect.defineProperty(error, 'stack', { enumerable: true })
  }

  return error
}

export function becomeLeaderQuery () {
  return sql`SELECT pg_try_advisory_lock(0) as lock`
}

export function resignAsLeaderQuery () {
  return sql`SELECT pg_advisory_unlock(0) as unlock`
}

export function getNextPendingMessageQuery () {
  return sql`
    SELECT 
      messages.id, messages.queue_id as queue, messages.headers as message_headers, 
      messages.payload, messages.retries, messages.schedule,
      queues.url, queues.method, queues.headers as queue_headers, queues.max_retries
    FROM 
      pending_messages as messages
    INNER JOIN 
      queues ON messages.queue_id = queues.id
    WHERE 
      execute_at < NOW()
    ORDER BY
      messages.execute_at ASC
    LIMIT 1
  `
}

export function markMessageAsProcessingQuery (id) {
  return sql`
    UPDATE pending_messages 
    SET execute_at = NULL
    WHERE id = ${id}
  `
}

export function markMessageAsToRetryQuery (message, timestamp) {
  return sql`
    UPDATE pending_messages 
    SET retries = retries + 1, execute_at = ${timestamp} 
    WHERE id = ${message.id}
  `
}

export function markMessageAsFailedQuery (message, error) {
  const serializedError = JSON.stringify(ensureLoggableError(error), null, 2)

  return sql`
    INSERT INTO failed_messages 
      (id, queue_id, headers, payload, retries, error)
    VALUES
      (${message.id}, ${message.queue}, ${message.message_headers}, NULL, ${message.retries}, ${serializedError})    
  `
}

export function markMessageAsCompletedQuery (message, response) {
  const serializedResponse = JSON.stringify(response, null, 2)

  return sql`
    INSERT INTO completed_messages 
      (id, queue_id, headers, payload, retries, response)
    VALUES
      (${message.id}, ${message.queue}, ${message.message_headers}, NULL, ${message.retries}, ${serializedResponse})    
  `
}

export function rescheduleMessageQuery (message) {
  return sql`
    UPDATE pending_messages
    SET retries=0, execute_at = ${parser.parseExpression(message.schedule).next()}
    WHERE id=${message.id}
  `
}

export function deletePendingMessageQuery (id) {
  return sql`DELETE FROM pending_messages WHERE id = ${id}`
}
