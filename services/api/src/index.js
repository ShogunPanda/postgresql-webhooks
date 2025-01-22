import createConnectionPool from '@databases/pg'
import sql from '@databases/sql'
import fastify from 'fastify'
import { messageCreationQuery, messageCreationSchema, queueCreationQuery, queueCreationSchema } from './data.js'

export function create () {
  const server = fastify({
    loggerInstance: globalThis.platformatic.logger.child({}, { level: globalThis.platformatic.logLevel })
  })

  // This accesses process.env.DATABASE_URL by default
  const db = createConnectionPool({ bigIntMode: 'bigint' })
  server.addHook('onClose', () => {
    return db.dispose()
  })

  server.addContentTypeParser('application/octet-stream', { parseAs: 'buffer' }, async (_, payload) => payload)

  server.get('/queues', async () => {
    return db.query(sql`SELECT * FROM queues ORDER BY id ASC`)
  })

  server.get('/messages', async () => {
    return db.query(sql`SELECT * FROM messages ORDER BY id ASC`)
  })

  server.route({
    method: 'POST',
    url: '/queues',
    async handler (request, reply) {
      const [row] = await db.query(queueCreationQuery(request.body))
      reply.code(201)
      return row
    },
    schema: {
      body: queueCreationSchema
    }
  })

  server.route({
    method: 'POST',
    url: '/messages',
    async handler (request, reply) {
      const queue = request.body.queue

      if (typeof queue === 'string') {
        const [found] = await db.query(sql`SELECT id FROM queues WHERE name=${queue}`)

        if (!found) {
          reply.code(400).send({ error: `Queue with name "${queue}" not found.` })
          return
        }

        request.body.queue = found.id
      }

      const [row] = await db.query(messageCreationQuery(request.body))
      reply.code(201)
      return row
    },
    schema: {
      body: messageCreationSchema
    }
  })

  return server
}
