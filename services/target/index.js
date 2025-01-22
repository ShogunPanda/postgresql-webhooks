import fastify from 'fastify'

export function create () {
  const server = fastify({
    loggerInstance: globalThis.platformatic.logger.child({}, { level: globalThis.platformatic.logLevel })
  })

  server.post('/first', async function (request, reply) {
    const { method, headers, body } = request
    request.log.info({ method, headers, body }, 'Received hook invocation on endpoint first')
    reply.code(204)
  })

  server.post('/second', async function (request, reply) {
    if (Math.random() < 0.5) {
      reply.code(429)
      return { error: 'Please try again later' }
    }

    const { method, headers, body } = request
    request.log.info({ method, headers, body }, 'Received hook invocation on endpoint second')
    reply.code(204)
  })

  return server
}
