import sql from '@databases/sql'

export const queueCreationSchema = {
  type: 'object',
  properties: {
    name: { type: 'string' },
    url: { type: 'string' },
    method: { type: 'string', enum: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'], default: 'GET' },
    headers: { type: 'object' },
    maxRetries: { type: 'number', default: 3, minimum: 0, maximum: 10 }
  },
  required: ['name', 'url']
}

export const messageCreationSchema = {
  type: 'object',
  properties: {
    queue: { anyOf: [{ type: 'string' }, { type: 'number' }] },
    headers: { type: 'object' },
    payload: true
  },
  required: ['queue', 'payload']
}

export function queueCreationQuery (body) {
  const { name, url, method, headers, maxRetries } = body

  return sql`
    INSERT INTO queues 
      (name, url, method, headers, max_retries) 
    VALUES 
      (${name}, ${url}, ${method}, ${headers ?? null}, ${maxRetries})
    RETURNING id;
  `
}

export function messageCreationQuery (body) {
  let { queue, headers, payload } = body

  if (!Buffer.isBuffer(payload)) {
    if (typeof payload === 'object') {
      payload = JSON.stringify(payload)
    } else {
      payload = payload.toString()
    }
  }

  return sql`
    INSERT INTO pending_messages
      (queue_id, headers, payload)
    VALUES 
      (${queue}, ${headers ?? null}, ${payload})
    RETURNING id;
  `
}
