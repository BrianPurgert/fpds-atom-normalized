import server from './dist/server/server.js'

export default {
  async fetch(request, env, ctx) {
    // Populate process.env with Cloudflare environment bindings
    for (const [key, value] of Object.entries(env)) {
      if (typeof value === 'string') {
        process.env[key] = value
      }
    }

    return server.fetch(request)
  },
}
