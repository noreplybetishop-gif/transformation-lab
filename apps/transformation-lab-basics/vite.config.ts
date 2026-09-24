import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import type { IncomingMessage, ServerResponse } from 'node:http'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

function wheelhousePlugin(): Plugin {
  return {
    name: 'wheelhouse-static',
    configureServer(server) {
      server.middlewares.use((req: IncomingMessage, res: ServerResponse, next: () => void) => {
        const url = req.url?.split('?')[0] || ''
        if (url.startsWith('/wheelhouse/')) {
          const rel = url.slice('/wheelhouse/'.length)
          const searchPaths = [
            path.resolve(__dirname, 'public/wheelhouse', rel),
            path.resolve(__dirname, '../../packages/dbt-wasm-engine/wheelhouse', rel),
          ]
          for (const filePath of searchPaths) {
            if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
              const ext = path.extname(filePath)
              const mime = ext === '.json' ? 'application/json' : 'application/octet-stream'
              res.setHeader('Content-Type', mime)
              res.setHeader('Cache-Control', 'no-cache')
              return fs.createReadStream(filePath).pipe(res)
            }
          }
        }
        next()
      })
    },
  }
}

export default defineConfig({
  plugins: [react(), tailwindcss(), wheelhousePlugin()],
  server: {
    host: '0.0.0.0',
    port: 3000,
    allowedHosts: true,
  },
  // @dbt-wasm/engine spawns a module Web Worker via `new Worker(new URL('./worker.js', import.meta.url))`.
  // Excluding it from esbuild pre-bundling keeps it as source so Vite's worker pipeline handles the
  // worker chunk (and its CDN dynamic import) correctly.
  optimizeDeps: { exclude: ['@dbt-wasm/engine'] },
})
