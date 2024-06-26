import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)

/**
 * There is a bug in Node which causes Node to produce
 * an error if sending an ArrayBuffer >4GB to another thread.
 *
 * Set IS_CHUNKING to false to reproduce the error
 *
 * Set IS_CHUNKING to true to instead chunk
 * the ArrayBuffer and cicumvent the bug
 */

const FIVE_GB = 5 * 1024 * 1024 * 1024
const TWO_GB = 2 * 1024 * 1024 * 1024

const IS_CHUNKING = true

export function splitArrayBuffer (ab, chunkSize) {
  const chunks = []
  const totalBytes = ab.byteLength

  for (let i = 0; i < totalBytes; i += chunkSize) {
    const end = Math.min(i + chunkSize, totalBytes)
    chunks.push(ab.slice(i, end))
    process.stdout.write('.')
  }
  process.stdout.write('*')
  return chunks
}

export function concatArrayBuffers (abs) {
  const totalLength = abs.reduce((sum, ab) => sum + ab.byteLength, 0)

  const newGuy = new ArrayBuffer(totalLength)
  const view = new Uint8Array(newGuy)

  let offset = 0
  abs.forEach((cur) => {
    view.set(new Uint8Array(cur), offset)
    offset += cur.byteLength
  })

  return newGuy
}

export function createLargeArrayBuffer (size) {
  const ab = new ArrayBuffer(size)
  const view = new Uint8Array(ab)
  for (let i = 0; i < size; i++) view[i] = i % 256 // Fill with some data
  return ab
}

if (isMainThread) {
  // Main thread
  const worker = new Worker(__filename, { workerData: { IS_CHUNKING } })

  // Create a large ArrayBuffer and send it to the worker
  console.log('Creating large ArrayBuffer...')
  let payload = createLargeArrayBuffer(FIVE_GB)

  /**
   * time and chunk
   */
  if (IS_CHUNKING) {
    console.log('Chunking...')
    console.time('split')
    payload = splitArrayBuffer(payload, TWO_GB)
    console.timeEnd('split')
  }

  console.log('Sending payload to worker...')
  worker.postMessage(payload)

  // Receive the processed ArrayBuffer from the worker
  worker.on('message', (res) => {
    console.log('Received message back from worker', res)
    console.log('DONE')
  })
} else {
  // Worker thread
  console.log({ workerData })
  parentPort.on('message', (payload) => {
    console.log('Worker received payload')
    console.log({ workerData, inside: true })
    /**
     * time and concat
     * then chunk again to send back to main thread
     */
    if (workerData.IS_CHUNKING) {
      console.log('Concatenating then Re-Chunking...')
      console.time('concat')
      payload = concatArrayBuffers(payload)
      console.timeEnd('concat')
      payload = splitArrayBuffer(payload, TWO_GB)
    }

    parentPort.postMessage(payload)
  })
}
