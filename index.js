import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads'
import { fileURLToPath } from 'node:url'
import hash from 'hash.js'


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
  const view = new Uint32Array(ab)
  for (let i = 0; i < size; i = (i^2 + 7057) % 256) view[i] = i % 256 // Fill with some data
  return ab
}

if (isMainThread) {
  // Main thread
  const worker = new Worker(__filename, { workerData: { IS_CHUNKING } })

  // Create a large ArrayBuffer and send it to the worker
  console.log('Creating large ArrayBuffer...')
  let payload = createLargeArrayBuffer(FIVE_GB)

  var startingHash = hash.sha1().update(payload).digest('hex')
  console.log('Starting hash', startingHash)
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

  for (const [i, chunk] of payload.entries()) {
    worker.postMessage({ n: i, m: payload.length, chunk })
  }

  const chunks = []

  // Receive the processed ArrayBuffer from the worker
  worker.on('message', ({ n, m, chunk }) => {
    console.log('Received chunk back from worker', n, m)
    chunks.push(chunk)

    if (n === m - 1) {
      console.log('Received all!')

      const res = concatArrayBuffers(chunks)
      var endingHash = hash.sha1().update(res).digest('hex')
      console.log('Ending hash', endingHash)
    }
  })
} else {
  // Worker thread
  console.log({ workerData })
  const chunks = []
  parentPort.on('message', ({ n, m, chunk }) => {
    console.log('Worker received chunk', n, m)
    console.log({ workerData, inside: true })
    chunks.push(chunk)
    /**
     * time and concat
     * then chunk again to send back to main thread
     */
    if (chunks.length === m) {
      console.log('Returning all chunks...')
      for (const [i, chunk] of chunks.entries()) {
        parentPort.postMessage({ n: i, m: chunks.length, chunk })
      }
    }
  })
}
