const fs = require('fs')
const b4a = require('b4a')
const ReadyResource = require('ready-resource')

const MAX_BATCH_SIZE = 131072

class FixedBlock {
  constructor (store, index) {
    this.store = store
    this.index = index

    this.userData = null
    this.buffer = null
    this.loading = null
    this.refs = 1

    this.idle = 0
    this.set = null

    this.queued = false
  }

  queueFlush () {
    if (this.queued === true) return
    this.queued = true
    this.store.queued.push(this)
  }

  flush () {
    this.queueFlush()
    return this.store.flush()
  }

  _open () {
    if (this.refs++ === 0) {
      const top = this.set.pop()
      if (top !== this) this.set[top.idle = this.idle] = top
    }
  }

  close () {
    if (--this.refs === 0) {
      this.set = this.store.idle
      this.idle = this.set.push(this) - 1
      this.store._maybeGC()
    }
  }
}

module.exports = class FixedBlockStore extends ReadyResource {
  constructor (filename, { maxCache = 128, readBatch = 1 } = {}) {
    super()

    this.filename = filename
    this.fd = 0
    this.maxCache = maxCache
    this.readBatch = readBatch
    this.blocks = new Map()
    this.idle = []
    this.gc = []
    this.queued = []
    this.flushing = null
  }

  get active () {
    return this.blocks.size - this.idle.length - this.gc.length
  }

  _maybeGC () {
    if (this.gc.length === 0 && this.idle.length > (this.maxCache >> 1)) {
      const tmp = this.gc
      this.gc = this.idle
      this.idle = tmp
      return
    }

    if (this.gc.length + this.idle.length < this.maxCache) return

    // just pick anyone from the gc set that is not queued for flushing
    // we could maintain a separate set for ONLY non queued but seems like overkill
    for (let i = 0; i < this.gc.length; i++) {
      const gc = this.gc[i]
      if (gc.queued === true) continue

      const top = this.gc.pop()
      if (gc !== top) this.gc[top.idle = gc.idle] = top

      this.blocks.delete(gc.index)
      break
    }
  }

  _open () {
    return new Promise((resolve, reject) => {
      fs.open(this.filename, fs.constants.O_RDWR | fs.constants.O_CREAT, (err, fd) => {
        if (err) return reject(err)
        this.fd = fd
        resolve()
      })
    })
  }

  _close () {
    return new Promise((resolve, reject) => {
      if (this.fd === 0) return resolve()
      fs.close(this.fd, (err) => {
        if (err) return reject(err)
        this.fd = 0
        resolve()
      })
    })
  }

  _read (index, batch) {
    return new Promise((resolve, reject) => {
      const fd = this.fd
      const blocks = new Array(batch)

      let bytesMissing = batch * 4096
      let pos = index * 4096

      for (let i = 0; i < blocks.length; i++) {
        blocks[i] = b4a.allocUnsafe(4096)
      }

      fs.readv(fd, blocks, pos, onread)

      function onread (err, bytesRead, buffers) {
        if (err) return reject(err)

        if (bytesRead === 0) {
          for (const buffer of buffers) {
            b4a.fill(buffer, 0)
            bytesMissing -= buffer.byteLength
          }
        } else {
          bytesMissing -= bytesRead
          pos += bytesRead
        }

        if (bytesMissing === 0) {
          return resolve(blocks)
        }

        fs.readv(fd, sliceBlocks(buffers, bytesMissing), pos, onread)
      }
    })
  }

  _writev (pos, blocks, cb) {
    const fd = this.fd

    let bytesMissing = blocks.length * 4096

    fs.writev(fd, blocks, pos, onwrite)

    function onwrite (err, bytesWritten, blocks) {
      if (err) return cb(err)
      if (bytesWritten === bytesMissing) return cb(null)

      bytesMissing -= bytesWritten
      pos += bytesWritten

      fs.writev(fd, sliceBlocks(blocks, bytesMissing), pos, onwrite)
    }
  }

  _flush (resolve, reject) {
    this.queued.sort(cmp)

    const first = this.queued[0]
    first.queued = false

    let error = null
    let writing = 0
    let index = first.index
    let batch = [first.buffer]

    const ondone = (err) => {
      if (err) error = err

      if (--writing > 0) return

      this.flushing = null

      if (error) reject(error)
      else resolve()
    }

    for (let i = 1; i < this.queued.length; i++) {
      const q = this.queued[i]
      q.queued = false

      if (batch.length < MAX_BATCH_SIZE && q.index === index + batch.length) {
        batch.push(q.buffer)
        continue
      }

      writing++
      this._writev(index * 4096, batch, ondone)

      index = q.index
      batch = [q.buffer]
    }

    if (batch.length > 0) {
      writing++
      this._writev(index * 4096, batch, ondone)
    }

    this.queued = []
  }

  async info () {
    if (this.opened === false) await this.ready()

    return new Promise((resolve, reject) => {
      fs.fstat(this.fd, function (err, st) {
        if (err) return reject(err)
        resolve({ blocks: Math.floor(st.size / 4096) })
      })
    })
  }

  async flush () {
    if (this.opened === false) await this.ready()

    while (this.flushing !== null) await this.flushing
    if (this.queued.length === 0) return

    this.flushing = new Promise((resolve, reject) => {
      this._flush(resolve, reject)
    })
  }

  async get (index) {
    if (this.opened === false) await this.ready()

    const blk = this.blocks.get(index)

    if (blk) {
      while (blk.loading !== null) await blk.loading
      blk._open()
      return blk
    }

    const batchSize = this.readBatch
    const start = index - (index & (batchSize - 1))
    const end = index + batchSize

    const newBlk = new FixedBlock(this, index)
    const batch = [newBlk]

    this._maybeGC()

    for (let i = index - 1; i >= start; i--) {
      if (this.blocks.has(i)) break
      const blk = new FixedBlock(this, i)
      blk.close()
      batch.push(blk)
    }

    batch.reverse()

    for (let i = index + 1; i < end; i++) {
      if (this.blocks.has(i)) break
      const blk = new FixedBlock(this, i)
      blk.close()
      batch.push(blk)
    }

    const loading = this._read(batch[0].index, batch.length)

    for (let i = 0; i < batch.length; i++) {
      const blk = batch[i]

      blk.loading = loading

      this.blocks.set(blk.index, blk)
    }

    const buffers = await loading

    for (let i = 0; i < batch.length; i++) {
      const blk = batch[i]

      blk.loading = null
      blk.buffer = buffers[i]
    }

    return newBlk
  }
}

function cmp (a, b) {
  return a.index - b.index
}

function sliceBlocks (blocks, bytesMissing) {
  const b = []

  let i = blocks.length - 1

  while (bytesMissing > 0 && i >= 0) {
    let next = blocks[i--]
    if (next.byteLength > bytesMissing) {
      next = next.subarray(next.byteLength - bytesMissing, next.byteLength)
    }

    bytesMissing -= next.byteLength
    b.push(next)
  }

  return b.reverse()
}
