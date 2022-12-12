import BlockStore from './index.js'

const store = new BlockStore('/tmp/block-store')

await store.ready()

for (let i = 0; i < 500; i++) {
  const blk = await store.get(i)

  blk.buffer[0] = i
  blk.buffer[1] = i + 1

  blk.queueFlush()
}

await store.flush()
