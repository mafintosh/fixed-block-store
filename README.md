# fixed-block-store

Fast block store to access/update 4kb disk pages

```
npm install fixed-block-store
```

## Usage

``` js
const FixedBlockStore = require('fixed-block-store')

const store = new FixedBlockStore('/tmp/block-store')

const blk = await store.get(10) // get page 10

blk.buffer[0] = 2
blk.buffer[1] = 1

blk.queueFlush() // queue the block for disk write later
await blk.flush() // or flush it now

// when done with the block call close
blk.close()

// if queued, flush the block store at some point, flushes all queued blocks
await store.flush()
```

## License

MIT
