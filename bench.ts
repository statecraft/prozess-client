import {connect} from './client'

// console.log('argv', process.argv)

process.on('unhandledRejection', err => { throw err })

const hostname = process.argv[2] || 'localhost'
const key = process.argv[3] || 'asdf'
;(async () => {
  const client = await connect(9999, hostname)
  const v = await client.getVersion()
  console.log('connected at', v)

  let opts: {targetVersion?: number, conflictKeys?: string[]} = {
    conflictKeys: [key],
    targetVersion: v
  }

  // let lastPrint = Date.now()
  // let opCount = 0

  while (true) {
    const v = await client.send('hi', opts)
    opts.targetVersion = v + 1
    // opCount++

    // let now = Date.now()
    // if (lastPrint + 1000 < now) {
    //   console.log(opCount)
    //   lastPrint = now
    //   opCount = 0
    // }
  }
})()