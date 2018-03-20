import {connect} from './client'
import child_process = require('child_process')

process.on('unhandledRejection', err => { throw err })

const letters = 'abcdefghijklmnop'

const SPAWN = (+process.argv[3])|0 || 1
console.log('spawn', SPAWN)

const hostname = process.argv[2] || 'localhost'
const children = new Array(SPAWN).fill(0).map((_, i) => child_process.fork('./dist/bench', [hostname, letters[i]]))

;(async () => {
  const client = await connect(9999, hostname)
  let lastV = await client.getVersion()
  console.log('Monitor connected at', lastV)
  setInterval(async () => {
    const v = await client.getVersion()
    console.log(v - lastV)
    lastV = v
  }, 10000)
})()

// child.on('exit', () => {
//   console.log('child exited')
// })
