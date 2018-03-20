import {connect} from './client'

// console.log(process.argv)
connect(9999, 'localhost').then(client => {
  process.stdin.on('data', (data) => {
    console.log(`Creating message '${data.toString('utf8')}'`)
    client.sendRaw(Buffer.from(data), {targetVersion:1, conflictKeys:['a']}, (err, v) => {
      if (err) console.log(err.name)
      console.log('SUCCESS _ CALLBACK CALLED', !!err)
      if (err) throw err
      console.log('Event recieved at version', v)
    })
  })
  process.stdin.on('end', () => {
    client.close()
  })
})
