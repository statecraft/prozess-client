import {connect} from './client'

connect(9999, 'localhost', (err, client) => {
  if (err) throw err
  client = client!

  console.log('connected', client.source)

  client.onevents = (events) => {
    console.log('got', events.length, 'events')
    events.forEach(event => {
      console.log('event version', event.version)
      process.stdout.write(event.data.toString('utf8'))
    })
  }
  client.subscribe({from:5, oneshot: true}, (err, response) => { // from:-1 should subscribe raw.
    if (err) throw err
    console.log('sub response', response)
    // console.log(response.data.toString('utf8'))
  })
  client.onunsubscribe = () => client!.close()
})
