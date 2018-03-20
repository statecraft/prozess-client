import {connect} from './client'

connect(9999, 'localhost').then(client => {
  console.log('connected', client.source)

  client.onevents = (events) => {
    console.log('got', events.length, 'events')
    events.forEach(event => {
      console.log('event version', event.version)
      //process.stdout.write(event.data.toString('utf8'))
    })
  }

  client.subscribe(2, {}, (err, response) => { // from:-1 should subscribe raw.
    if (err) throw err
    console.log('sub response', response)
    // console.log(response.data.toString('utf8'))
    // if (response.oneshot) client!.close()
  })
  client.onunsubscribe = () => {
    console.log('unsub!')
    client!.close()
  }

  // client.getEvents(2, -1, {}, (err, response) => {
  //   if (err) throw err
  //   console.log('oneshot response', response)
  //   client!.close()
  // })

})
