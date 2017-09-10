import {connect} from './client'

connect((err, client) => {
	console.log('connected', client.source)
	if (err) throw err

	client.onevent = (event) => {
		console.log('event version', event.version)
		process.stdout.write(event.data.toString('utf8'))
	}
	client.subscribe({from:-1}, (err, response) => {
		// console.log('sub response', response)
		// console.log(response.data.toString('utf8'))
	})
})
