import {connect} from './client'

// console.log(process.argv)
connect((err, client) => {
	if (err) throw err

	process.stdin.on('data', (data) => {
		console.log(`Creating message '${data.toString('utf8')}'`)
		client.send(Buffer.from(data), {}, (err, v) => {
			if (err) throw err
			console.log('Event recieved at version', v)
		})
	})
	process.stdin.on('end', () => {
		client.close()
	})
})
