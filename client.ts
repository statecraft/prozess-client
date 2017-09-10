import * as net from 'net'
import * as msgpack from 'msgpack-lite'
import * as assert from 'assert'

// messages (in and out) are made up of:
// - Length (4 bytes)
// - Message itself, which is a msgpack object.

enum ServerMsgType {
	Hello = 0,
	Event = 1,
	EventConfirm = 2,
	Subscribe = 3,
	SubscribeEnd = 4,
}
enum ClientMsgType {
	Event = 1,
	Subscribe = 3,
}
enum SubFlags {
	Ongoing = 0x1
}

const DEFAULT_SUB_SIZE = 1024*1024

const msgToBytes = (type: ClientMsgType, msg: Object) => {
	const content = msgpack.encode(msg)
	const buf = new Buffer(4 + 1 + content.length)
	buf.writeUInt32LE(content.length + 1, 0)
	buf.writeUInt8(type, 4)
	content.copy(buf, 4 + 1)
	return buf
}

const readCursor = (buf: NodeBuffer) => ({
	n: 0,
	remain() { return buf.length - this.n },
	readByte() { return buf.readUInt8(this.n++) },
	readUInt16() {
		const x = buf.readUInt16LE(this.n)
		this.n += 2
		return x
	},
	readUInt32() {
		const x = buf.readUInt32LE(this.n)
		this.n += 4
		return x
	},
	readUInt64() {
		return this.readUInt32() + this.readUInt32() << 32
	},
	readBytes(n) {
		const bytes = buf.slice(this.n, this.n + n)
		this.n += n
		return bytes
	},

	readEvent(base_version: number) {
		// console.log(buf.slice(this.n))
		if (this.remain() < 4 + 4 + 2 + 1 + 1) return null
		const size = this.readUInt32()
		const crc32 = this.readUInt32()
		const batch_size = this.readUInt16()
		const proto_v = this.readByte()
		const flags = this.readByte()
		// console.log('readevent', this.remain(), size)
		if (this.remain() < size) return null
		const data = this.readBytes(size)
		return {
			version: base_version,
			size, crc32, batch_size, proto_v, flags, data
		}
	}
})

const doNothing = () => {}

export const connect = (callback) => {
	const client = net.connect(9999, 'localhost', () => {
		// console.log('connected')

		let eventstream_version = -1
		let subcb = null
		let resub = false
		const sendcbs = []

		function subscribeRaw(from: number, maxbytes: number) {
				const msg = msgToBytes(ClientMsgType.Subscribe, [from, maxbytes])
				console.log(`writing subscribe ${msg.length} bytes`, msg)
				client.write(msg)
		}

		const api = {
			source: null,
			onevent: null,

			subscribe(opts: {from?: number, maxbytes?: number} = {}, callback = doNothing) {
				const from = opts.from || 0
				resub = opts.maxbytes == null
				const maxbytes = opts.maxbytes || DEFAULT_SUB_SIZE
				subscribeRaw(from, maxbytes)
				subcb = callback
			},

			send(data: NodeBuffer, opts: {targetVersion?: number, conflictKeys?: string[]}, callback = doNothing) {
				// Array of version, conflict keys, data blob.
				// ['keya', 'keyb']
				const targetVersion = opts.targetVersion || 0
				const conflictKeys = opts.conflictKeys || []
				sendcbs.push(callback)
				const msg = msgToBytes(ClientMsgType.Event, [targetVersion, conflictKeys, data])
				// console.log(`writing event ${msg.length} bytes`, msg)
				client.write(msg)
			},

			close() {
				client.end()
			},
		}

		const onEvent = (evt) => {
			// console.log('got event', evt)
			api.onevent && api.onevent(evt)
		}

		const onUnsubscribe = () => {
			console.log('Subscription finished')
			if (resub) subscribeRaw(eventstream_version, DEFAULT_SUB_SIZE)
			eventstream_version = -1
		}

		let buf: NodeBuffer = null
		const tryReadOne = () => {
			if (buf == null) return false

			const cursor = readCursor(buf)

			// First try to read the header.
			if (buf.length < 1) return false
			const msgtype: ServerMsgType = cursor.readByte()

			switch (msgtype) {
				case ServerMsgType.Hello: {
					if (cursor.remain() < 1 + 8) return false
					const proto_v = cursor.readByte()
					api.source = cursor.readBytes(8).toString('ascii')
					callback(null, api)
					break
				}
				case ServerMsgType.Event: {
					const evt = cursor.readEvent(eventstream_version)
					if (evt == null) return false
					eventstream_version += evt.batch_size
					onEvent(evt)
					break
				}
				case ServerMsgType.EventConfirm: {
					if (cursor.remain() < 1 + 8) return false
					const proto_v = cursor.readByte()
					const version = cursor.readUInt64()
					const cb = sendcbs.shift()
					cb(null, version)
					break
				}
				case ServerMsgType.Subscribe: {
					if (cursor.remain() < 1 + 8 + 8 + 1) return false
					const proto_v = cursor.readByte()
					const v_start = cursor.readUInt64()
					const size = cursor.readUInt64()
					const flags = cursor.readByte()

					// TODO: Could start processing messages without waiting for entire catchup.
					if (cursor.remain() < size) return false

					// console.log('remain', cursor.remain(), 'size', size, buf.slice(cursor.n))
					const start = cursor.n

					eventstream_version = v_start

					const events = []
					while (cursor.n - start < size) {
						// console.log('n', cursor.n, 'start', start, 'size', size)
						const evt = cursor.readEvent(eventstream_version)
						assert(evt)
						events.push(evt)
						eventstream_version += evt.batch_size
						onEvent(evt)
					}

					// OK!
					if (subcb) subcb(null, {v_start, size, flags, events})
					subcb = null

					if (!(flags & SubFlags.Ongoing)) onUnsubscribe()
					break
				}
				case ServerMsgType.SubscribeEnd: {
					if (cursor.remain() < 1) return false
					const proto_v = cursor.readByte()
					onUnsubscribe()
					break
				}
				default: throw Error('Invalid message type ' + msgtype)
			}

			buf = (cursor.n === buf.length) ? null : buf.slice(cursor.n)
			return true
		}
		client.on('data', (data: NodeBuffer) => {
			// console.log('read data of length', data.length, data)
			buf = (buf == null) ? data : Buffer.concat([buf, data])
			while (1) { if (!tryReadOne()) break }
		})

	})
}