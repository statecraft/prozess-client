// import {EventEmitter} from 'events'
import * as net from 'net'
import * as assert from 'assert'
import msgpack = require('msgpack-lite')
import errno = require('errno')

export const VersionConflictError = errno.custom.createError('VersionConflict')
export const ClosedBeforeConfirmError = errno.custom.createError('ClosedBeforeConfirm')

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
  Oneshot = 0x1, // On subscribe request

  Complete = 0x1, // On subscribe response
}

// We only support 32bit versions here. More details below.
const VERSION_CONFLICT = 0xffffffff

const DEFAULT_SUB_SIZE = 1024*1024

export interface Event {
  version: number,
  size: number,
  crc32?: number,
  batch_size: number,
  flags: number,
  data: NodeBuffer
}

type SubCb = (err: Error | null, data: {v_start: number, size: number, complete: boolean, events: Event[]}) => void
type SendCb = (err: Error | null, version?: number) => void

export interface KApi /*extends EventEmitter*/ {
  source?: string
  onevents?(evt: Event[], resultingVersion: number): void
  onclose?(): void
  onunsubscribe?(): void
  subscribe(opts: {from?: number, maxbytes?: number, oneshot?: boolean},
    callback?: (err: Error | null, data: {v_start: number, size: number, complete: boolean, events: Event[]}) => void): void
  send(data: NodeBuffer | string, opts: {targetVersion?: number, conflictKeys?: string[]},
    callback?: (err: Error | null, version?: number) => void): void
  close(): void
}


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
    // For now we're going to ignore the top 32 bits of the version number,
    // leaving us with just 4bn events max. Numbers in JS can go up to 2^51
    // but the shift operator only uses int32 operands.
    const low = this.readUInt32()
    /*const high = */this.readUInt32()
    return low
  },
  readBytes(n: number) {
    const bytes = buf.slice(this.n, this.n + n)
    this.n += n
    return bytes
  },

  readEvent(base_version: number): Event | null {
    // console.log(buf.slice(this.n))
    if (this.remain() < 4 + 4 + 2 + 1 + 1) return null
    const size = this.readUInt32()
    const crc32 = this.readUInt32()
    const batch_size = this.readUInt16()
    const proto_v = this.readByte()
    assert(proto_v === 0)
    const flags = this.readByte()
    // console.log('readevent', this.remain(), size)
    if (this.remain() < size) return null
    const data = this.readBytes(size)
    return {
      version: base_version,
      size, crc32, batch_size, flags, data
    }
  }
})

const doNothing = () => {}

const connect = (port: number, hostname: string, callback: (err: Error | null, api: KApi | null) => void) => {
  const errListener = (err: Error | null) => { callback(err, null) }

  const client = net.connect(port, hostname, () => {
    client.removeListener('error', errListener)

    let eventstream_version = -1
    let subcb: SubCb | null = null
    let resub = false
    const sendcbs: SendCb[] = []

    function subscribeRaw(flags: number, from: number, maxbytes: number) {
        const msg = msgToBytes(ClientMsgType.Subscribe, [flags, from, maxbytes])
        console.log(`writing subscribe ${msg.length} bytes`, msg)
        client.write(msg)
    }

    const api: KApi = {
      source: undefined,
      onevents: undefined,
      onclose: undefined,
      onunsubscribe: undefined,

      subscribe(opts: {from?: number, maxbytes?: number, oneshot?: boolean} = {}, callback = doNothing) {
        const from = opts.from || 0
        resub = opts.maxbytes == null && !opts.oneshot
        const maxbytes = opts.maxbytes || DEFAULT_SUB_SIZE
        const flags = opts.oneshot ? SubFlags.Oneshot : 0
        subscribeRaw(flags, from, maxbytes)
        subcb = callback
      },

      send(data: NodeBuffer | string, opts: {targetVersion?: number, conflictKeys?: string[]}, callback = doNothing) {
        // Array of version, conflict keys, data blob.
        // ['keya', 'keyb']
        const targetVersion = opts.targetVersion || 0
        const conflictKeys = opts.conflictKeys || []
        sendcbs.push(callback)

        // TODO: Might make sense for the server to just support strings directly.
        const dataBuffer: Buffer = Buffer.isBuffer(data) ? data : Buffer.from(data)
        const msg = msgToBytes(ClientMsgType.Event, [targetVersion, conflictKeys, dataBuffer])
        // console.log(`writing event ${msg.length} bytes`, msg)
        client.write(msg)
      },

      close() {
        client.end()
      },
    }

    const onEvents = (evts: Event[], resultingVersion: number) => {
      // console.log('got event', evt)
      api.onevents && api.onevents(evts, resultingVersion)
    }

    const onUnsubscribe = () => {
      console.log('Subscription finished')
      eventstream_version = -1
      if (resub) subscribeRaw(0, eventstream_version, DEFAULT_SUB_SIZE)
      else api.onunsubscribe && api.onunsubscribe()
    }

    let connected = true
    const onClose = () => {
      if (connected) {
        connected = false
        api.onclose && api.onclose()
      }
      sendcbs.forEach(cb => {
        cb(new ClosedBeforeConfirmError('Connection closed before server confirmed event'))
      })
      sendcbs.length = 0
    }
    client.on('close', onClose)
    client.on('end', onClose)
    // onclose gets called after this anyway, but we need to eat the error.
    client.on('error', onClose)

    let buf: NodeBuffer | null = null
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
          onEvents([evt], eventstream_version)
          break
        }
        case ServerMsgType.EventConfirm: {
          if (cursor.remain() < 1 + 8) return false
          const proto_v = cursor.readByte()
          const version = cursor.readUInt64()
          const cb = sendcbs.shift()!
          if (version === VERSION_CONFLICT) cb(new VersionConflictError('Server rejected event due to version conflict'))
          else cb(null, version)
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

          const events: Event[] = []
          while (cursor.n - start < size) {
            // console.log('n', cursor.n, 'start', start, 'size', size)
            const evt = cursor.readEvent(eventstream_version)
            assert(evt)
            events.push(evt!)
            eventstream_version += evt!.batch_size
          }
          onEvents(events, eventstream_version)

          // OK!
          const complete = !!(flags & SubFlags.Complete)
          if (subcb) subcb(null, {v_start, size, complete, events})
          subcb = null

          if (complete) onUnsubscribe()
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

  client.on('error', errListener)
}

export {connect}
