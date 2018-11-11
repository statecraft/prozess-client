// import {EventEmitter} from 'events'
import * as net from 'net'
import * as assert from 'assert'
import msgpack = require('msgpack-lite')
import errno = require('errno')

export type Callback<T> = (err: Error | undefined | null, results?: T) => void

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
  Oneshot = 0x1, // Shared between request and response

  // A complete response means we don't have an active subscription
  Complete = 0x2,

  // Current flags that we're up to date according to the prozess server.
  Current = 0x4,
}

// We only support 32bit versions here. More details below.
const VERSION_CONFLICT = 0xffffffff

const DEFAULT_SUB_SIZE = 1024*1024

export interface Event {
  version: number,
  crc32?: number,
  batch_size: number,
  flags: number,
  data: Buffer
}

export type SubCbData = {
  v_start: number,
  v_end: number,
  size: number,
  oneshot: boolean,
  complete: boolean,
  current: boolean,
  events: Event[]
}
export type SubCb = Callback<SubCbData>
type SendCb = (err: Error | null, version?: number) => void

export type EventListener = (events: Event[], resultingVersion: number) => void
export interface PClient /*extends EventEmitter?*/ {
  source?: string
  onevents?: EventListener
  onclose?(): void
  onunsubscribe?(): void
  subscribe(from?: number, opts?: {maxbytes?: number}, callback?: SubCb): void
  getEventsRaw(from: number, to: number, opts?: {maxbytes?: number}, callback?: SubCb): void
  getEvents(from: number, to: number, opts?: {maxbytes?: number}): Promise<SubCbData>
  sendRaw(data: Buffer | string, opts: {targetVersion?: number, conflictKeys?: string[]}, callback?: Callback<number>): void
  send(data: Buffer | string, opts?: {targetVersion?: number, conflictKeys?: string[]}): Promise<number>
  getVersion(): Promise<number>
  close(): void
}

const mergeSubCbData = (a: SubCbData | null, b: SubCbData): SubCbData => {
  if (a == null) a = b
  else {
    a.v_end = b.v_end
    a.size += b.size
    a.complete = b.complete
    a.current = a.current || b.current
    a.events = a.events.concat(b.events)
  }
  return a
}

const msgToBytes = (type: ClientMsgType, msg: Object) => {
  const content = msgpack.encode(msg)
  const buf = Buffer.alloc(4 + 1 + content.length)
  buf.writeUInt32LE(content.length + 1, 0)
  buf.writeUInt8(type, 4)
  content.copy(buf, 4 + 1)
  return buf
}

const readCursor = (buf: Buffer) => ({
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
      crc32, batch_size, flags, data
    }
  }
})

const doNothing = () => {}

function getEvents(this: PClient, from: number, to: number, opts?: {maxbytes?: number}): Promise<SubCbData> {
  return new Promise((resolve, reject) => {
    this.getEventsRaw(from, to, opts, (err, data) => {
      if (err) reject(err)
      else resolve(data)
    })
  })
}

function send(this: PClient,
    data: Buffer | string,
    opts: {targetVersion?: number, conflictKeys?: string[]} = {}
): Promise<number> {
  return new Promise((resolve, reject) => {
    this.sendRaw(data, opts, (err, version) => {
      if (err) reject(err)
      else resolve(version)
    })
  })
}

function getVersion(this: PClient): Promise<number> {
  // v_start is next_version.
  return this.getEvents(-1, -1, {maxbytes: 0})
  .then(data => data.v_start - 1)
}
// function getVersion(this: PClient): Promise<number> {
//   return new Promise((resolve, reject) => {
//     this.getEventsRaw(-1, -1, { maxbytes: 0 }, (err, data) => {
//       if (err) reject(err)
//       // v_start is next_version.
//       else resolve(data!.v_start - 1)
//     })
//   })
// }

function connect(port: number = 9999, hostname: string = 'localhost'): Promise<PClient> {
  return new Promise((resolve, reject) => {
    connectRaw(port, hostname, (err, client) => {
      if (err) reject(err)
      else resolve(client)
    })
  })
}

function connectRaw(port: number, hostname: string, callback: Callback<PClient>) {
  const errListener = (err?: Error | null) => { callback(err) }

  const client = net.connect(port, hostname, () => {
    client.removeListener('error', errListener)

    // This is the version of the next event we expect to see.
    let eventstream_version = -1

    // For normal subscriptions, if the user doesn't specify a maximum size
    // limit, we'll just keep polling for more data until we hit current.
    let subCb: SubCb | null = null
    let subCbData: SubCbData | null = null
    let resub = false

    let oneshotCbs: SubCb[] = []

    const sendcbs: SendCb[] = []

    function subscribeRaw(flags: number, from: number, maxbytes: number) {
      const msg = msgToBytes(ClientMsgType.Subscribe, [flags, from, maxbytes])
      // console.log(`writing subscribe ${msg.length} bytes`, msg)
      client.write(msg)
    }

    const subscribe = (from: number = 0, opts: {maxbytes?: number, oneshot?: boolean} = {}, callback: SubCb) => {
      resub = opts.maxbytes == null && !opts.oneshot
      const maxbytes = opts.maxbytes || DEFAULT_SUB_SIZE
      const flags = opts.oneshot ? SubFlags.Oneshot : 0
      subscribeRaw(flags, from, maxbytes)

      if (opts.oneshot) oneshotCbs.push(callback)
      else {
        assert(subCb == null)
        subCb = callback
      }
    }

    const api: PClient = {
      source: undefined,
      onevents: undefined,
      onclose: undefined,
      onunsubscribe: undefined,

      subscribe(from = -1, opts: {maxbytes?: number} = {}, callback = doNothing) {
        subscribe(from, opts, callback)
      },

      // This gets ops in the range [from, to].
      getEventsRaw(from, to, opts: {maxbytes?: number} = {}, callback = doNothing) {
        let aggregateData: SubCbData | null = null

        const getNext = (from: number) => {
          // TODO: Pass in maxops.
          subscribe(from, {maxbytes: opts.maxbytes, oneshot: true}, (err, data) => {
            if (err) return callback(err)

            aggregateData = mergeSubCbData(aggregateData, data!)

            if (to === -1
                || (to >= 0 && to <= aggregateData.v_end)
                || aggregateData.current) {

              // TODO: maxopts. Until then we'll trim the end off the aggregate events.
              const {events} = aggregateData
              if (to > 0) while (events.length > 0 && events[events.length - 1].version < to) {
                aggregateData.v_end = events.pop()!.version
              }
              return callback(null, aggregateData)
            }
            else getNext(aggregateData.v_end)
          })
        }
        getNext(from)
      },
      getEvents,

      sendRaw(data, opts, callback = doNothing) {
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

      send,
      getVersion,

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
      assert(resub === false)
      api.onunsubscribe && api.onunsubscribe()
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

    let buf: Buffer | null = null
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
          // This gets called for both subscribe and oneshot requests.

          if (cursor.remain() < 1 + 8 + 8 + 1) return false
          const proto_v = cursor.readByte()
          const v_start = cursor.readUInt64()
          const size = cursor.readUInt64()
          const flags = cursor.readByte()

          // console.log('sub response', {proto_v, v_start, size, flags})

          // TODO: Could start processing messages without waiting for entire catchup.
          if (cursor.remain() < size) return false

          // console.log('remain', cursor.remain(), 'size', size, buf.slice(cursor.n))
          const start = cursor.n

          const oneshot = !!(flags & SubFlags.Oneshot)
          const complete = !!(flags & SubFlags.Complete)
          const current = !!(flags & SubFlags.Current)

          const events: Event[] = []
          let v_next = v_start
          while (cursor.n - start < size) {
            // console.log('n', cursor.n, 'start', start, 'size', size)
            const evt = cursor.readEvent(v_next)
            assert(evt)
            events.push(evt!)
            v_next += evt!.batch_size
          }

          if (!oneshot) {
            eventstream_version = v_next
            onEvents(events, eventstream_version)
          }

          // OK!

          const data: SubCbData = {v_start, v_end: v_next, size, complete, oneshot, current, events}

          if (oneshot) {
            oneshotCbs.shift()!(null, data)
          } else {
            subCbData = mergeSubCbData(subCbData, data)
            // If we're resubscribing, just re-subscribe asking for the next batch. We'll call the cb
            // when we hit current.
            if (resub && !data.current) {
              subscribeRaw(0, eventstream_version, DEFAULT_SUB_SIZE)
              break
            }

            subCb!(null, subCbData)
            subCb = null
            subCbData = null
            resub = false
          }

          if (complete && !oneshot) onUnsubscribe()
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
    client.on('data', (data: Buffer) => {
      // console.log('read data of length', data.length, data)
      buf = (buf == null) ? data : Buffer.concat([buf, data])
      while (1) { if (!tryReadOne()) break }
    })
  })

  client.on('error', errListener)
}

type EventOpts = {targetVersion?: number, conflictKeys?: string[]}
type EventProps = {
  evt: Buffer | string,
  opts: EventOpts,
  callback: Callback<number>
}

type GetEventsProps = {
  from: number,
  to: number,
  callback: Callback<SubCbData>
}

// Callback returns source, which is the only thing we don't have from the outset.
const reconnecter = (port: number, hostname: string, firstConnectCallback: Callback<string>
): PClient => {
  let callbackCalled = false
  let wantSubFromV: number | null = null

  const retryTimeoutSeconds = 2
  type State = 'connecting' | 'waiting' | 'connected' | 'stopped'
  let state: State = 'waiting'
  let client: PClient | null = null
  let timer: NodeJS.Timer | null = null
  let sendEventQueue: EventProps[] = []
  let subCb: SubCb | null = null

  let getEventsQueue: GetEventsProps[] = []

  const trySendRaw = (eventProps: EventProps) => {
    if (client != null) client.sendRaw(eventProps.evt, eventProps.opts, (err, v) => {
      // We eat ClosedBeforeConfirm errors because we'll just resend the event
      // when we reconnect.
      if (err && err.name === 'ClosedBeforeConfirm') return

      const idx = sendEventQueue.indexOf(eventProps)
      assert(idx >= 0)
      if (idx === 0) sendEventQueue.shift()
      else sendEventQueue.splice(idx, 1)
      eventProps.callback(err, v)
    })
  }

  const trySendGetEvents = (props: GetEventsProps) => {
    const {from, to, callback} = props
    if (client != null) client.getEventsRaw(from, to, {}, (err, data) => {
      if (err && err.name === 'ClosedBeforeConfirm') return

      // Ugh this is a copy+paste from trySendRaw.
      const idx = getEventsQueue.indexOf(props)
      assert(idx >= 0)
      if (idx === 0) getEventsQueue.shift()
      else getEventsQueue.splice(idx, 1)
      callback(err, data)
    })
  }

  const api: PClient = {
    source: undefined,
    onevents: undefined,
    onclose: undefined,
    onunsubscribe: undefined,

    // Aka wantSubscribe.
    subscribe(from = -1, opts = {}, callback = doNothing) {
      // You can't bounce around with this because there's race conditions
      // in how I'm updating wantSubFromV in the event listener.
      assert(wantSubFromV === null)

      wantSubFromV = from
      subCb = callback
      if (client != null) sub()
    },

    // Note this returns ops in the [from, to] range.
    getEventsRaw(from, to, opts = {}, callback: Callback<SubCbData>) {
      const oneshot = {from, to, callback}
      getEventsQueue.push(oneshot)
      trySendGetEvents(oneshot)
    },

    getEvents,

    // Oops - never exposed this from prozess client.
    // wantUnsubscribe() {
    //   if (client != null) client.
    // },
    //send(data: Buffer | string, opts: {targetVersion?: number, conflictKeys?: string[]}, callback = doNothing) {
    sendRaw(evt, opts = {}, callback = doNothing) {
      const eventProps = {evt, opts, callback}
      sendEventQueue.push(eventProps)
      trySendRaw(eventProps)
    },

    send,
    getVersion,

    close() {
      if (client) {
        client.close()
        client = null
      }
      if (timer) {
        clearTimeout(timer)
        timer = null
      }
      state = 'stopped'
    },
  }

  function sub() {
    if (wantSubFromV === null) return

    assert(client)
    client!.onevents = (evts: Event[], resultingVersion: number) => {
      // console.log('xxxx', !!eventListener)
      wantSubFromV = resultingVersion
      api.onevents && api.onevents(evts, resultingVersion)
    }
    client!.subscribe(wantSubFromV, {}, (err, data) => {
      if (subCb) {
        subCb(err, data)
        subCb = null
      }
    })
  }

  const tryConnect = () => {
    assert.strictEqual(state, 'waiting')
    state = 'connecting'
    connectRaw(port, hostname, (err, _client) => {
      if (state === 'stopped') {
        // This is a bit inelegent. It'd be nicer to call close on the
        // underlying socket.
        if (_client) _client.close()
        return
      } else if (err) {
        console.warn('Connection failed:', err.message)
        state = 'waiting'
        timer = setTimeout(tryConnect, retryTimeoutSeconds * 1000)
        return
      } else {
        assert(client == null)

        state = 'connected'
        client = _client!

        // If the source has changed we've connected to a different store.
        if (api.source != null && client.source !== api.source) {
          console.error('Prozess server has changed identity. Aborting!')
          state = 'stopped'
          client.close()
          client = null
        }

        // Resubscribe
        sub()

        // Re-send all our pending events.
        // TODO: Resend as a batch, not as individual events.
        sendEventQueue.forEach(trySendRaw)
        getEventsQueue.forEach(trySendGetEvents)

        if (!callbackCalled) {
          api.source = client!.source
          callbackCalled = true
          firstConnectCallback(null, client!.source)
        }
      }
    })
  }
  tryConnect()

  return api
}

export {connect, reconnecter}
