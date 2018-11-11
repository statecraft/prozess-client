declare module 'errno' {
  let custom: {
    createError(name: string, parent?: ErrorConstructor): ErrorConstructor
  }
}

declare module 'msgpack-lite' {
  function encode(data: any): Buffer
}
