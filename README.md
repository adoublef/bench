# bench

## rust

### trait aliases

```rs
trait CsvByteStreamHandle: CsvByteStream + Clone + Send + Sync + 'static {}
impl<T: ?Sized + CsvByteStream + Clone + Send + Sync + 'static> CsvByteStreamHandle for T {}
```
> not to confuse with unstable feature with the same name

---

- [Project layout](https://doc.rust-lang.org/cargo/guide/project-layout.html)
- [Pin](https://without.boats/blog/pin/)
- [Tree structured concurrency](https://blog.yoshuawuyts.com/tree-structured-concurrency/)
- [Elegant and safe concurrency in Rust with async combinators](https://kerkour.com/rust-async-combinators-concurrency)
- [Why this `while let` loop doesn't terminate](https://users.rust-lang.org/t/why-this-while-let-loop-doesnt-terminate/20640)
- [Async deserializing an array of json as a stream](https://users.rust-lang.org/t/async-deserializing-an-array-of-json-as-a-stream/60299/3)
- [Darksonn/backblaze-b2-rs](https://github.com/Darksonn/backblaze-b2-rs/tree/ver0.2/src/b2_future)
- [Async rust: server-sent events with a remote heartbeat](https://elfsternberg.com/blog/axum-sse-remote-heartbeat/#NWD4PsPaR-5)
- [Dependency injection in Axum handlers. A quick tour](https://tulipemoutarde.be/posts/2023-08-20-depencency-injection-rust-axum/)
- [backblaze-b2-rs](https://github.com/Darksonn/backblaze-b2-rs/tree/ver0.2/src/b2_future)
- [hyper-json-stream](https://github.com/arnaudpoullet/hyper-json-stream/blob/main/README.md)
