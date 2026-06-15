# Load & chaos tests (Chunk 8)

- gRPC load: use `ghz` or a custom Go generator against SubmitPayment.
- Correctness proofs (the headline results):
  - Concurrent-duplicate test: fire N identical submissions in parallel,
    assert exactly one transaction row is booked.
  - Relay-kill test: kill the relay mid-run, restart, assert zero lost events
    (every received payment eventually appears in payments.received).
- Record p99 latency + sustained TPS on a single instance. Label everything as
  load-test benchmarks, not production metrics.
