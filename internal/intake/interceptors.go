package intake

// gRPC server interceptors:
//   - Auth:        validate JWT / mTLS on each call.
//   - RateLimit:   per-caller token bucket for backpressure.
//   - Logging:     inject + propagate a correlation_id, log structured entries.
//
// TODO: implement as grpc.UnaryServerInterceptor / StreamServerInterceptor.
