# Integration Guide

Guide for integrating `@fjell/lib-gcs` into larger Fjell-based systems.

## Where It Fits

Google Cloud Storage-backed Fjell library implementation for object/file workflows.

## Recommended Integration Pattern

- Inject authenticated GCS clients and keep bucket/prefix config environment-specific
- Use deterministic path building to avoid cross-tenant collisions
- Pair with cache and logging layers for latency and traceability

## System Composition Checklist

- Define package boundaries: schema/types, transport, operations, adapters, and UI.
- Keep contracts stable by sharing @fjell/types interfaces where applicable.
- Centralize retries/timeouts/logging around infrastructure-facing operations.
- Validate inputs at API boundaries before invoking persistence or provider layers.
- Add contract and integration tests for every generated workflow.

## Cross-Library Pairings

- Pair with @fjell/types for shared contracts.
- Pair with @fjell/validation for input and schema checks.
- Pair with @fjell/logging for observability in integration flows.
- Pair with storage/router/provider packages based on your runtime architecture.

## Integration Example Shape

Use this package behind an application service layer that exposes stable domain methods. Generated code should call those service methods, not raw infrastructure primitives, unless your architecture intentionally keeps infrastructure at the edge.
