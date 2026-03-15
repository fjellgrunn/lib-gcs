# Usage Guide

Comprehensive usage guidance for `@fjell/lib-gcs`.

## Installation

```bash
npm install @fjell/lib-gcs
```

## API Highlights

- `GCSLibrary`, `GCSLibraryFactory`, and options/operations exports
- `PathBuilder`, `FileProcessor`, and `FileOperations` helpers
- `Primary` and `Contained` namespaces plus file type exports

## Quick Example

```ts
import { GCSLibraryFactory } from "@fjell/lib-gcs";

const factory = new GCSLibraryFactory({
  bucket: "my-fjell-bucket",
  prefix: "widgets",
});

const library = factory.createPrimaryLibrary();
await library.save({ pri: { pk: "widget#1", sk: "meta" }, name: "Widget One" });
```

## Model Consumption Rules

1. Import from the package root (`@fjell/lib-gcs`) instead of deep-internal paths unless explicitly documented.
2. Keep usage aligned with exported public symbols listed in this guide.
3. Prefer explicit typing at package boundaries so generated code remains robust during upgrades.
4. Keep error handling deterministic and map infrastructure failures into domain-level errors.
5. Co-locate integration wrappers in your app so model-generated code has one canonical entry point.

## Best Practices

- Keep examples and abstractions consistent with existing Fjell package conventions.
- Favor composable wrappers over one-off inline integration logic.
- Add targeted tests around generated integration code paths.
