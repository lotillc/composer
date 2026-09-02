---
"@lotiai/composer": minor
---

Stop leaking raw error text into sinks a consumer cannot redact, and add the seams to control the ones that remain.

Composer put a step error's message into three sinks without asking the consumer: the injected logger, OpenTelemetry span attributes, and Temporal's event history. For a MikroORM/knex driver error that message is the fully parameter-inlined SQL, quoting real row values.

- **Failure logs carry the `Error`, not a projection of it.** `endWorkflowObservability` logged `error: { name, message, stack }` and the activity worker logged a bare `error: stepError.message`. Both discarded the structured fields a serializer classifies on -- a pg error's `code`, `severity`, `table`, `constraint` -- and severed the `cause` chain below the top-level wrapper. Composer now hands the logger the `Error` and lets its serializer decide. Swept across every site that logs through the injected `ComposerLogger`. **A logger that `JSON.stringify`s its metadata will now render `error` as `{}`; give it an error serializer.**
- **Span attributes no longer carry the raw message by default.** `workflow.error.message`, `workflow.batch.error.message` and `step.error.message` are replaced by `*.error.type` (the error's name), and `recordException` receives a name-only exception rather than the `Error` -- whose `stack` header line repeats the message. The new `createComposer({ traceErrorMessage })` option puts text back on the span, scrubbed or raw as the consumer decides.
- **`temporal.dataConverter` and `temporal.interceptors` are now exposed.** A `failureConverterPath` or an activity-inbound interceptor can rewrite a failure before Temporal records it. `dataConverter` reaches both worker types and the client Composer uses to start and await workflows, since a converter installed only on the workers leaves the client unable to decode what they wrote.
