---
"@lotiai/composer": patch
---

Validate developer-chosen names and stop interpolating them raw into generated code.

**Definition-time validation.** `createWorkflow`, `step`, `fanOut` and
`.checkpoint` now reject `__proto__`, `constructor` and `prototype`, and any name
that does not start with a letter or `_` or that contains characters other than
letters, digits, `_`, `.` and `-`. The error names the kind of name and the rule
it broke.

**Generated workflow source.** The workflow worker's generated module now names
each plan with an indexed identifier (`plan0`, `plan1`, ...) and writes each
export key as a JSON string literal through `Object.defineProperty`. Previously a
workflow name containing a space, a quote or a leading digit produced a module
that would not parse, failing the worker at startup, and a workflow named
`__proto__` replaced the prototype of `exports` instead of being exported.

**Activity registration.** The activity worker registers step, error-handler,
`mapInput` and `aggregateResults` activities as own properties, so a reserved
name cannot be swallowed by the `__proto__` setter.
