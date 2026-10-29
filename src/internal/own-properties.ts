/**
 * Writes keyed by names Composer does not choose: bag fields, step outputs, signal names.
 *
 * A plain assignment `target[key] = value` with the key `__proto__` calls the inherited
 * prototype setter instead of storing a field, so the value silently disappears. These
 * helpers define every key as an own data property, whatever its name. The objects stay
 * ordinary objects, since step code receives them.
 *
 * Kept in a leaf module with no imports: the workflow factory runs in Temporal's
 * deterministic sandbox and bundles whatever it imports.
 *
 * @module own-properties
 */

/** Sets `target[key]` as an own, enumerable, writable data property. */
export function setOwn(target: object, key: string, value: unknown): void {
  Object.defineProperty(target, key, { value, writable: true, enumerable: true, configurable: true });
}

/** Like `Object.assign`, but every key of `source` lands as an own data property. */
export function assignOwn<T extends object>(target: T, source: object | null | undefined): T {
  if (source) {
    for (const [key, value] of Object.entries(source)) setOwn(target, key, value);
  }
  return target;
}
