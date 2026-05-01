/**
 * Memo key and value used to tag schedules managed by composer.
 *
 * During sync, only schedules with this memo are considered for update
 * or deletion. Schedules without it (manually created or from other
 * systems) are never touched.
 */
export const MANAGED_BY_MEMO_KEY = "managedBy";
export const MANAGED_BY_MEMO_VALUE = "composer";
