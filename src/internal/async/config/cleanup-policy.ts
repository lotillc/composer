/**
 * Cleanup Policy Configuration
 *
 * Defines rules for cleaning up old composer bundles from S3.
 * This policy helps manage storage costs while preserving bundles
 * needed for in-flight workflows.
 */

/**
 * Cleanup policy for managing composer bundle retention in S3.
 *
 * There are two types of bundles:
 * - **Version bundles**: Individual builds by git hash (e.g., workflow-bundle-abc1234.js)
 *   Governed by: minimumRetentionDays, gracePeriodHours, forceKeepHashes
 * - **Merged bundles**: Combined bundles containing multiple versions (e.g., merged-workflow-bundle.js)
 *   Governed by: maxMergedBundlesToKeep
 */
export interface CleanupPolicy {
  /**
   * Minimum number of days to retain a version bundle before considering deletion.
   * Version bundles newer than this are always kept regardless of other factors.
   * Only applies to version bundles (per git hash), not merged bundles.
   */
  minimumRetentionDays: number;

  /**
   * Git hashes that should never be deleted, regardless of age or activity.
   * Useful for pinning specific production versions or important releases.
   * Only applies to version bundles.
   */
  forceKeepHashes: string[];

  /**
   * Number of hours after a workflow becomes inactive before its version bundle
   * can be deleted (assuming minimumRetentionDays has passed).
   * This grace period prevents premature deletion of recently-completed workflows.
   * Only applies to version bundles.
   */
  gracePeriodHours: number;

  /**
   * Maximum number of merged bundles to keep in S3.
   * Older merged bundles are deleted first (FIFO).
   * This enables fast rollback to recent versions.
   * Only applies to merged bundles, not version bundles.
   * Note: Does not respect minimumRetentionDays - purely count-based.
   */
  maxMergedBundlesToKeep: number;
}

/**
 * Cleanup policy for composer bundles
 */
export const CLEANUP_POLICY: CleanupPolicy = {
  minimumRetentionDays: 1,
  forceKeepHashes: [],
  gracePeriodHours: 48,
  maxMergedBundlesToKeep: 5,
};
