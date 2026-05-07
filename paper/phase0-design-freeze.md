# Phase 0 Design Freeze

## Goal

Phase 0's goal is to freeze the implementation boundary for the paper's second contribution so later development stays aligned with the current TerarkDB codebase, paper claims, and evaluation plan.

This phase does not add code. It produces a design contract for the next phases.

## Target

The target of this round is:

- `chunk-level validity materialization`
- `compaction-coupled live-chunk bitmap`
- `GC fast path + legacy fallback`

The target is not a full redesign of TerarkDB's GC subsystem. The implementation must remain an incremental extension of the current `KV separation -> dependence metadata -> VersionBuilder aggregation -> GC` pipeline.

## Scope

This round only commits to the following technical path:

1. Extend separated value references so new-format value index can carry `blob_file_number + chunk_id`.
2. Let Flush/Compaction output SSTs record per-blob chunk reference sets.
3. Persist those chunk reference sets through SST properties and Manifest.
4. Aggregate per-SST chunk references into version-level `live-chunk bitmap`.
5. Let Blob GC consume the bitmap first, while retaining the current lookup-based path as fallback.

## Non-Goals

Phase 0 explicitly excludes the following work:

- No `object-level invalidation bitmap`.
- No object id allocation or per-object stable identifier.
- No change to the current GC candidate picker framework.
- No rewrite of compaction scheduling, blob picker scoring, or maintenance budgeting.
- No claim of fully eliminating all verification in every case.
- No automatic multi-mode GC policy in the first implementation round.
- No requirement to retrofit old SSTs or old manifests with new metadata offline.

## Why Chunk-Level

Chunk-level is the smallest unit that is both useful and realistic for the current codebase:

- The existing GC path is blob-file oriented.
- The current dependence metadata is already file-level and easy to extend incrementally.
- Chunk-level can support a real fast path without requiring object-id plumbing across write path, compaction, manifest, recovery, and GC.
- Chunk-level is strong enough to support the paper's central claim: move from "GC-time reconstruction" toward "online materialized validity view".

## Required Compatibility

The new design must preserve the following compatibility rules:

- New-format SSTs may write new bitmap metadata.
- Old SSTs, old manifests, and old value-index encodings must remain readable.
- If any required bitmap metadata is missing, the system must automatically fall back to the legacy `GetKey()`-based GC validation path.
- Feature-off behavior must be equivalent to the current implementation.

In short: new metadata is opportunistic acceleration, not a new correctness dependency.

## Required Default Behavior

Phase 1 and beyond must follow these defaults:

- `enable_blob_validity_bitmap = false`
- `blob_gc_chunk_size = 64 * 1024`

When the feature is disabled:

- no new fast path is used;
- old GC semantics remain unchanged;
- new code paths must degrade to today's behavior.

## Code Constraints

The implementation must fit the current TerarkDB structure:

- Write path hotness routing already exists and is not part of this phase's redesign.
- Value separation is currently encoded through `SeparateHelper` and `kTypeValueIndex/kTypeMergeIndex`.
- Dependence metadata already flows through SST property cache and `VersionEdit`.
- Version-level garbage estimation already exists in `VersionBuilder`.
- Current Blob GC still uses `ProcessGarbageCollection()` with `Version::GetKey()` validation.

Therefore the new work must be expressed as an extension of existing metadata flow, not a parallel subsystem.

## Claim Boundary For Paper

After this design freeze, the intended claim boundary is:

- We can claim `compaction-coupled validity materialization`.
- We can claim `online maintenance of blob-level live-chunk view`.
- We can claim `GC fast path with legacy fallback`.

We cannot yet claim:

- object-level invalidation tracking;
- complete elimination of all GC-time validation;
- globally optimal multi-mode reclamation;
- a full replacement of the current GC picker.

## Deliverables For Next Phases

A phase is considered on track only if it contributes to one of these end states:

- reference graph generation;
- live view aggregation;
- GC consumption of live view;
- recovery and legacy fallback.

Any change outside these four goals is out of scope for the first round.

## Exit Criteria

Phase 0 is complete when the team agrees on the following:

- what the first implementation will do;
- what it will not do;
- what the compatibility contract is;
- what the paper may and may not claim before later phases are finished.
