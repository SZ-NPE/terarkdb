# Delta-Block + Exact-GC Port Diff (reference → third-party)

Purpose: a porting reference that lists every implementation difference between the
original `delta_block` / `exact_gc` work and the third-party TerarkDB tree, so the
two features can be re-extended in `third-party/terarkdb`. Scope is the full usage
chain, not just renamed symbols.

This document is a non-authoritative handoff note. If it conflicts with current
code, trust the code and update this file.

---

## 0. Repositories and roles

| Role | Path | Include dir | Namespace flavor |
| --- | --- | --- | --- |
| Reference (original impl) | `/home/pengzhifeng.002/temp/terarkdb` | `terarkdb/` | terarkdb |
| Target (to be extended) | `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb` | `rocksdb/` | rocksdb-style |
| Design docs only | `/home/pengzhifeng.002/terarkdb` | — | `paper/delta-block-exact-gc-design.md`, `paper/delta-separate-poc-switch.md` |

Both code repos are on branch `release-version`. `/home/pengzhifeng.002` is a
symlink to `/data00/home/pengzhifeng.002`.

Naming caveat: the user's phrasing "I implemented it in `/temp/terarkdb`" maps to
`/home/pengzhifeng.002/temp/terarkdb`. The path `/temp/terarkdb` literally does not
exist.

---

## 1. Global name / semantic mapping

This table is the single most important porting key. Most files differ only by
applying this mapping; the sections below flag where real logic also changes.

| Concept | Reference | Target |
| --- | --- | --- |
| Delta block enable (table opt) | `use_delta_block` (default `true`) | `use_separated_value_meta_block` (default `false`); `use_delta_block` kept as alias |
| Exact GC enable (cf opt) | `enum ExactGarbageRatioModeEnum exact_garbage_ratio` (3-state) | `bool byte_precise_gc` (+ `bool precise_gc` compat alias) |
| Table-factory capability query | `IsExactGarbageCollectionSupported()` | `SupportsBytePreciseGC()` |
| Per-Blob obsolete byte counter | `size_antiquated` | `num_antiquation_bytes` |
| GC accounting denominator | `raw_size()` (= `raw_key_size + raw_value_size`) | `BlobGcAccountingBytes()` (same, with legacy `fd.GetFileSize()` fallback + overflow guard) |
| Dependence separated-bytes field | `separated_total_size` (plain) | `union { byte_count; separated_total_size; }` |
| Reader → iterator value transfer | typed `GetExtendedInfo(IteratorExtendedInfo*)` | string `GetProperty("rocksdb.delta.is-separated" / ".value-size" / ".value-meta")` |
| Delta hash container | `chash_map` (terark) | `std::unordered_map` |
| Varint decode helper | `GetVarint32Ptr` (assert on OOB) | `GetVarint32Checked` (Corruption Status on OOB) |

Key structural simplifications in target:
- The reference 3-state enum (`kExactGCDisabled` / `kExactGCEnabled` /
  `kExactGCUpdateValueSize`) is collapsed into a single `byte_precise_gc` bool. The
  `kExactGCUpdateValueSize` "refresh value_size during compaction" third state is
  **not** enabled by plain `byte_precise_gc`; the target has an explicit
  `ShouldUpdateValueSize()` hook, and the current paper path keeps it disabled so
  byte-precise GC consumes metadata instead of fetching separated values to repair
  missing sizes.
- Target additionally has an `enable_delta_separate` master gate plus drop-key
  cache / hotness / richer metrics that do not exist in the reference. Target also
  dropped `remote_value_size` from version_edit.

---

## 2. Feature impact map

### 2.1 `delta_block` (separated-value metadata block)

What it is: an SST-side sidecar aligned to entry order. It records per-entry
separated `value_size` (and optional `value_meta`), using a bitmap + rank/select to
locate separated entries. It is the data source for byte-precise accounting.

Files in the chain (target paths):
- Options: [include/rocksdb/table.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/rocksdb/table.h) (`use_separated_value_meta_block`)
- Factory: [table/block_based_table_factory.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_factory.h) / [.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_factory.cc) (`SupportsBytePreciseGC`, option-map alias, sanitize)
- Builder: [table/delta_builder.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_builder.h) / [.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_builder.cc)
- Reader: [table/delta_reader.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_reader.h) / [.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_reader.cc)
- Table builder integration: [table/block_based_table_builder.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc)
- Table reader integration: [table/block_based_table_reader.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_reader.cc)
- Iterator value extraction: [table/iterator.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/iterator.cc)
- Properties: [table/meta_blocks.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/meta_blocks.cc), table_properties
- db_bench flag: [tools/db_bench_tool.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/tools/db_bench_tool.cc)

### 2.2 `exact_gc` (byte-precise Blob GC)

What it is: GC candidate selection by byte-level garbage ratio
(`obsolete_bytes / accounting_bytes`) instead of entry-count ratio
(`dead_entries / total_entries`). It consumes the per-entry `value_size` produced by
`delta_block`.

Files in the chain (target paths):
- Options: [include/rocksdb/options.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/rocksdb/options.h) (`byte_precise_gc`, `precise_gc`)
- cf sanitize: [db/column_family.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/column_family.cc)
- Dependence struct: [include/rocksdb/types.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/rocksdb/types.h)
- FileMetaData accounting: [db/version_edit.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_edit.h) (`num_antiquation_bytes`, `BlobGcAccountingBytes`)
- Online materialization: [db/version_builder.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_builder.cc)
- Version ratio: [db/version_set.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_set.cc)
- Candidate scoring: [db/compaction_picker.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_picker.cc)
- Compaction dependence aggregation: [db/compaction_iterator.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_iterator.cc), [db/compaction_job.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_job.cc)
- Serialization: version_edit.cc, meta_blocks.cc, table_properties
- Metrics: [monitoring/statistics.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/monitoring/statistics.h)

Dependency: `exact_gc` is only meaningful when `delta_block` is enabled; both
sanitize paths enforce this (see §4).

---

## 3. Option definitions

### 3.1 cf options

Reference [include/terarkdb/options.h](file:///home/pengzhifeng.002/temp/terarkdb/include/terarkdb/options.h):
```cpp
enum ExactGarbageRatioModeEnum {     // ~L107-122
  kExactGCDisabled,
  kExactGCEnabled,
  kExactGCUpdateValueSize,
};
ExactGarbageRatioModeEnum exact_garbage_ratio = kExactGCDisabled;  // ~L457
size_t blob_size;            // ~L397
size_t middle_blob_size;     // ~L407
size_t middle_combine_level; // ~L413
// no enable_delta_separate
```

Target [include/rocksdb/options.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/rocksdb/options.h):
```cpp
bool enable_delta_separate = true;   // ~L313  (master gate, NEW)
size_t middle_blob_size = size_t(-1);     // ~L319
size_t middle_combine_level = size_t(-1); // ~L323
bool byte_precise_gc = false;        // ~L372
bool precise_gc = false;             // ~L376  (compat alias)
```

Porting note: target replaces the 3-state enum with a bool plus an overridable
`SeparateHelper::ShouldUpdateValueSize()` hook. Plain `byte_precise_gc` must not
silently fetch old separated values during compaction; if the old
`kExactGCUpdateValueSize` repair mode is needed again, reintroduce it deliberately
behind a separate switch and validate the write-workload cost.

### 3.2 table options

Reference [include/terarkdb/table.h](file:///home/pengzhifeng.002/temp/terarkdb/include/terarkdb/table.h):
```cpp
bool use_delta_block = true;                       // ~L275
virtual bool IsExactGarbageCollectionSupported();  // ~L568
```

Target [include/rocksdb/table.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/rocksdb/table.h):
```cpp
bool use_separated_value_meta_block = false;       // ~L196
```

---

## 4. Configuration validation / sanitize

### 4.1 table factory capability

Reference [table/block_based_table_factory.h](file:///home/pengzhifeng.002/temp/terarkdb/table/block_based_table_factory.h):
```cpp
bool IsExactGarbageCollectionSupported() const override {  // ~L102
  return use_delta_block;
}
```

Target [table/block_based_table_factory.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_factory.h):
```cpp
bool SupportsBytePreciseGC() const override {  // ~L80
  return use_separated_value_meta_block;
}
```

Target option map aliases both names to the same offset
([.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_factory.h) ~L167-172): `use_separated_value_meta_block`
and legacy `use_delta_block` both write the `use_separated_value_meta_block` field.

### 4.2 factory sanitize

Target [table/block_based_table_factory.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_factory.cc) ~L222-277: index-type
restriction plus an explicit check — `byte_precise_gc && !use_separated_value_meta_block`
returns `InvalidArgument`.

Reference [table/block_based_table_factory.cc](file:///home/pengzhifeng.002/temp/terarkdb/table/block_based_table_factory.cc) ~L383-390: index-type
restriction with a message suggesting to disable `use_delta_block` +
`exact_garbage_ratio`.

### 4.3 cf sanitize

Target [db/column_family.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/column_family.cc) ~L329-337:
```cpp
// precise_gc -> byte_precise_gc fold
result.byte_precise_gc = result.byte_precise_gc || result.precise_gc;
if (!table_factory_supports) {
  result.byte_precise_gc = false;
}
result.precise_gc = result.byte_precise_gc;   // keep alias mirrored
```

Reference [db/column_family.cc](file:///home/pengzhifeng.002/temp/terarkdb/db/column_family.cc) ~L366-368: when unsupported,
`exact_garbage_ratio = kExactGCDisabled`.

Porting note: target maintains two mirrored bools (`byte_precise_gc` and
`precise_gc`). Keep them in sync on every downgrade path.

---

## 5. Delta block builder format

Reference [table/delta_builder.h](file:///home/pengzhifeng.002/temp/terarkdb/table/delta_builder.h) / [.cc](file:///home/pengzhifeng.002/temp/terarkdb/table/delta_builder.cc):
```cpp
void Add(bool is_separated, uint32_t value_size, const std::string* value_meta);
// Reset() does NOT clear num_entries_ / finishing_delta_
```

Target [table/delta_builder.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_builder.h) / [.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_builder.cc):
```cpp
Status Add(bool is_separated, uint32_t value_size, const Slice* value_meta);
// returns InvalidArgument when value_meta too large
// Reset() clears finishing_delta_ and num_entries_
// AddIndexEntry has a finishing_delta_ early-return guard
```

Porting note: the target hardened the builder to return `Status` and guard against
oversized `value_meta` and overflow. The reference asserts / silently proceeds.

---

## 6. Delta block reader API (architecture difference)

This is the most invasive difference. The reference exposes value metadata via a
typed callback; the target exposes it via string properties.

Reference [table/delta_reader.h](file:///home/pengzhifeng.002/temp/terarkdb/table/delta_reader.h):
```cpp
void GetExtendedInfo(IteratorExtendedInfo* info);   // single typed call
chash_map<uint32_t, DeltaBlockInfo> delta_blocks_;
// OOB -> terarkdb_assert ; decode with GetVarint32Ptr
```

Target [table/delta_reader.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_reader.h):
```cpp
bool   CurrentEntryIsSeparated() const;  // split into 3 accessors
uint32_t CurrentValueSize() const;
Slice    CurrentValueMeta() const;
std::unordered_map<uint32_t, DeltaBlockInfo> delta_blocks_;
// OOB -> safe return (0 / empty) ; decode with GetVarint32Checked (Corruption)
```

Both keep the same on-disk layout: delta index block maps data-block numbers to
physical delta blocks; first delta entry is the meta block; remaining entries hold
separated value metadata grouped by data block; bitmap + rank/select
(`terark::rank_select_il`) locates separated entries via `IsSeparated` /
`BitmapRank1`.

---

## 7. Table builder integration

Reference [table/block_based_table_builder.cc](file:///home/pengzhifeng.002/temp/terarkdb/table/block_based_table_builder.cc):
```cpp
delta_block.Add(is_separated, value_size, &value_meta.meta_data);  // ~L606, void
WriteDeltaBlock gated on use_delta_block;                          // ~L1150
```

Target [table/block_based_table_builder.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc):
- gates on `use_separated_value_meta_block` (~L439, L464, L939)
- `delta_block.Add(...)` returns `Status`, checked (~L465-470)
- `delta_index_fits` overflow check
- accumulates props: `separated_total_size`, `separated_entry_count`,
  `value_meta_total_size`
- stores `internal_key_size + value_size` with `UINT32` overflow checks

---

## 8. Table reader → iterator value extraction

Reference [table/block_based_table_reader.cc](file:///home/pengzhifeng.002/temp/terarkdb/table/block_based_table_reader.cc) ~L3530-3553:
```cpp
void GetExtendedInfo(IteratorExtendedInfo* info) {
  delta_block_reader_->GetExtendedInfo(info);
}
```
Reference [table/iterator.cc](file:///home/pengzhifeng.002/temp/terarkdb/table/iterator.cc) ~L170-184:
```cpp
iter_->GetExtendedInfo(&info);
meta->value_size = info.value_size;
```

Target [table/block_based_table_reader.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_reader.cc) ~L2469-2496:
```cpp
GetProperty("rocksdb.delta.is-separated") -> CurrentEntryIsSeparated()
GetProperty("rocksdb.delta.value-size")   -> CurrentValueSize()
GetProperty("rocksdb.delta.value-meta")   -> CurrentValueMeta()
```
Target [table/iterator.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/iterator.cc) ~L154-208:
```cpp
CombinedInternalIterator::value(user_key, meta, value_size, block_handle)
// reads via GetProperty, decodes with GetVarint32
```

Porting note: this is where the typed-vs-string design split lives. If the target
keeps `GetProperty`, every consumer that previously called `GetExtendedInfo` must be
re-pointed to the property accessors.

---

## 9. GC accounting chain (exact_gc)

### 9.1 Dependence struct

Reference [include/terarkdb/types.h](file:///home/pengzhifeng.002/temp/terarkdb/include/terarkdb/types.h) ~L49-53:
```cpp
struct Dependence { uint64_t file_number; uint64_t entry_count;
                    uint64_t separated_total_size; };
```
Target [include/rocksdb/types.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/rocksdb/types.h) ~L42-55:
```cpp
struct Dependence {
  uint64_t file_number; uint64_t entry_count;
  union { uint64_t byte_count; uint64_t separated_total_size; };
  // explicit constructors
};
```

### 9.2 FileMetaData

Reference [db/version_edit.h](file:///home/pengzhifeng.002/temp/terarkdb/db/version_edit.h): `size_antiquated`;
`raw_size() = raw_key_size + raw_value_size`.

Target [db/version_edit.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_edit.h#L140-191): `num_antiquation_bytes`;
`BlobGcAccountingBytes()` prefers `raw_key_size + raw_value_size`, falls back to
`fd.GetFileSize()` for legacy metadata, with explicit overflow saturation.

### 9.3 VersionBuilder online materialization

Reference [db/version_builder.cc](file:///home/pengzhifeng.002/temp/terarkdb/db/version_builder.cc) ~L131-132, L320-325, L402-452:
```cpp
size_depended += dependence.separated_total_size;
size_antiquated = raw_data_size - size_depended;
```
Target [db/version_builder.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_builder.cc) ~L129-130, L372-375, L447-489:
```cpp
bytes_depended += dependence.byte_count;
num_antiquation_bytes = BlobGcAccountingBytes() - bytes_depended;  // Clamp()
```

### 9.4 Version-level ratio

Reference [db/version_set.cc](file:///home/pengzhifeng.002/temp/terarkdb/db/version_set.cc) ~L2411-2417: maintains both
`entry_garbage_ratio_` and `size_garbage_ratio_`; picks `size_garbage_ratio_` when
exact mode is on.

Target [db/version_set.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_set.cc) ~L1853-1908:
```cpp
total_garbage_ratio_ = std::min(1.0,
  byte_precise_gc ? num_antiquation_bytes / blob_accounting_bytes
                  : num_antiquation     / num_entries);  // saturating throughout
```

### 9.5 Candidate scoring

Reference [db/compaction_picker.cc](file:///home/pengzhifeng.002/temp/terarkdb/db/compaction_picker.cc) ~L47:
`score = size_antiquated / raw_size()`.

Target [db/compaction_picker.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_picker.cc#L84-99):
```cpp
GarbageFileInfo::ComputeScore(f, precise):
  precise ? num_antiquation_bytes / BlobGcAccountingBytes()
          : num_antiquation       / num_entries;
```
Target also adds `blob_gc_diagnostics` logging and `selected_bytes` /
`selected_live_bytes` computation (~L905-1060).

### 9.6 Compaction dependence aggregation

Reference: per-SST dependence summary built directly.
Target [db/compaction_job.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_job.cc) uses a `DependenceAccumulator`
with saturating overflow handling plus a mixed `sst_type` validation guard. The
target [db/compaction_iterator.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_iterator.cc) feeds per-entry
separated `value_size` into the accumulator.

---

## 10. Serialization (MANIFEST / VersionEdit / table properties)

- Target [db/version_edit.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_edit.cc) removed `remote_value_size`; encodes
  `Dependence::byte_count` in the same tag slot as the reference
  `separated_total_size` (union keeps wire layout identical).
- Target [table/meta_blocks.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/meta_blocks.cc) and table_properties carry
  `separated_total_size` / `separated_entry_count` / `value_meta_total_size`.
- Keep forward/backward compatibility: legacy SSTs without raw key/value sizes must
  fall back to `fd.GetFileSize()` in `BlobGcAccountingBytes()`.

---

## 11. Metrics

Target [monitoring/statistics.h](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/monitoring/statistics.h) adds many new tickers
(drop-key-cache / hotness / byte-precise GC diagnostics) absent in the reference.
These are target-only and not required for functional parity, but porting code that
emits them must register the tickers first.

---

## 12. db_bench flags

Reference [tools/db_bench_tool.cc](file:///home/pengzhifeng.002/temp/terarkdb/tools/db_bench_tool.cc):
- `--use_delta_block` (~L1476, default true)
- `--open_exact_gc` parsed into the enum (~L4077-4079)
- `--blob_size` (~L1225); no `--enable_delta_separate` / `--middle_blob_size` /
  `--middle_combine_level`

Target [tools/db_bench_tool.cc](file:///home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/tools/db_bench_tool.cc):
- `--use_separated_value_meta_block` (~L514) + `--use_delta_block` alias (~L517)
- string-typed `--byte_precise_gc` / `--precise_gc` / `--open_exact_gc` folded into a
  bool (~L1061-1072, L3572-3605)
- `--enable_delta_separate` (~L998), `--middle_blob_size` (~L1002),
  `--middle_combine_level` (~L1007)
- `--hot_warm_blob_gc_max_files` controls the hot/warm vSST 100%-garbage policy;
  `0` disables it (~L1061, L3761)

---

## 13. Porting checklist (real logic, not pure rename)

1. Builder returns `Status` and guards oversized `value_meta` / overflow (§5).
2. Reader splits one typed call into three string-property accessors with safe OOB
   handling and checked varint decode (§6, §8).
3. `byte_precise_gc` collapses the 3-state enum, but does not imply the old
   `kExactGCUpdateValueSize` value-size-refresh mode; keep refresh disabled unless
   a separate switch deliberately re-homes it (§3.1).
4. Dual mirrored bools `byte_precise_gc` / `precise_gc` kept in sync on downgrade
   (§4.3).
5. `Dependence` third field is a `union`; preserve wire layout when serializing
   (§9.1, §10).
6. `BlobGcAccountingBytes()` legacy fallback + overflow saturation (§9.2).
7. All ratios use saturating arithmetic / `Clamp` (§9.3-9.5).
8. `DependenceAccumulator` with saturating overflow + mixed `sst_type` guard (§9.6).
9. Target-only surroundings (`enable_delta_separate` gate, drop-key cache, hotness,
   extra tickers) are not part of the two features but share files — port around
   them, do not delete them.
