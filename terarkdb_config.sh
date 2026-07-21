#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=${REPO_ROOT:-"${SCRIPT_DIR}"}
BUILD_DIR=${BUILD_DIR:-"${REPO_ROOT}/build"}

MB_SIZE=$((1024 * 1024))
GB_SIZE=$((1024 * MB_SIZE))

# Target workload: 50 GiB logical keyspace and eight overwrite threads.
DB_SIZE_BYTES=${DB_SIZE_BYTES:-$((50 * GB_SIZE))}
KEY_SIZE=${KEY_SIZE:-24}
VALUE_SIZE=${VALUE_SIZE:-1024}
NUM=${NUM:-$((DB_SIZE_BYTES / (KEY_SIZE + VALUE_SIZE)))}
FOREGROUND_THREADS=${FOREGROUND_THREADS:-8}
FILL_BENCHMARK=${FILL_BENCHMARK:-filluniquerandom}
FILL_THREADS=${FILL_THREADS:-1}
FILL_WRITES_PER_THREAD=${FILL_WRITES_PER_THREAD:-"${NUM}"}
OVERWRITE_WRITES_PER_THREAD=${OVERWRITE_WRITES_PER_THREAD:-"${NUM}"}
TARGET_LIVE_BYTES=$((NUM * (KEY_SIZE + VALUE_SIZE)))
FILL_WRITE_BYTES=$((FILL_WRITES_PER_THREAD * FILL_THREADS * (KEY_SIZE + VALUE_SIZE)))
OVERWRITE_WRITE_BYTES=$((OVERWRITE_WRITES_PER_THREAD * FOREGROUND_THREADS * (KEY_SIZE + VALUE_SIZE)))
MAX_PHASE_WRITE_BYTES=${FILL_WRITE_BYTES}
if ((OVERWRITE_WRITE_BYTES > MAX_PHASE_WRITE_BYTES)); then
  MAX_PHASE_WRITE_BYTES=${OVERWRITE_WRITE_BYTES}
fi

POST_OVERWRITE_DRAIN_SECONDS=${POST_OVERWRITE_DRAIN_SECONDS:-0}
POST_OVERWRITE_DRAIN_THREADS=${POST_OVERWRITE_DRAIN_THREADS:-1}
POST_OVERWRITE_DRAIN_READS=${POST_OVERWRITE_DRAIN_READS:-"${NUM}"}
POST_OVERWRITE_DRAIN_READ_RATE_LIMIT=${POST_OVERWRITE_DRAIN_READ_RATE_LIMIT:-1024}

# Threading: eight foreground threads and two flush threads.
FLUSH_THREADS=${FLUSH_THREADS:-2}
COMPACTION_THREADS=${COMPACTION_THREADS:-8}
GC_THREADS=${GC_THREADS:-2}
MAX_BACKGROUND_JOBS=${MAX_BACKGROUND_JOBS:-$((FLUSH_THREADS + COMPACTION_THREADS + GC_THREADS))}

# Size knobs: 4 memtables, 64MB memtable/kSST/vSST, 5 levels, L1=256MB.
MEMTABLE_SIZE=${MEMTABLE_SIZE:-$((64 * MB_SIZE))}
KSST_SIZE=${KSST_SIZE:-$((64 * MB_SIZE))}
VSST_SIZE=${VSST_SIZE:-$((64 * MB_SIZE))}
MAX_WRITE_BUFFER_NUMBER=${MAX_WRITE_BUFFER_NUMBER:-4}
NUM_LEVELS=${NUM_LEVELS:-5}
L1_SIZE=${L1_SIZE:-$((256 * MB_SIZE))}
OPEN_FILES=${OPEN_FILES:--1}

# Run exactly one GC policy per pipeline invocation. "baseline" is for
# dev.1.4, which does not recognize strategy-specific db_bench flags.
GC_STRATEGY=${GC_STRATEGY:-defer}
BLOB_GC_RATIO=${BLOB_GC_RATIO:-0.2}
BLOB_GC_DEFER_RATIO=${BLOB_GC_DEFER_RATIO:-0.65}
# Pipeline activation settings. The public Options defaults remain
# conservative; these values ensure the selected Ceph strategy runs during a
# finite A/B benchmark.
BLOB_GC_CEPH_MIN_WAIT_SECONDS=${BLOB_GC_CEPH_MIN_WAIT_SECONDS:-0}
BLOB_GC_CEPH_PROCESSOR_PERIOD_SECONDS=${BLOB_GC_CEPH_PROCESSOR_PERIOD_SECONDS:-1}
BLOB_GC_CEPH_MAX_FILES_PER_CYCLE=${BLOB_GC_CEPH_MAX_FILES_PER_CYCLE:-32}
case "${GC_STRATEGY}" in
  baseline|legacy|defer|ceph) ;;
  *)
    echo "GC_STRATEGY must be one of: baseline, legacy, defer, ceph" >&2
    exit 1
    ;;
esac
if [[ "${FILL_BENCHMARK}" != "filluniquerandom" ]]; then
  echo "terarkdb_config.sh requires FILL_BENCHMARK=filluniquerandom for exact key coverage" >&2
  exit 1
fi
if ((FILL_THREADS != 1)); then
  echo "terarkdb_config.sh requires FILL_THREADS=1 for filluniquerandom" >&2
  exit 1
fi
if ((FILL_WRITES_PER_THREAD != NUM)); then
  echo "terarkdb_config.sh requires FILL_WRITES_PER_THREAD=NUM for exact key coverage" >&2
  exit 1
fi
for drain_option in \
  "${POST_OVERWRITE_DRAIN_SECONDS}" \
  "${POST_OVERWRITE_DRAIN_THREADS}" \
  "${POST_OVERWRITE_DRAIN_READS}" \
  "${POST_OVERWRITE_DRAIN_READ_RATE_LIMIT}"; do
  if [[ ! "${drain_option}" =~ ^[0-9]+$ ]]; then
    echo "post-overwrite drain values must be non-negative integers" >&2
    exit 1
  fi
done
if ((POST_OVERWRITE_DRAIN_SECONDS > 0)) &&
   { ((POST_OVERWRITE_DRAIN_THREADS == 0)) ||
     ((POST_OVERWRITE_DRAIN_READS == 0)) ||
     ((POST_OVERWRITE_DRAIN_READ_RATE_LIMIT == 0)); }; then
  echo "enabled post-overwrite drain requires positive threads, reads, and read rate limit" >&2
  exit 1
fi

PIPELINE_DB_PATH="/usr/testdb"
DB_PATH=${DB_PATH:-"${PIPELINE_DB_PATH}"}
if [[ "${DB_PATH}" != "${PIPELINE_DB_PATH}" ]]; then
  echo "terarkdb_config.sh requires DB_PATH=${PIPELINE_DB_PATH}" >&2
  exit 1
fi
REQUIRED_FREE_SPACE_MULTIPLIER=${REQUIRED_FREE_SPACE_MULTIPLIER:-2}
KEEP_DB=${KEEP_DB:-false}
DRY_RUN=${DRY_RUN:-false}
BUILD_DB_BENCH=${BUILD_DB_BENCH:-true}

validate_pipeline_db() {
  local parent_dir
  local available_bytes
  local required_bytes

  parent_dir=$(dirname "${DB_PATH}")
  if [[ ! -d "${parent_dir}" ]]; then
    echo "pipeline DB parent does not exist: ${parent_dir}" >&2
    exit 1
  fi
  if [[ -e "${DB_PATH}" && ! -d "${DB_PATH}" ]]; then
    echo "pipeline DB path is not a directory: ${DB_PATH}" >&2
    exit 1
  fi
  if [[ "${DRY_RUN}" != "true" ]] &&
     { [[ -d "${DB_PATH}" && ! -w "${DB_PATH}" ]] ||
       [[ ! -d "${DB_PATH}" && ! -w "${parent_dir}" ]]; }; then
    echo "pipeline DB path is not writable: ${DB_PATH}" >&2
    exit 1
  fi

  available_bytes=$(df --output=avail -B1 "${parent_dir}" | tail -n 1)
  required_bytes=$((MAX_PHASE_WRITE_BYTES * REQUIRED_FREE_SPACE_MULTIPLIER))
  if ((available_bytes < required_bytes)); then
    echo "insufficient free space under ${parent_dir}: need ${required_bytes} bytes for the largest write phase, have ${available_bytes}" >&2
    exit 1
  fi
}
validate_pipeline_db

find_db_bench() {
  local candidates=(
    "${DB_BENCH_BIN:-}"
    "${BUILD_DIR}/tools/db_bench"
    "${BUILD_DIR}/db_bench"
    "${REPO_ROOT}/build/tools/db_bench"
    "${REPO_ROOT}/build/db_bench"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -n "${candidate}" && -x "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

if [[ "${BUILD_DB_BENCH}" == "true" ]]; then
  cmake --build "${BUILD_DIR}" --target db_bench ldb \
    --parallel "${BUILD_PARALLEL:-64}"
fi

DB_BENCH_BIN=$(find_db_bench || true)
if [[ -z "${DB_BENCH_BIN}" ]]; then
  echo "db_bench binary not found. Set DB_BENCH_BIN or BUILD_DIR." >&2
  exit 1
fi

find_ldb() {
  local candidates=(
    "${LDB_BIN:-}"
    "${BUILD_DIR}/tools/ldb"
    "${BUILD_DIR}/ldb"
    "${REPO_ROOT}/build/tools/ldb"
    "${REPO_ROOT}/build/ldb"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -n "${candidate}" && -x "${candidate}" ]]; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

LDB_BIN=$(find_ldb || true)
if [[ -z "${LDB_BIN}" ]]; then
  echo "ldb binary not found. Set LDB_BIN or BUILD_DIR." >&2
  exit 1
fi

declare -A DB_BENCH_FLAGS=()
load_db_bench_flags() {
  local flag_names

  if ! flag_names=$(
    { "${DB_BENCH_BIN}" --helpxml 2>/dev/null || true; } |
      python3 -c 'import sys
import xml.etree.ElementTree as ET
try:
    root = ET.fromstring(sys.stdin.read())
except ET.ParseError:
    sys.exit(1)
for flag in root.findall(".//flag"):
    name = flag.findtext("name")
    if name:
        print(name)'
  ); then
    echo "unable to enumerate db_bench flags; build db_bench with gflags enabled" >&2
    exit 1
  fi
  if [[ -z "${flag_names}" ]]; then
    echo "db_bench reported no flags" >&2
    exit 1
  fi

  while IFS= read -r flag_name; do
    DB_BENCH_FLAGS["${flag_name}"]=1
  done <<<"${flag_names}"
}
load_db_bench_flags

has_flag() {
  local name=$1
  [[ -n "${DB_BENCH_FLAGS[${name}]+x}" ]]
}

add_flag() {
  local -n flag_out=$1
  local name=$2
  local value=$3
  if has_flag "${name}"; then
    flag_out+=("--${name}=${value}")
  else
    echo "required db_bench flag not found: --${name}" >&2
    exit 1
  fi
}

base_flags() {
  local -n base_out=$1
  add_flag base_out db "${DB_PATH}"
  add_flag base_out num "${NUM}"
  add_flag base_out key_size "${KEY_SIZE}"
  add_flag base_out value_size "${VALUE_SIZE}"
  add_flag base_out write_buffer_size "${MEMTABLE_SIZE}"
  add_flag base_out target_file_size_base "${KSST_SIZE}"
  add_flag base_out target_blob_file_size "${VSST_SIZE}"
  add_flag base_out max_write_buffer_number "${MAX_WRITE_BUFFER_NUMBER}"
  add_flag base_out max_background_flushes "${FLUSH_THREADS}"
  add_flag base_out max_background_compactions "${COMPACTION_THREADS}"
  add_flag base_out max_background_jobs "${MAX_BACKGROUND_JOBS}"
  add_flag base_out num_high_pri_threads "${FLUSH_THREADS}"
  add_flag base_out num_low_pri_threads "${COMPACTION_THREADS}"
  add_flag base_out num_bottom_pri_threads "${GC_THREADS}"
  add_flag base_out disable_wal true
  add_flag base_out sync false
  add_flag base_out statistics true
  add_flag base_out histogram true
  add_flag base_out stats_interval_seconds 60
  add_flag base_out compression_type none
  add_flag base_out use_terark_table false
  add_flag base_out num_levels "${NUM_LEVELS}"
  add_flag base_out block_size 16384
  add_flag base_out open_files "${OPEN_FILES}"
  add_flag base_out bloom_bits 10
  add_flag base_out memtablerep skip_list
  add_flag base_out level_compaction_dynamic_level_bytes true
  add_flag base_out max_bytes_for_level_base "${L1_SIZE}"
  add_flag base_out max_bytes_for_level_multiplier 10
  add_flag base_out level0_file_num_compaction_trigger 4
  add_flag base_out level0_slowdown_writes_trigger 21
  add_flag base_out level0_stop_writes_trigger 36
  add_flag base_out subcompactions 1
  add_flag base_out blob_size 512
  add_flag base_out blob_gc_ratio "${BLOB_GC_RATIO}"
  case "${GC_STRATEGY}" in
    baseline)
      ;;
    legacy)
      add_flag base_out blob_gc_defer_enabled false
      ;;
    defer)
      add_flag base_out blob_gc_defer_enabled true
      add_flag base_out blob_gc_defer_ratio "${BLOB_GC_DEFER_RATIO}"
      ;;
    ceph)
      add_flag base_out blob_gc_defer_enabled false
      add_flag base_out blob_gc_ceph_enabled true
      add_flag base_out blob_gc_ceph_min_wait_seconds \
        "${BLOB_GC_CEPH_MIN_WAIT_SECONDS}"
      add_flag base_out blob_gc_ceph_processor_period_seconds \
        "${BLOB_GC_CEPH_PROCESSOR_PERIOD_SECONDS}"
      add_flag base_out blob_gc_ceph_max_files_per_cycle \
        "${BLOB_GC_CEPH_MAX_FILES_PER_CYCLE}"
      ;;
  esac
  add_flag base_out max_dependence_blob_overlap 1024
  add_flag base_out maintainer_job_ratio 0
  add_flag base_out verify_checksum true
}

run_db_bench() {
  local benchmark=$1
  local use_existing_db=$2
  local operations_per_thread=$3
  local threads=$4
  local duration_seconds=${5:-0}
  local gc_metrics_mode=${6:-none}
  local read_rate_limit=${7:-0}
  local args=()
  base_flags args
  add_flag args threads "${threads}"
  add_flag args benchmarks "${benchmark}"
  add_flag args use_existing_db "${use_existing_db}"
  if [[ "${benchmark}" == "readrandom" ]]; then
    add_flag args reads "${operations_per_thread}"
  else
    add_flag args writes "${operations_per_thread}"
  fi
  if ((duration_seconds > 0)); then
    add_flag args duration "${duration_seconds}"
  fi
  if ((read_rate_limit > 0)); then
    add_flag args benchmark_read_rate_limit "${read_rate_limit}"
  fi

  echo
  echo "===== db_bench ${benchmark} ====="
  echo "DB_BENCH_BIN=${DB_BENCH_BIN}"
  echo "DB_PATH=${DB_PATH}"
  echo "TARGET_LIVE_BYTES=${TARGET_LIVE_BYTES} NUM=${NUM}"
  if [[ "${benchmark}" == "readrandom" ]]; then
    echo "THREADS=${threads} READS_PER_THREAD=${operations_per_thread} DURATION_SECONDS=${duration_seconds} READ_RATE_LIMIT=${read_rate_limit}"
  else
    echo "THREADS=${threads} WRITES_PER_THREAD=${operations_per_thread} TOTAL_WRITES=$((operations_per_thread * threads))"
  fi
  printf ' %q' "${DB_BENCH_BIN}" "${args[@]}"
  echo

  if [[ "${DRY_RUN}" == "true" ]]; then
    return 0
  fi

  local benchmark_output
  benchmark_output=$(mktemp)
  if ! "${DB_BENCH_BIN}" "${args[@]}" 2>&1 | tee "${benchmark_output}"; then
    rm -f "${benchmark_output}"
    return 1
  fi

  local no_file_errors
  if ! no_file_errors=$(awk '/^rocksdb\.no\.file\.errors COUNT/ { count = $NF }
                             END { if (count == "") exit 1; print count }' \
      "${benchmark_output}"); then
    rm -f "${benchmark_output}"
    echo "benchmark ${benchmark} did not report no_file_errors" >&2
    return 1
  fi
  if [[ -z "${no_file_errors}" || "${no_file_errors}" != "0" ]]; then
    rm -f "${benchmark_output}"
    echo "benchmark ${benchmark} reported no_file_errors=${no_file_errors:-unknown}" >&2
    return 1
  fi
  echo "no_file_errors=0 benchmark=${benchmark}"

  if [[ "${gc_metrics_mode}" != "none" ]]; then
    local gc_get_keys
    local gc_touch_files
    local gc_skip_by_seqno
    local gc_skip_by_file_meta
    if ! gc_get_keys=$(awk '/^rocksdb\.num\.gc\.get_keys COUNT/ { count = $NF }
                            END { if (count == "") exit 1; print count }' \
        "${benchmark_output}"); then
      rm -f "${benchmark_output}"
      echo "benchmark ${benchmark} did not report gc_get_keys" >&2
      return 1
    fi
    if ! gc_touch_files=$(awk '/^rocksdb\.num\.gc\.touch_files COUNT/ { count = $NF }
                               END { if (count == "") exit 1; print count }' \
        "${benchmark_output}"); then
      rm -f "${benchmark_output}"
      echo "benchmark ${benchmark} did not report gc_touch_files" >&2
      return 1
    fi
    if ! gc_skip_by_seqno=$(awk '/^rocksdb\.num\.gc\.skip_by_seqno COUNT/ { count = $NF }
                                  END { if (count == "") exit 1; print count }' \
        "${benchmark_output}"); then
      rm -f "${benchmark_output}"
      echo "benchmark ${benchmark} did not report gc_skip_by_seqno" >&2
      return 1
    fi
    if ! gc_skip_by_file_meta=$(awk '/^rocksdb\.num\.gc\.skip_by_file_meta COUNT/ { count = $NF }
                                     END { if (count == "") exit 1; print count }' \
        "${benchmark_output}"); then
      rm -f "${benchmark_output}"
      echo "benchmark ${benchmark} did not report gc_skip_by_file_meta" >&2
      return 1
    fi
    rm -f "${benchmark_output}"
    if [[ "${gc_metrics_mode}" == "required" ]]; then
      if [[ -z "${gc_get_keys}" || -z "${gc_touch_files}" ||
            -z "${gc_skip_by_seqno}" || -z "${gc_skip_by_file_meta}" ]]; then
        echo "benchmark ${benchmark} did not exercise GC: gc_get_keys=${gc_get_keys:-unknown} gc_touch_files=${gc_touch_files:-unknown} gc_skip_by_seqno=${gc_skip_by_seqno:-unknown} gc_skip_by_file_meta=${gc_skip_by_file_meta:-unknown}" >&2
        return 1
      fi
      if ((gc_get_keys + gc_touch_files + gc_skip_by_seqno +
              gc_skip_by_file_meta == 0)); then
        echo "benchmark ${benchmark} did not exercise GC: gc_get_keys=${gc_get_keys} gc_touch_files=${gc_touch_files} gc_skip_by_seqno=${gc_skip_by_seqno} gc_skip_by_file_meta=${gc_skip_by_file_meta}" >&2
        return 1
      fi
      echo "gc_activity=1 gc_get_keys=${gc_get_keys} gc_touch_files=${gc_touch_files} gc_skip_by_seqno=${gc_skip_by_seqno} gc_skip_by_file_meta=${gc_skip_by_file_meta}"
    else
      echo "gc_drain_get_keys=${gc_get_keys} gc_drain_touch_files=${gc_touch_files} gc_drain_skip_by_seqno=${gc_skip_by_seqno} gc_drain_skip_by_file_meta=${gc_skip_by_file_meta}"
    fi
  else
    rm -f "${benchmark_output}"
  fi
}

verify_db() {
  echo
  echo "===== ldb checkconsistency ====="
  printf ' %q' "${LDB_BIN}" "--db=${DB_PATH}" checkconsistency
  echo

  echo
  echo "===== ldb dump --count_only ====="
  printf ' %q' "${LDB_BIN}" "--db=${DB_PATH}" dump --count_only
  echo

  if [[ "${DRY_RUN}" == "true" ]]; then
    return 0
  fi

  "${LDB_BIN}" "--db=${DB_PATH}" checkconsistency
  echo "ldb_checkconsistency=1"

  local count_output
  local keys_covered
  count_output=$("${LDB_BIN}" "--db=${DB_PATH}" dump --count_only)
  printf '%s\n' "${count_output}"
  keys_covered=$(printf '%s\n' "${count_output}" |
    awk '/^Keys in range:/ { print $4 }')
  if [[ "${keys_covered}" != "${NUM}" ]]; then
    echo "coverage_complete=0 keys_covered=${keys_covered:-unknown} expected_keys=${NUM}" >&2
    exit 1
  fi
  echo "coverage_complete=1 keys_covered=${keys_covered}"
}

report_db_footprint() {
  local label=${1:-}
  if [[ "${DRY_RUN}" == "true" ]]; then
    return 0
  fi

  local db_footprint_bytes
  db_footprint_bytes=$(du -sb "${DB_PATH}" | awk '{ print $1 }')
  if [[ -n "${label}" ]]; then
    echo "db_footprint_${label}_bytes=${db_footprint_bytes}"
  else
    echo "db_footprint_bytes=${db_footprint_bytes}"
  fi
}

cleanup() {
  if [[ "${KEEP_DB}" != "true" && -d "${DB_PATH}" ]]; then
    find "${DB_PATH}" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  fi
}
if [[ "${DRY_RUN}" != "true" ]]; then
  trap cleanup EXIT
  mkdir -p "${DB_PATH}"
  cleanup
fi

run_db_bench "${FILL_BENCHMARK}" false "${FILL_WRITES_PER_THREAD}" \
  "${FILL_THREADS}" 0 none
run_db_bench overwrite true "${OVERWRITE_WRITES_PER_THREAD}" \
  "${FOREGROUND_THREADS}" 0 required
if ((POST_OVERWRITE_DRAIN_SECONDS > 0)); then
  report_db_footprint "before_drain"
  echo "post_overwrite_drain_seconds=${POST_OVERWRITE_DRAIN_SECONDS}"
  echo "post_overwrite_drain_read_rate_limit=${POST_OVERWRITE_DRAIN_READ_RATE_LIMIT}"
  run_db_bench readrandom true "${POST_OVERWRITE_DRAIN_READS}" \
    "${POST_OVERWRITE_DRAIN_THREADS}" \
    "${POST_OVERWRITE_DRAIN_SECONDS}" optional \
    "${POST_OVERWRITE_DRAIN_READ_RATE_LIMIT}"
fi
verify_db
report_db_footprint

echo
echo "devbox db_bench pipeline finished."
