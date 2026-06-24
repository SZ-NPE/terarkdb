# /data00 fio baseline

Date: 2026-06-22

## Target disk

- Device: `/dev/vdb`
- Filesystem: `ext4`
- Mount point: `/data00`
- Capacity at test time: `492G`
- Used at test time: `125G`
- Available at test time: `342G`

Note: the test file was created under `/data00/home/pengzhifeng.002/` because the current user does not have write permission on the `/data00` root directory. The data path is still on the same `/data00` mount, so the result is valid for `/dev/vdb`.

## fio commands

Random write 10GB, 1MB block size:

```bash
fio --name=data00_randwrite_10g_1m \
  --filename=/data00/home/pengzhifeng.002/fio_data00_probe_10g_1m.dat \
  --size=10G \
  --bs=1M \
  --rw=randwrite \
  --ioengine=libaio \
  --direct=1 \
  --iodepth=32 \
  --numjobs=1 \
  --group_reporting \
  --randrepeat=0 \
  --output-format=json \
  --output=/tmp/data00_randwrite_10g_1m.json
```

Random read 10GB, 1MB block size:

```bash
fio --name=data00_randread_10g_1m \
  --filename=/data00/home/pengzhifeng.002/fio_data00_probe_10g_1m.dat \
  --size=10G \
  --bs=1M \
  --rw=randread \
  --ioengine=libaio \
  --direct=1 \
  --iodepth=32 \
  --numjobs=1 \
  --group_reporting \
  --randrepeat=0 \
  --output-format=json \
  --output=/tmp/data00_randread_10g_1m.json
```

## Summary

| Workload | Size | Block size | BW | IOPS | Runtime | Mean latency | p99 clat |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| randwrite | 10G | 1M | 157958 KiB/s (154.26 MiB/s) | 154.26 | 66.383 s | 207.44 ms | 943.72 ms |
| randread | 10G | 1M | 158875 KiB/s (155.15 MiB/s) | 155.15 | 66.000 s | 206.23 ms | 826.28 ms |

## Raw key metrics

### Random write

- io_bytes: `10737418240`
- bw_bytes: `161749517`
- bw: `157958 KiB/s`
- iops: `154.256361`
- runtime: `66383 ms`
- lat_ns.mean: `207436070.876171`
- clat_ns.percentile.99.000000: `943718400`

### Random read

- io_bytes: `10737418240`
- bw_bytes: `162688155`
- bw: `158875 KiB/s`
- iops: `155.151515`
- runtime: `66000 ms`
- lat_ns.mean: `206225760.396973`
- clat_ns.percentile.99.000000: `826277888`

## Notes

- `direct=1` was used to reduce page cache effects.
- `iodepth=32` was used, so latency reflects queued async IO rather than single-request service time.
- The temporary 10GB test file `/data00/home/pengzhifeng.002/fio_data00_probe_10g_1m.dat` has been removed after the test.
- Raw fio JSON outputs were generated at:
  - `/tmp/data00_randwrite_10g_1m.json`
  - `/tmp/data00_randread_10g_1m.json`
