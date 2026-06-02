## 顺序写 1M
pzf@client:~/workplace/terarkdb/log_rocksdb$ fio --name=seq_write_1k --ioengine=libaio --direct=1 --rw=write --bs=1M --iodepth=64 --size=100g --fil
ename=/storage/testfile --runtime=600 --time_based
seq_write_1k: (g=0): rw=write, bs=(R) 1024KiB-1024KiB, (W) 1024KiB-1024KiB, (T) 1024KiB-1024KiB, ioengine=libaio, iodepth=64
fio-3.16
Starting 1 process
Jobs: 1 (f=1): [W(1)][100.0%][w=2505MiB/s][w=2504 IOPS][eta 00m:00s]
seq_write_1k: (groupid=0, jobs=1): err= 0: pid=432776: Thu May 28 15:04:34 2026
  write: IOPS=2992, BW=2993MiB/s (3138MB/s)(1753GiB/600026msec); 0 zone resets
    slat (usec): min=55, max=26752, avg=118.78, stdev=88.36
    clat (usec): min=5256, max=58947, avg=21266.81, stdev=2389.80
     lat (usec): min=5344, max=59087, avg=21385.83, stdev=2390.42
    clat percentiles (usec):
     |  1.00th=[17171],  5.00th=[18744], 10.00th=[19006], 20.00th=[19792],
     | 30.00th=[20055], 40.00th=[20579], 50.00th=[20841], 60.00th=[21103],
     | 70.00th=[21627], 80.00th=[22414], 90.00th=[23987], 95.00th=[26084],
     | 99.00th=[29492], 99.50th=[30540], 99.90th=[33424], 99.95th=[36439],
     | 99.99th=[54264]
   bw (  MiB/s): min= 2155, max= 3484, per=100.00%, avg=2992.36, stdev=206.60, samples=1200
   iops        : min= 2155, max= 3484, avg=2992.35, stdev=206.60, samples=1200
  lat (msec)   : 10=0.05%, 20=27.15%, 50=72.78%, 100=0.01%
  cpu          : usr=18.83%, sys=18.13%, ctx=1651743, majf=0, minf=30
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued rwts: total=0,1795581,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
  WRITE: bw=2993MiB/s (3138MB/s), 2993MiB/s-2993MiB/s (3138MB/s-3138MB/s), io=1753GiB (1883GB), run=600026-600026msec

Disk stats (read/write):
  nvme1n1: ios=0/14360728, merge=0/274, ticks=0/195828775, in_queue=172555868, util=100.00%

## 顺序读 1M
pzf@client:~/workplace/terarkdb/log_rocksdb$ fio --name=seq_read_1M --ioengine=libaio --direct=1 --rw=read --bs=1M --iodepth=64 --size=100g --filen
ame=/storage/testfile --runtime=600 --time_based
seq_read_1M: (g=0): rw=read, bs=(R) 1024KiB-1024KiB, (W) 1024KiB-1024KiB, (T) 1024KiB-1024KiB, ioengine=libaio, iodepth=64
fio-3.16
Starting 1 process
Jobs: 1 (f=1): [R(1)][100.0%][r=6020MiB/s][r=6020 IOPS][eta 00m:00s]
seq_read_1M: (groupid=0, jobs=1): err= 0: pid=435814: Thu May 28 15:56:23 2026
  read: IOPS=6114, BW=6115MiB/s (6412MB/s)(3583GiB/600009msec)
    slat (usec): min=37, max=2487, avg=64.26, stdev=14.67
    clat (usec): min=3195, max=47453, avg=10401.34, stdev=841.99
     lat (usec): min=3259, max=48195, avg=10465.75, stdev=841.88
    clat percentiles (usec):
     |  1.00th=[ 7308],  5.00th=[ 8455], 10.00th=[ 9634], 20.00th=[10290],
     | 30.00th=[10421], 40.00th=[10421], 50.00th=[10552], 60.00th=[10552],
     | 70.00th=[10683], 80.00th=[10814], 90.00th=[10945], 95.00th=[11207],
     | 99.00th=[12518], 99.50th=[13173], 99.90th=[14484], 99.95th=[15008],
     | 99.99th=[15795]
   bw (  MiB/s): min= 5880, max= 6636, per=99.99%, avg=6114.09, stdev=187.18, samples=1200
   iops        : min= 5880, max= 6636, avg=6114.06, stdev=187.19, samples=1200
  lat (msec)   : 4=0.01%, 10=13.81%, 20=86.18%, 50=0.01%
  cpu          : usr=1.17%, sys=40.65%, ctx=2457084, majf=0, minf=32854
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued rwts: total=3668834,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=6115MiB/s (6412MB/s), 6115MiB/s-6115MiB/s (6412MB/s-6412MB/s), io=3583GiB (3847GB), run=600009-600009msec

Disk stats (read/write):
  nvme1n1: ios=29344072/3, merge=0/1, ticks=201099032/1, in_queue=138945328, util=100.00%

## 随机写 1M
pzf@client:~/workplace/terarkdb/log_rocksdb$ fio --name=rand_write_1M --ioengine=libaio --direct=1 --rw=randwrite --bs=1M --iodepth=64 --size=100g --filename=/storage/testfile --runtime=600 --time_based
rand_write_1M: (g=0): rw=randwrite, bs=(R) 1024KiB-1024KiB, (W) 1024KiB-1024KiB, (T) 1024KiB-1024KiB, ioengine=libaio, iodepth=64
fio-3.16
Starting 1 process
Jobs: 1 (f=1): [w(1)][100.0%][w=3034MiB/s][w=3034 IOPS][eta 00m:00s]
rand_write_1M: (groupid=0, jobs=1): err= 0: pid=439349: Thu May 28 16:19:03 2026
  write: IOPS=2950, BW=2951MiB/s (3094MB/s)(1729GiB/600021msec); 0 zone resets
    slat (usec): min=57, max=34419, avg=131.51, stdev=107.60
    clat (usec): min=5443, max=71307, avg=21557.54, stdev=2929.56
     lat (usec): min=5528, max=71451, avg=21689.29, stdev=2930.88
    clat percentiles (usec):
     |  1.00th=[17695],  5.00th=[19006], 10.00th=[19530], 20.00th=[19792],
     | 30.00th=[20317], 40.00th=[20579], 50.00th=[20841], 60.00th=[21365],
     | 70.00th=[21627], 80.00th=[22414], 90.00th=[24249], 95.00th=[26870],
     | 99.00th=[33162], 99.50th=[35914], 99.90th=[47449], 99.95th=[50070],
     | 99.99th=[58983]
   bw (  MiB/s): min= 1316, max= 3384, per=99.99%, avg=2950.35, stdev=269.47, samples=1200
   iops        : min= 1316, max= 3384, avg=2950.32, stdev=269.46, samples=1200
  lat (msec)   : 10=0.09%, 20=22.20%, 50=77.66%, 100=0.05%
  cpu          : usr=21.50%, sys=18.68%, ctx=1601590, majf=0, minf=24
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued rwts: total=0,1770426,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
  WRITE: bw=2951MiB/s (3094MB/s), 2951MiB/s-2951MiB/s (3094MB/s-3094MB/s), io=1729GiB (1856GB), run=600021-600021msec

Disk stats (read/write):
  nvme1n1: ios=0/14159421, merge=0/116, ticks=0/180220891, in_queue=159824188, util=100.00%

## 随机读 1M
pzf@client:~/workplace/terarkdb/log_rocksdb$ fio --name=rand_read_1M --ioengine=libaio --direct=1 --rw=randread --bs=1M --iodepth=64 --size=100g --
filename=/storage/testfile --runtime=600 --time_based
rand_read_1M: (g=0): rw=randread, bs=(R) 1024KiB-1024KiB, (W) 1024KiB-1024KiB, (T) 1024KiB-1024KiB, ioengine=libaio, iodepth=64
fio-3.16
Starting 1 process
Jobs: 1 (f=1): [r(1)][100.0%][r=5166MiB/s][r=5166 IOPS][eta 00m:00s]
rand_read_1M: (groupid=0, jobs=1): err= 0: pid=441901: Thu May 28 16:34:28 2026
  read: IOPS=6168, BW=6169MiB/s (6468MB/s)(3615GiB/600013msec)
    slat (usec): min=37, max=1142, avg=63.43, stdev=14.33
    clat (usec): min=3081, max=51606, avg=10310.35, stdev=1344.56
     lat (usec): min=3133, max=51707, avg=10373.92, stdev=1344.83
    clat percentiles (usec):
     |  1.00th=[ 7767],  5.00th=[ 8291], 10.00th=[ 8586], 20.00th=[ 9110],
     | 30.00th=[ 9634], 40.00th=[10028], 50.00th=[10290], 60.00th=[10552],
     | 70.00th=[10814], 80.00th=[11207], 90.00th=[11994], 95.00th=[12780],
     | 99.00th=[14091], 99.50th=[14615], 99.90th=[15664], 99.95th=[16057],
     | 99.99th=[17171]
   bw (  MiB/s): min= 4874, max= 6622, per=100.00%, avg=6168.49, stdev=269.48, samples=1200
   iops        : min= 4874, max= 6622, avg=6168.48, stdev=269.48, samples=1200
  lat (msec)   : 4=0.01%, 10=40.41%, 20=59.59%, 50=0.01%, 100=0.01%
  cpu          : usr=1.26%, sys=40.45%, ctx=2445129, majf=0, minf=32896
  IO depths    : 1=0.1%, 2=0.1%, 4=0.1%, 8=0.1%, 16=0.1%, 32=0.1%, >=64=100.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.1%, >=64=0.0%
     issued rwts: total=3701314,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=6169MiB/s (6468MB/s), 6169MiB/s-6169MiB/s (6468MB/s-6468MB/s), io=3615GiB (3881GB), run=600013-600013msec

Disk stats (read/write):
  nvme1n1: ios=29605077/3, merge=0/1, ticks=193226555/1, in_queue=139364316, util=100.00%
