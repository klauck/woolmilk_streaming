# Windowing Benchmark Guide

## Prerequisites

- For plotting, install `matplotlib`
- The `nexmark` generator must be on your `PATH` or accessible via `--nexmark-bin`

## Benchmark script

`scripts/benchmark_windowing.py`

### Basic usage

```bash
python scripts/benchmark_windowing.py --records 1000000 --chunk-size 10000 --window-size 10 --window-slide 2
```

### Optionally run a SQL query on each window

The query runs against a table named `window_table`. Query time is included in `processing_seconds` and also reported as `query_seconds`.

```bash
python scripts/benchmark_windowing.py \
  --records 1000000 \
  --chunk-size 10000 \
  --window-size 10 \
  --window-slide 2 \
  --query "SELECT COUNT(*) FROM window_table"
```

### Sweep parameters

Compare effect of changing different parameters. Produces cartesian product of all parameters. 

```bash
python scripts/benchmark_windowing.py \
  --sweep-records 100000,500000,1000000 \
  --sweep-chunk-size 10000,100000 \
  --sweep-window-size 5,10 \
  --sweep-window-slide 2,5
```

## Plotting script

`scripts/plot_windowing_sweep.py`

Sweeps only record count, keeps chunk size fixed at 10000, and plots:

- Peak RSS in MB
- Processing time in seconds
- Per-point annotations with windows and input size (MB)

```bash
python scripts/plot_windowing_sweep.py \
  --start-records 100000 \
  --step-records 100000 \
  --steps 10 \
  --window-size 10 \
  --window-slide 2 \
  --output windowing_sweep.png
```
