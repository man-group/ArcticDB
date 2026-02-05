# Async Module

The async module (`cpp/arcticdb/async/`) manages asynchronous task execution and thread pools for storage I/O operations.

## Overview

This module provides:
- Task scheduler singleton
- CPU and I/O thread pool management
- Async storage operations

## Task Scheduler

### Location

`cpp/arcticdb/async/task_scheduler.hpp`

### Purpose

Central singleton managing all async task execution.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     TaskScheduler                            │
│                                                              │
│  ┌─────────────────────────┐  ┌──────────────────────────┐  │
│  │     CPU Thread Pool     │  │     I/O Thread Pool      │  │
│  │                         │  │                          │  │
│  │  ┌───┐ ┌───┐ ┌───┐     │  │  ┌───┐ ┌───┐ ┌───┐      │  │
│  │  │ T │ │ T │ │ T │ ... │  │  │ T │ │ T │ │ T │ ...  │  │
│  │  └───┘ └───┘ └───┘     │  │  └───┘ └───┘ └───┘      │  │
│  │                         │  │                          │  │
│  │  Compute-bound tasks    │  │  Storage I/O tasks       │  │
│  └─────────────────────────┘  └──────────────────────────┘  │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                   Task Queue                         │    │
│  │   [task1] [task2] [task3] [task4] ...               │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### Key Methods

All tasks submitted must derive from `BaseTask`. See `cpp/arcticdb/async/base_task.hpp` for the task base class and `cpp/arcticdb/async/tasks.hpp` for task type definitions.

## Thread Pools

### CPU Thread Pool

For compute-intensive operations:
- Data compression/decompression
- Expression evaluation
- Aggregation calculations

### I/O Thread Pool

For storage backend operations:
- Storage reads/writes (S3, Azure, LMDB, MongoDB, Memory backends)
- Version map reload operations (`CheckReloadTask`)

## Async Store

### Location

`cpp/arcticdb/async/async_store.hpp`

### Purpose

Async wrapper around storage operations using Folly futures.

## Configuration

### Thread Counts

| Pool | Default Count | Config Override |
|------|---------------|-----------------|
| CPU | `hardware_concurrency()` (cgroup-aware) | `VersionStore.NumCPUThreads` |
| I/O | `CPU threads × 1.5` | `VersionStore.NumIOThreads` |

The CPU count respects cgroup CPU limits (v1 and v2) for containerized environments.

## Key Files

| File | Purpose |
|------|---------|
| `task_scheduler.hpp` | TaskScheduler singleton |
| `task_scheduler.cpp` | Implementation |
| `async_store.hpp` | Async storage wrapper |
| `tasks.hpp` | Task type definitions |
| `base_task.hpp` | Task base class |

## Performance Considerations

### Thread Pool Sizing

- CPU pool: Match physical cores for compute tasks
- I/O pool: Over-provision (1.5x CPU) to hide storage latency
- Monitor thread utilization to tune

### Task Granularity

- Too fine-grained: Scheduling overhead dominates
- Too coarse-grained: Poor parallelism

## Related Documentation

- [PIPELINE.md](PIPELINE.md) - Uses async for parallel I/O
- [STORAGE_BACKENDS.md](STORAGE_BACKENDS.md) - Storage being accessed
