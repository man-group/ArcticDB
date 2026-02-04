# Async Module

The async module (`cpp/arcticdb/async/`) manages asynchronous task execution and thread pools.

## Overview

This module provides:
- Task scheduler singleton
- CPU and I/O thread pool management
- Async storage operations
- Task composition and chaining

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

```cpp
class TaskScheduler {
public:
    // Get singleton instance (returns pointer)
    static TaskScheduler* instance();

    // Submit CPU-bound task (task must derive from BaseTask)
    template<class Task>
    auto submit_cpu_task(Task&& task) -> folly::Future<...>;

    // Submit I/O task (task must derive from BaseTask)
    template<class Task>
    auto submit_io_task(Task&& task) -> folly::Future<...>;

    // Thread pool executors
    SchedulerWrapper<CPUSchedulerType>& cpu_exec();
    SchedulerWrapper<IOSchedulerType>& io_exec();
};
```

Note: All tasks submitted must derive from `BaseTask`. Thread pool sizes are configured via `ConfigsMap` settings or at initialization time.

## Thread Pools

### CPU Thread Pool

For compute-intensive operations:
- Data compression/decompression
- Expression evaluation
- Aggregation calculations

```cpp
// Submit CPU-bound work (task must derive from BaseTask)
auto future = TaskScheduler::instance()->submit_cpu_task(EncodeAtomTask{...});
```

### I/O Thread Pool

For storage backend operations:
- Storage reads/writes (S3, Azure, LMDB, MongoDB, Memory backends)
- Version map reload operations (`CheckReloadTask`)
- Key existence checks, removes, object size calculations

```cpp
// Submit I/O work
auto future = TaskScheduler::instance()->submit_io_task(MyStorageTask{...});
```

## Async Store

### Location

`cpp/arcticdb/async/async_store.hpp`

### Purpose

Async wrapper around storage operations.

### Key Methods

```cpp
class AsyncStore {
public:
    // Async read - returns key-segment pair
    folly::Future<std::pair<VariantKey, Segment>> read(
        const VariantKey& key,
        const ReadOptions& opts = ReadOptions{}
    );

    // Async write
    folly::Future<VariantKey> write(
        KeyType key_type,
        const StreamId& stream_id,
        Segment&& segment
    );

    // Multiple reads are submitted individually and collected
    // using folly::collectAll()
};
```

## Task Types

### Location

`cpp/arcticdb/async/tasks.hpp`

### Common Task Types

```cpp
// Read task
struct ReadTask {
    VariantKey key_;
    std::shared_ptr<Store> store_;

    Segment operator()() const {
        return store_->read(key_);
    }
};

// Write task
struct WriteTask {
    VariantKey key_;
    Segment segment_;
    std::shared_ptr<Store> store_;

    void operator()() const {
        store_->write(key_, std::move(segment_));
    }
};

// Decode task
struct DecodeTask {
    Segment segment_;

    SegmentInMemory operator()() const {
        return decode(std::move(segment_));
    }
};
```

## Future Composition

### Using Folly Futures

```cpp
#include <folly/futures/Future.h>

// Chain operations
auto result = submit_io_task([&]{ return storage.read(key); })
    .thenValue([](Segment seg) {
        return decode(std::move(seg));
    })
    .thenValue([](SegmentInMemory mem_seg) {
        return process(std::move(mem_seg));
    });

// Wait for result
auto final_result = result.get();
```

### Parallel Execution

```cpp
// Execute multiple tasks in parallel
std::vector<folly::Future<Segment>> futures;
for (const auto& key : keys) {
    futures.push_back(submit_io_task([&, key]() {
        return storage.read(key);
    }));
}

// Wait for all
auto results = folly::collectAll(futures).get();
```

## Configuration

### Thread Counts

Thread pool sizes are typically configured at initialization or via environment variables. The scheduler creates thread pools during construction based on system configuration.

### Default Settings

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

## Usage Examples

### Parallel Segment Read

```cpp
#include <arcticdb/async/async_store.hpp>

AsyncStore async_store(storage);

// Read multiple segments in parallel
std::vector<folly::Future<std::pair<VariantKey, Segment>>> futures;
for (const auto& key : keys) {
    futures.push_back(async_store.read(key));
}

// Wait for all and process
auto results = folly::collectAll(futures).get();
for (auto& result : results) {
    auto [key, segment] = result.value();
    process(segment);
}
```

### Async Write Pipeline

```cpp
// Pipeline: encode -> compress -> write
auto write_future = submit_cpu_task([&]() {
    return encode(data);  // CPU-bound
}).thenValue([&](Segment encoded) {
    return submit_cpu_task([encoded = std::move(encoded)]() {
        return compress(std::move(encoded));  // CPU-bound
    });
}).thenValue([&](Segment compressed) {
    return submit_io_task([&, compressed = std::move(compressed)]() {
        storage.write(key, std::move(compressed));  // I/O-bound
    });
});

write_future.wait();
```

### Timeout Handling

```cpp
auto future = submit_io_task([&]() {
    return storage.read(key);
});

// With timeout
try {
    auto result = future.get(std::chrono::seconds(30));
} catch (const folly::FutureTimeout&) {
    // Handle timeout
}
```

## Error Handling

```cpp
auto future = submit_io_task([&]() {
    return storage.read(key);
}).thenTry([](folly::Try<Segment>&& result) {
    if (result.hasException()) {
        // Handle error
        throw StorageException("Read failed");
    }
    return std::move(result.value());
});
```

## Performance Considerations

### Thread Pool Sizing

- CPU pool: Match physical cores for compute tasks
- I/O pool: Over-provision to hide latency
- Monitor thread utilization to tune

### Task Granularity

- Too fine-grained: Scheduling overhead dominates
- Too coarse-grained: Poor parallelism

## Integration with Storage

```cpp
// In storage/store.hpp
class Store {
public:
    // Synchronous API
    Segment read(const VariantKey& key);

    // Used by AsyncStore internally
    void read_async(
        const VariantKey& key,
        std::function<void(Segment)> callback
    );
};
```

## Related Documentation

- [PIPELINE.md](PIPELINE.md) - Uses async for parallel I/O
- [STORAGE_BACKENDS.md](STORAGE_BACKENDS.md) - Storage being accessed
