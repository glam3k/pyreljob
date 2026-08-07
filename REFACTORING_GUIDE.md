A major refactoring to simplify pyreljob by removing cron and using only next_runtime.

This document explains the refactoring approach and key changes.

## Overview

This refactoring removes cron-based scheduling and uses only the `next_runtime` method for job scheduling. This simplifies the framework and makes scheduling more flexible.

## Key Changes

### 1. Job Class (task.py)
- Removed: `cron: ClassVar[str | None] = None`
- Simplified: `next_runtime()` method is the only scheduling mechanism
- Jobs now have full control over their scheduling logic

### 2. JobManager (manager.py)
- Removed: `schedule()` method (no longer needed)
- Removed: `_next_occurrence()` method
- Removed: `_is_misfired()` method
- Removed: `croniter` import
- Simplified: `run_forever()` just calls `tick()` periodically
- Maintains: `schedule()` method is no longer needed because `enqueue()` can set `next_run_at`

### 3. Backend Interface (backends/base.py)
- Removed: `cron` parameter from `schedule()` method
- Removed: `schedule()` method entirely
- Kept: `claim_scheduled()` and `set_next_run_at()` for maintaining jobs

### 4. Database Schema (orm.py, migrations/versions.py)
- Removed: `cron` column from `jobs` table
- Removed: All cron-related migration versions
- Kept: `next_run_at` column for scheduling

### 5. Examples (examples/)
- Updated: `hello.py` to use next_runtime scheduling
- Created: `fastapi_example.py` showing web framework integration
- Kept: Both examples demonstrate different use cases

## New Usage Patterns

### Pattern 1: One-Time Jobs
```python
@dataclass
class DataImportJob(Job):
    file_path: str
    tasks: ClassVar = [ImportTask]
    
    def next_runtime(self, last_run: RunRecord, ctx: TaskContext) -> datetime | None:
        return None  # Run once, then stop
```

### Pattern 2: Periodic Jobs
```python
@dataclass
class HeartbeatJob(Job):
    interval_minutes: int
    tasks: ClassVar = [HeartbeatTask]
    
    def next_runtime(self, last_run: RunRecord, ctx: TaskContext) -> datetime | None:
        if last_run is None:
            return datetime.now()  # Run immediately first time
        return datetime.now() + timedelta(minutes=self.interval_minutes)
```

### Pattern 3: Event-Driven Jobs
```python
class EventProcessorJob(Job):
    tasks: ClassVar = [ProcessEventTask]
    
    def next_runtime(self, last_run: RunRecord, ctx: TaskContext) -> datetime | None:
        # Schedule next run based on business logic
        if some_event_occurred:
            return datetime.now() + timedelta(seconds=5)
        return None  # Stop after processing
```

## Simplified JobManager Usage

```python
from pyreljob import JobManager
from examples.heartbeat import HeartbeatJob

manager = JobManager("sqlite:///jobs.db")
manager.migrate()

# Enqueue with immediate first run
job = manager.enqueue(HeartbeatJob(interval_minutes=5))

# Start the beat scheduler
manager.run_forever()
```

## Examples Directory Structure

```
examples/
├── hello.py                    # Standalone CLI example
├── fastapi_example.py          # Web framework example
└── common/                     # Shared utilities (if needed)
```

## Migration Guide

### From Cron to next_runtime

#### Old Pattern (Cron-based):
```python
@dataclass
class ScheduledJob(Job):
    cron: ClassVar[str | None] = "0 2 * * *"  # Daily at 2 AM
    tasks: ClassVar = [SomeTask]
    
    # Uses croniter for scheduling
```

#### New Pattern (next_runtime-based):
```python
@dataclass
class ScheduledJob(Job):
    tasks: ClassVar = [SomeTask]
    
    def next_runtime(self, last_run: RunRecord, ctx: TaskContext) -> datetime | None:
        if last_run is None:
            return datetime.now()
        return datetime.now() + timedelta(days=1)  # Daily
```

## Benefits

1. **Simplified API**: Only one scheduling mechanism to learn
2. **More Flexible**: Jobs can implement any scheduling logic
3. **Cleaner Code**: Less conditional logic and dependencies
4. **Better Testability**: Easier to test custom scheduling logic
5. **More Intuitive**: `next_runtime` is more explicit than cron

## Breaking Changes

This refactoring introduces breaking changes:

1. **Job class signature**: Removed `cron` parameter
2. **Database schema**: `cron` column removed (may require migration)
3. **API**: `JobManager.schedule()` method removed
4. **Migration**: Existing databases need to be migrated

## Migration Steps

### 1. Update Job Classes
```python
# Before
@dataclass
class MyJob(Job):
    cron: ClassVar[str] = "0 * * * *"
    tasks: ClassVar = [MyTask]

# After
@dataclass
class MyJob(Job):
    tasks: ClassVar = [MyTask]
    
    def next_runtime(self, last_run: RunRecord, ctx: TaskContext) -> datetime | None:
        if last_run is None:
            return datetime.now()
        return datetime.now() + timedelta(hours=1)
```

### 2. Update Database
Run migration script to remove cron column from jobs table.

### 3. Update JobManager Usage
```python
# Before
manager.schedule(MyJob, cron="0 * * * *")

# After
manager.enqueue(MyJob())  # next_runtime decides when to run
```

## Testing

All tests need to be updated:
1. Replace cron-based tests with next_runtime tests
2. Update fixture setup
3. Verify database migrations

This refactoring results in a much cleaner, more flexible framework while maintaining backward compatibility where possible.