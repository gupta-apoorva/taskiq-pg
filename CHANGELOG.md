# Changelog

## [Unreleased]

### Features

* Add atomic message claiming via `FOR UPDATE SKIP LOCKED` to prevent duplicate processing
* Add message states (queued, active, completed) for better tracking
* Add group-based job coordination to prevent concurrent execution of related tasks
* Add opt-in FIFO within a group via the `ordered` label: an ordered message waits for every older unfinished message in its group, so delayed or retrying messages are not overtaken
* Add `OrderedRetryMiddleware`: retries requeue the existing row instead of kicking a new message, so a retry keeps its place in an `ordered` group
* Honour a per-message `max_retries` label in the sweeper, so it and the retry middleware share one attempt budget
* Add message TTL support for automatic cleanup of completed messages
* Add automatic sweeping of stuck messages back to queue
* Add connection health checks and automatic reconnection
* Add dedicated dequeue connection for improved performance
* Implement `FOR UPDATE SKIP LOCKED` for efficient concurrent dequeuing
* Add configurable sweep interval and stuck message timeout

### Breaking Changes

* Require `taskiq>=0.11.20` for `SmartRetryMiddleware`, which `OrderedRetryMiddleware` extends
* Stamp `_tpg_row_id` and `_tpg_attempts` labels onto every delivered message
* Count attempts at claim time: `retry_count` is bumped when a message is claimed rather than when the sweeper reclaims it, so worker crashes and task failures share one budget and `max_retry_attempts` bounds attempts rather than retries

### Bug Fixes

* Fix the group mutex admitting two messages of a group at once: the mutex is re-checked under the advisory lock, in a statement of its own

### Performance Improvements

* Use optimized indexes for efficient dequeuing operations
* Implement connection pooling best practices
* Add batch operations for cleanup tasks

## [0.2.0](https://github.com/karoo-ca/taskiq-pg/compare/v0.1.7...v0.2.0) (2025-03-06)


### Features

* add dependency injection for dsn ([#23](https://github.com/karoo-ca/taskiq-pg/issues/23)) ([72170fe](https://github.com/karoo-ca/taskiq-pg/commit/72170fe15f26acb7640e89fd0fa498439cf41d08))

## [0.1.7](https://github.com/karoo-ca/taskiq-pg/compare/v0.1.6...v0.1.7) (2025-03-06)


### Documentation

* readme correction ([1c015a0](https://github.com/karoo-ca/taskiq-pg/commit/1c015a0f25b236a4c8699cd4ad7e01bd6383f3ab))

## [0.1.6](https://github.com/karoo-ca/taskiq-pg/compare/v0.1.5...v0.1.6) (2025-03-04)


### Bug Fixes

* drop python 3.8 support (EOL) ([#19](https://github.com/karoo-ca/taskiq-pg/issues/19)) ([10572d6](https://github.com/karoo-ca/taskiq-pg/commit/10572d6ab1abe5432954bd9369a37a62fea2ce5d))

## [0.1.5](https://github.com/karoo-ca/taskiq-pg/compare/v0.1.4...v0.1.5) (2025-03-04)


### Bug Fixes

* clean up type annotations ([#16](https://github.com/karoo-ca/taskiq-pg/issues/16)) ([8d3f5d0](https://github.com/karoo-ca/taskiq-pg/commit/8d3f5d0fd0d7ae338b30bb42a6fbf22f0b5bcb18))

## [0.1.4](https://github.com/karoo-ca/taskiq-pg/compare/v0.1.3...v0.1.4) (2025-03-03)


### Bug Fixes

* use write connection to delete instead of read connection ([#14](https://github.com/karoo-ca/taskiq-pg/issues/14)) ([85eb9fe](https://github.com/karoo-ca/taskiq-pg/commit/85eb9fe82d69adf882fa3524097aa0383bfa20fd))

## [0.1.3](https://github.com/karoo-ca/taskiq-pg/compare/v0.1.2...v0.1.3) (2025-01-17)


### Bug Fixes

* add py.typed marker ([#12](https://github.com/karoo-ca/taskiq-pg/issues/12)) ([a217d92](https://github.com/karoo-ca/taskiq-pg/commit/a217d92263c5003835348ce9fe33ed5d611451da))

## [0.1.2](https://github.com/karoo-ca/taskiq-pg/compare/v0.1.1...v0.1.2) (2025-01-09)


### Bug Fixes

* docs and example ([9a18a4a](https://github.com/karoo-ca/taskiq-pg/commit/9a18a4a6b7821cb403cf6294f020363fb9e0008f))

## [0.1.1](https://github.com/karoo-ca/taskiq-pg/compare/v0.1.0...v0.1.1) (2025-01-07)


### Bug Fixes

* docs ([b8054f8](https://github.com/karoo-ca/taskiq-pg/commit/b8054f8f99fd91dae77c07c55eefe520a9471b63))

## 0.1.0 (2025-01-07)


### Miscellaneous Chores

* initial release
