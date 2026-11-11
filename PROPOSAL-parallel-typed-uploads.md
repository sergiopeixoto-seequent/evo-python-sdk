# Parallel typed uploads: findings and proposed plan

## Recommendation

**Rework the branch in Daniel's direction, rather than discard its useful changes or merge its current concurrency design unchanged. Deliver the work in two phases.**

Keep off-event-loop preparation, concurrent independent operations, ordered results, and cancellation/failure cleanup. Remove the new global upload-scope machinery. Use the default thread pool initially; introduce a dedicated preparation executor only if a lower worker count or isolation is demonstrably needed.

There is one important qualification: **a thread pool plus an HTTP connection limit does not bound all upload resources.** This SDK reads chunks into memory before acquiring HTTP connections. Removing the file-level limit while retaining unrestricted nested gathers therefore needs a resource check, not just a throughput comparison.

| Question | Answer |
|---|---|
| Are the new semaphores necessary for the performance improvement? | No. Offloading blocking preparation and scheduling independent work concurrently produce the opportunity for improvement. A semaphore restricts that work. |
| Was adding a limit unreasonable? | No. It protected against nested fan-out and concurrent serialization/transfers. However, its scope expanded into a cross-client, per-loop policy without an upload-specific performance/resource justification in the material reviewed. |
| Can a thread pool replace the save semaphore? | Yes, for the number of actively executing save jobs. It does not limit queued jobs, retained inputs, or file uploads. |
| Does `AioTransport` make the upload semaphore redundant? | For limiting simultaneous HTTP connections, largely yes when using one transport with a finite limit. For limiting open files, buffered chunks, and pending tasks, no. |
| Should the branch be replaced by Daniel's proposal? | Use a mix: his simpler resource ownership, plus the branch's necessary ordering, cleanup, and mutation safeguards. Do not substitute bare `gather` everywhere. |
| Is a two-phase delivery feasible? | Yes. Phase 1 improves the existing async execution; Phase 2 separates typed preparation from transfer and centralizes their scheduling. Some small typed call-site edits can belong in Phase 1. |

## Scope and evidence

This assessment compares local HEAD `5409813` with base `152dda6` on `sergiopeixoto/parallel-typed-uploads`. It concerns a proposed implementation, not a runtime-code change.

The tagged `REVIEW-parallel-typed-uploads.md` describes an earlier revision ending at `b507b11`. Its approval, issue list, line numbers, and test results must not be treated as an assessment of the current HEAD.

In particular, HEAD already has private `_upload`/`_apply` methods instead of public `prepare`/`commit`, one `gather_cancelling` helper, worker-based attribute inference, less-copying column selection, repeated-cancellation draining, monotonic retry feedback, per-client in-flight deduplication, and a common-package version/dependency bump. The recommendation below accounts for those changes rather than proposing their fixes again. [S1-S4, S10]

The source establishes where work is scheduled and resources are acquired. It does **not** establish an optimal worker count or a measured speedup for either proposed phase. No representative upload benchmark or peak-memory comparison was supplied with the reviewer comment or historical review.

## 1. Why the semaphore was added, and what it actually does

### History and likely motivation

The initial `13070b9` implementation introduced concurrent typed traversal alongside a shared limit of eight. `gather_uploads` installed a context-local semaphore for nested operations; `ObjectDataClient` also had a fallback semaphore. The slot enclosed saving and uploading.

Subsequent commits added public configuration and made the scope work across repeated event loops and threads. At HEAD:

- `upload_concurrency()` installs an `_UploadScope` in a `ContextVar`.
- The scope manages weak references to per-loop semaphores under a threading lock.
- Outside an explicit scope, clients on the same loop share a default limit of four.
- Preparation and each upload attempt acquire the **same** semaphore, separately.
- Parent gathers do not acquire slots while awaiting their children. [S1]

The reasonable inferred motivation was to prevent nested groups from multiplying active work, including clients created internally by typed code. That matters because `get_data_client()` constructs a new client: a per-client limit alone would not constrain an entire typed operation. [S8]

However, the commit messages do not demonstrate that eight, then four, was required for uploads. Matching the download default is not evidence: downloads use a per-call limit, whereas this upload policy coordinates unrelated clients on a loop.

### Strengths and weaknesses of the current limit

The limit does provide useful admission control for active preparation jobs and file-transfer attempts. But it combines different resources:

| Resource | Relevant control | Limitation of the current design |
|---|---|---|
| Active conversion/compression/hash jobs | Executor worker count | The extra semaphore reduces the executor's available concurrency and couples preparation to transfer occupancy. |
| Simultaneous HTTP connections | `AioTransport` connector limit | A file slot is not an HTTP connection slot; each file can have several chunk requests. |
| Active files and their chunk buffers | File admission or a shared chunk/byte budget | The current semaphore helps here, but its purpose is obscured by sharing it with preparation. |
| Pending tasks and executor submissions | Lazy production or bounded work queues | Eager gathers still construct all their tasks. A semaphore does not eliminate this backlog. |
| Cached files waiting to upload | Backpressure between preparation and transfer | A preparation slot is released before an upload slot is acquired. There is no bound on the entire save-to-upload lifecycle. |

With a limit of four, four slow transfers can exclude new preparation, even when CPU and disk capacity are available. Conversely, many queued preparation jobs can delay upload admission. This coupling can reduce overlap.

The scope is also not a process-wide budget: separate loops and explicitly created scopes have separate limits. Replacing it with one shared executor would change that scope, not simply reproduce it more cheaply.

**Conclusion:** the protective intent makes sense; a global, combined CPU/network semaphore is not the simplest expression of that intent.

## 2. Can saving just use the thread pool?

**Yes. `asyncio.to_thread()` already uses the loop's default executor.** In the inspected Python 3.12 environment, it delegates to `run_in_executor(None, ...)` and propagates `contextvars`.

The usual Python 3.10-3.12 worker default is `min(32, (os.cpu_count() or 1) + 4)`. Python 3.13+ uses `os.process_cpu_count()` instead. Applications can also replace the default executor, so the SDK should not promise a fixed number of workers. [P1-P2]

The offloaded boundary should include the expensive synchronous preparation, not just the final file write:

- DataFrame-to-Arrow schema inference and conversion.
- Datetime normalization and category dictionary construction where applicable.
- Parquet compression, hashing, and cache-file finalization.

HEAD already moves much of this work into `run_upload_preparation`; retain that useful boundary and separate its worker-cleanup responsibility from semaphore acquisition. Saving only the already-created Arrow table would leave DataFrame conversion blocking the loop. [S2-S4]

Threads improve event-loop responsiveness and allow overlap. They do not guarantee linear CPU scaling: Python/GIL behavior, native-library parallelism, compression, memory bandwidth, and disk throughput all matter.

### Default versus dedicated executor

| Option | Assessment |
|---|---|
| Default executor via `to_thread` | Recommended starting point. No SDK executor lifecycle or new configuration API. |
| Dedicated `ThreadPoolExecutor(max_workers=N)` | Appropriate if measurements show excessive preparation concurrency or interference with other default-pool work, including DNS resolution. |
| Keep the current preparation semaphore | Only justified by an explicit scoped-admission requirement that the worker pool does not satisfy; not necessary merely to move saving off the loop. |

A dedicated executor is thread-safe and not bound to one event loop. Its asyncio wrappers and the HTTP transport are still loop-bound. Use it for synchronous preparation, not for running the async upload stack in worker threads.

If a dedicated executor becomes necessary:

- Give it an explicit owner and lifetime, shared across that operation's internally created clients. Do not create an executor per column or per ephemeral `ObjectDataClient`.
- Do not change the application's default executor from SDK code.
- Preserve context propagation when using `run_in_executor` directly.
- Drain active writes before ending the operation. Cancellation cannot kill an already-running thread.
- Avoid blocking the event loop with executor shutdown or `Future.result()`. A `with ThreadPoolExecutor(...)` block can block on exit.

Replacing the scope registry with a module-global executor would remove loop-management complexity, but would still introduce global state and a shutdown policy. That is not needed in the first phase.

## 3. The transport limit does not cover file-buffer memory

Daniel is correct that `AioTransport(num_pools=...)` already configures `aiohttp.TCPConnector(limit=num_pools)`. The default is four simultaneous connections across hosts for that transport; zero disables the limit. Different transports have independent limits, and custom `ITransport` implementations need not provide this behavior. [S5]

The SDK upload path adds another layer:

1. `StorageDestination.upload_file()` creates a `ChunkedIOManager` and opens the cached file.
2. Each manager allows up to `min(32, os.cpu_count() + 4)` chunk transfers by default.
3. Each admitted chunk is read into a `bytes` object **before** `destination.write_chunk()` awaits its HTTP request.
4. The connection limiter is reached downstream, after that allocation. [S6]

Consequently, requests waiting for one of four connections can already hold many chunks across many files.

For sufficiently large files, a useful capacity estimate for this one category of live data is:

```text
chunk payloads retained ~= active files x chunk workers per file x chunk size
```

The default chunk size is 4 MiB. At 32 chunk workers, one large file can retain roughly 128 MiB of chunk payloads; ten such files can approach 1.25 GiB even with only four HTTP connections. These are illustrative capacity estimates, **not measured RSS**, and exclude Arrow/Pandas data, task overhead, and other allocations.

The current upload semaphore reduces that risk by limiting active files. A preparation executor cannot replace this protection. Even a one-worker save pool can eventually hand many completed files to a slow network.

There are two further caveats:

- The chunk manager creates a task for every unfinished chunk, even though it bounds active chunk transfers.
- `BytesSource.read_chunk()` currently performs synchronous reads inside an async method. Moving Parquet saving off the loop does not make every local I/O path nonblocking. [S6]

**Recommendation:** do not defend the semaphore as an extra network-connection cap. Explain its genuine file-resource role. Remove the global mechanism, but do not enable arbitrary recursive fan-out on the assumption that the transport makes it resource-safe.

A future operation-local set of upload workers can express file admission without another global semaphore. If a hard memory guarantee is required, file counts alone are insufficient; the design also needs a chunk/byte budget.

## 4. Which parts of this branch should survive?

| Branch change | Proposed treatment |
|---|---|
| Off-loop DataFrame/Arrow/Parquet preparation | Keep. This is a core improvement. |
| One ordered, cancel-and-drain gather helper | Keep. It is independent of upload limiting. |
| Fixed-size independent upload groups, such as category values and lookup | Keep in Phase 1. |
| Eager parallelism over arbitrary attributes, collections, and schema lists | Move to the Phase 2 scheduler unless Phase 1 resource measurements justify retaining it. |
| Global upload `ContextVar`, `_UploadScope`, weak-reference registry, lock, and public `upload_concurrency` API | Remove from the proposed design. If already released outside this branch, removal needs a compatibility/deprecation decision. |
| Worker draining on cancellation | Keep without coupling it to a semaphore. A cancelled await does not stop a running cache write. |
| Private staging/apply hooks for parallel mutations | Keep where needed to safely parallelize an existing mutation. Do not turn them into a new public API. |
| Per-client, per-loop in-flight deduplication | Reassess separately. Phase 2 can deduplicate prepared content within one operation without a long-lived task registry. |
| Outer retry policy and common-package logging changes | Separate from the performance-only work. Neither is necessary to demonstrate offloading plus concurrency; preserve the base retry behavior if those additions are omitted. |
| Existing download and chunk-manager semaphores | Leave unchanged in Phase 1. Daniel's concern is not a reason to delete unrelated resource controls. |

Bare `asyncio.gather()` propagates a child's first exception without automatically cancelling its siblings. Dropping `gather_cancelling` could leave other uploads running after the caller has received a failure. The helper is useful even with no upload semaphore. [S1, P1]

The packages support Python 3.10, so replacing it with `TaskGroup` is not a drop-in change; it also changes exception handling unless adapted. [S10]

Ordering and mutation safety are distinct from resource limiting. Concurrent completion must not reorder columns, collections, or schema assignments.

Also avoid overstating current atomicity: `DataTableAndAttributes.from_dataframe()` applies the table before `Attributes._apply()` checks for concurrent attribute edits. That later check can raise after table/bounding-box state changed. Any retained parallel mutation path must perform all fallible conflict/derived-metadata checks **before** changing live state. Successfully uploaded remote blobs are not rolled back by local mutation staging. [S3-S4]

## 5. Phase 1: improve the existing async execution

**Goal:** deliver a small performance change without introducing a typed upload planner or a new global concurrency policy.

### Proposed scope

1. Keep synchronous save APIs synchronous. Their async upload wrappers offload synchronous preparation through a small cancellation-safe worker helper using `to_thread`, with no upload-scope lookup.
2. Keep `gather_cancelling` for independent operations. Initially favor fixed-size groups, such as category values/lookup and carefully staged table/attribute work.
3. Keep arbitrary-length attribute/collection traversal serial where necessary for a small first delivery. Defer the branch's broad recursive gathers rather than silently replacing a limit of four with effectively unlimited active files.
4. If broad gathers are essential to the first delivery, make the resource comparison below a merge condition. If it fails, either narrow the first delivery or bring forward operation-local bounded scheduling. A per-client cap is not an equivalent substitute.
5. Preserve feedback, schema ordering, error context, completed cache-file semantics, and safe mutation behavior for the paths changed. Do not remove cleanup code just because its original comment mentioned a semaphore.
6. Remove upload-scope exports, configuration documentation, and tests that exist only to enforce the rejected global policy. Retain/adapt the tests of concurrency, cleanup, and output behavior.
7. Keep public dispatch/override behavior compatible where practical. HEAD changes DataFrame upload dispatch and the typed table extension hook; making hooks private alone does not preserve subclasses overriding the old entry points. Resolve and document those compatibility decisions explicitly.
8. Keep retry/logging changes out of this performance-only scope unless independently required. Update version/dependency changes consistently with what actually remains.

**What Phase 1 is not:** a guarantee of the same speedup as every nested gather currently on the branch. A conservative split can deliver responsiveness and selected overlap first, then deliver broad typed throughput in Phase 2.

"Async-only" also cannot literally mean "no typed-file edits" if typed operations are to run concurrently: the sequential awaits live in typed code. Small scheduling and staging edits are different from redesigning typed serialization.

### Resource and behavior gates

Compare the base, current branch, and simplified candidate on the same workload, hardware, transport limit, and cache/server state.

| Check | Required observation |
|---|---|
| Blocking preparation | An event-loop heartbeat continues while conversion/compression is held in a worker. |
| Actual overlap | Independent operations start before the first finishes; do not rely solely on elapsed-time assertions. |
| Large/wide inputs and slow transfer | Record peak process RSS, open files, active file transfers, queued tasks, and cache growth. Include realistic numeric, datetime, category, and mixed data. |
| Worker choices | Compare the default executor with smaller dedicated-pool candidates only if the default shows pressure or contention. |
| Output compatibility | Table schemas/data references, category pairs, attribute order/keys/metadata, collection units, and bounding boxes retain their intended behavior. |
| Failures and cancellation | Cancel and drain siblings; wait for active cache writes; preserve the primary exception and live state. Include repeated cancellation. |
| Duplicate content and cache use | Concurrent saves/uploads of identical data remain safe, including Windows cache-file replacement and files already open for upload. |

Set concrete memory/latency budgets from the target deployment before deciding that unrestricted fan-out is acceptable. Thread count, connection count, and passing functional checks are not substitutes for a process-memory measurement.

The existing package test entry points are documented in `Makefile`; target the changed data-client and typed paths first, then run the object-package suite. No new benchmark framework is needed merely to collect a controlled comparison.

## 6. Phase 2: separate typed preparation from uploading

**Feasible, and a good follow-up.** Here "two phases" means two implementation deliveries; within the second delivery, the runtime can have two overlapping stages.

### What is already available

The low-level pieces mostly exist:

- `save_table`/`save_dataframe` write completed cached files and return references.
- `_save_category_table` prepares a values/lookup pair.
- `_upload_cached_table` transfers a prepared reference.
- `upload_referenced_data` walks document references and obtains upload URLs in batches, but its current transfer loop is sequential. It is reusable prior art, not an existing streaming uploader. [S2, S7]

The main refactoring is in typed construction: `_data_to_schema` currently mixes validation, conversion, uploading, and schema assembly. `SchemaBuilder` gathers those mixed operations. `Attributes._upload` also means "perform uploads and return staged documents," not "save files only." [S3-S4]

### Proposed pipeline

```text
typed input + metadata validation
                 |
                 v
lazy preparation jobs
                 |
                 v
synchronous preparation in worker threads
  normalize -> Arrow -> Parquet -> hash -> finalize cache file
                 |
                 v
bounded queue of completed-file descriptors
                 |
                 v
operation-local async upload workers
  cached file -> existing HTTP/storage layer
                 |
                 v
ordered schema assembly / pre-commit checks / final application
```

Use small internal descriptors carrying stable ordering/association information, cached data references, and the schema information needed by the caller. Category results must associate both files with one logical attribute. Do not retain full converted frames or Arrow tables in completed descriptors.

Prefer fixed worker tasks and bounded queues to a new general-purpose scheduler. The owner is one high-level operation, not a module-global registry. Its internal clients share that operation's work and deduplication decisions explicitly.

### Important design rules

- **Start transfer as files finish.** A "save everything, then upload everything" barrier is a possible first prototype, but loses save/upload overlap and can increase cache backlog.
- **Bound submission as well as execution.** Submitting every save to an executor, or passing all futures to `as_completed`, still creates an unbounded pending set. Pull work lazily and stop producing when the ready queue is full.
- **Keep the thread boundary narrow.** Workers return completed descriptors. The event loop owns asyncio queues, transport calls, progress callbacks, and live model mutation. Do not have workers directly mutate asyncio futures or queues.
- **Bound active files independently of HTTP connections.** Upload workers address the chunk-buffer issue from section 3. A queue bound alone does not help if the consumer immediately starts unlimited upload tasks.
- **Preserve early validation deliberately.** Validate cheap metadata before remote work; cache expensive inferred types rather than infer twice. Define the validation boundary for the whole operation, not just each attribute group. Existing-value conversion and category-format errors can still arise during preparation.
- **Commit only when all prerequisites succeed.** Collect results by stable input position, precompute derived metadata, and check conflicts before applying any live changes. Do not mutate user models in worker completion callbacks.
- **Handle failure at the operation boundary.** Stop production, cancel/drain upload workers and queued work, drain running synchronous writes, and propagate the original error. Never publish a successful object referencing unfinished uploads.
- **Deduplicate locally after hashing.** One operation can upload identical content once and reuse its result for several references. Do not merge work across unrelated callers or incompatible retry policies without defining ownership and cancellation semantics.
- **Preserve cache ownership.** Backpressure bounds prepared-but-unconsumed work, not total retained cache size. Completed cached files remain subject to the existing cache policy. Failure is not permission to delete shared cached data or remotely uploaded blobs.
- **Do not mutate input DataFrames during preparation.** A thread-safe executor does not make the supplied frames, custom caches, or model objects thread-safe.

### Would gathering stop being important?

**Nested gathers throughout typed code would become less important, but concurrent scheduling and joining would remain necessary.**

Each current `save -> upload` coroutine can already start uploading as soon as its own save finishes; `gather` does not impose a save-all barrier. The present shared semaphore can interfere with that overlap, but the pipeline is not introducing overlap from nothing.

The stronger reasons for Phase 2 are centralized ownership, bounded pending work, fewer recursive upload tasks, better separation of preparation/transfer capacity, and operation-wide deduplication. It may enable small upload-URL batches using existing functionality, provided batching does not force all preparation to finish first.

The coordinator still needs to await its producer/consumer workers and propagate failures. That can use the same structured cleanup helper. Removing every `gather` is not a performance objective.

### Suggested implementation order

1. Separate typed schema preparation from transfer internally while preserving current public async entry points and return shapes. Prove the boundary on a point set with attributes, including a category pair.
2. Add lazy production, a bounded completed-file queue, and upload workers owned by that operation. Transfer fast preparation results without waiting for slower siblings.
3. Extend the same path to nested schema lists and downhole collections. Preserve units, stable ordering, validation, and mutation hooks.
4. Measure against Phase 1. Only then consider dedicated preparation-pool tuning, upload-URL batching, or deeper chunk-I/O changes.

Phase 2's acceptance criteria should add bounded outstanding-job counts, out-of-order preparation, a producer failing while consumers are active, cancellation with a full queue, duplicate references, and unchanged model state after any pre-commit failure.

## Final decision

**The branch's performance direction makes sense; its global semaphore architecture is not essential to that direction.** Follow Daniel's simplification for preparation and connection ownership, retain the necessary cleanup and staging, and treat file-resource admission as a separate concern.

**Two deliveries are practical.** Ship a narrowly scoped offloading/concurrency improvement first. Follow with an operation-local typed preparation/upload pipeline for broad concurrency, rather than carrying global loop/semaphore management forward merely to support eager nested gathers.

## Source references

Paths and line ranges below refer to HEAD `5409813`.

| ID | Evidence |
|---|---|
| S1 | `packages\evo-objects\src\evo\objects\utils\_concurrency.py:26-122` - upload scope, shared stage semaphore, worker draining, ordered gather cleanup. |
| S2 | `packages\evo-objects\src\evo\objects\utils\data.py:217-388,512-597` - upload wrappers, in-flight tasks, retry owner, category preparation, DataFrame worker boundary. |
| S3 | `packages\evo-objects\src\evo\objects\typed\attributes.py:258-297,344-401,449-520` - per-column preparation, validation, upload fan-out, staged application/conflict checks. |
| S4 | `packages\evo-objects\src\evo\objects\typed\_data.py:47-76,120-171`; `typed\_model.py:188-228,508-513`; `typed\pointset.py:81-84`; `typed\downhole_collection.py:350-364,476-492` under the same package source directory - typed scheduling and mutation boundaries. |
| S5 | `packages\evo-sdk-common\src\evo\aio\transport.py:45-76`; `packages\evo-sdk-common\src\evo\aio\_helpers.py:59-75` - transport default and connector construction. |
| S6 | `packages\evo-sdk-common\src\evo\common\io\chunked_io_manager.py:34-35,115-179`; `storage.py:117-136,182-235`; `bytes.py:70-84` under the same IO directory - chunk size, per-file concurrency, eager chunk tasks, and reads before HTTP transfer. |
| S7 | `packages\evo-objects\src\evo\objects\utils\data.py:161-196`; `packages\evo-objects\src\evo\objects\io.py:109-143`; `packages\evo-objects\src\evo\objects\utils\tables.py:234-271` - referenced-data traversal, batched URL preparation, completed hashed cache files. |
| S8 | `packages\evo-objects\src\evo\objects\typed\_utils.py:89-94` - typed helpers create fresh data clients. |
| S9 | `packages\evo-objects\tests\test_upload_concurrency.py:217-291`; `tests\test_data_client.py:979-1039`; `tests\typed\test_attributes.py:195-308` under `packages\evo-objects` - existing cleanup, deduplication, progress, and mutation coverage to preserve/adapt. |
| S10 | `packages\evo-objects\pyproject.toml:5,12-15`; `packages\evo-sdk-common\pyproject.toml:4-5`; `Makefile:1-5,19-20` - Python support, current dependency/version changes, documented validation entry points. |

- P1: Python documentation, [asyncio tasks, gathering, and running in threads](https://docs.python.org/3.12/library/asyncio-task.html).
- P2: Python documentation, [ThreadPoolExecutor defaults, pending work, cancellation, and shutdown](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor).
