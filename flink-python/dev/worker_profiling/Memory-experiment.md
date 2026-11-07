<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Python worker memory profiling prototype

The local prototype implements both memory modes in the revised [FLIP](FLIP-draft.en.md):
startup capture using Memray's public Tracker API and separately enabled on-demand attach/detach.
It reuses Flink's profiler REST/RPC, history, local result directory and BLOB download. No new worker
control protocol, profiler SPI or in-cluster report renderer is introduced. This is not a released
or community-approved feature.

## Deployment

Install Memray in the worker's Python environment. Attach also requires a compatible Memray CLI
and debugger on the TaskManager, with permission to inspect the worker. Memray is optional and
is not added to PyFlink's dependencies. The tested combination is Memray 1.20.0, CPython 3.11.18,
Beam 2.75.0 and Linux AArch64.

Configure the existing profiler and the shared local output directory on the TaskManager:

```yaml
rest.profiling.enabled: true
rest.profiling.dir: /flink/log/profiling
```

For a delayed startup capture, set these job options:

```yaml
python.memory-profiling.startup.enabled: true
python.memory-profiling.startup.delay: 120 s
python.memory-profiling.startup.duration: 60 s
python.memory-profiling.startup.native: true
```

Defaults are disabled, zero delay, 60 seconds and no native stacks. The duration must not exceed
`rest.profiling.duration-max`. The worker must be able to write to the configured directory.
The options travel through the existing managed Python environment, including with general
system-environment inheritance disabled. Legacy `PYFLINK_MEMRAY_*` variables are not configuration
aliases; the TM overrides inherited values with the resolved configuration.

Each worker start schedules one capture, and restarts use distinct filenames. Startup files share
`rest.profiling.dir` and the filename-based TaskManager download route, but not REST history or
its retention policy. The deployment owns cleanup and capacity planning for these files.

For on-demand attach, additionally configure:

```yaml
rest.python-profiling.memory.executable: /opt/diagnostics/bin/memray
rest.python-profiling.memory.attach-enabled: true
```

**Attach/detach can deadlock or crash a worker and cause job recovery.** It is disabled by default;
the UI requires explicit risk acknowledgment. Discover a prepared worker through
`GET /taskmanagers/:tm/python-workers`, then use the existing endpoint:

```text
POST /taskmanagers/:tm/profiler
{"workerId":"<returned id>","type":"MEMORY","duration":30,"includeNative":false}
```

`includeIdle` is not accepted. A startup window pending or recording in that worker returns 409;
disabled or unavailable attach returns 503. PyFlink cProfile prevents memory profiling in that worker.
The deployment must not run another independent Memray controller concurrently.

The TM runs attach, waits, and explicitly detaches on the profiling executor. CLI completion alone
is not capture completion. A target-side timer is a fallback, not proof of successful closure.
Only confirmed closure publishes a `.bin`. Unconfirmed stop leaves a failed session, an incomplete
artifact, and the JVM-wide REST slot reserved until the original worker exits. CLI/debugger cleanup
is bounded; Flink does not terminate the business worker to recover the profiler.

## Lifecycle and artifacts

The existing `beam_boot` entry point wraps the harness with a memory capture context.
A single background thread owns the Tracker from entry to exit. It waits for the
delay, records allocations for the duration, and closes the Tracker. The same worker
continues processing afterward. With zero delay, the harness waits for Tracker
startup before importing user code.

When the harness returns or raises a Python exception, the context wakes the capture
thread and joins it. An unstarted capture is cancelled; an active capture closes
once. UDF exceptions retain their original behavior. No profiler process is attached,
and no worker is killed or restarted to end capture.

Startup files begin as `python-memory-startup-<pid>-<unique-id>.bin.inprogress`;
attach files begin as `python-memory-<unique-id>.bin.inprogress`. Only successful Tracker
closure and file finalization publish a `.bin` file. INFO logs include effective
options, actual start/end timestamps, PID, filename, and completion or cancellation.
Failed captures log a warning and retain any incomplete file for diagnosis.

The disabled path does not import Memray or start a thread. Invalid options, missing
dependencies, and directory preparation failures log a warning and allow normal
worker startup. Python exceptions from Tracker initialization or closure are also
reported without swallowing harness exceptions. This does not isolate native crashes
or stalls: Memray runs inside the worker. Timers and normal shutdown depend on Python
execution opportunities; they are not hard deadlines. SIGKILL, OOM, and termination
that bypasses Python cleanup may leave an incomplete file.

## Reading a capture

Keep the original `.bin` file. Generate reports offline, preferably in the same
environment as the worker so that native libraries and debug symbols match:

```sh
python -m memray flamegraph -o allocations.html /flink/log/profiling/python-memory-startup-123-id.bin
python -m memray stats /flink/log/profiling/python-memory-startup-123-id.bin
python -m memray tree /flink/log/profiling/python-memory-startup-123-id.bin
```

Captures use Memray's default allocation format, with compression on closure disabled
to avoid adding that work to worker shutdown. Reports can show peak allocations and
allocations still live at capture end. Those remaining allocations are not by
themselves evidence of a leak. Delayed capture cannot attribute allocations made
before tracking began. Native allocations can be recorded even when native stack
collection is disabled. Correlate these files with RSS/PSS and the incident window;
they do not explain all container or GPU memory.

## Validation

The unit tests cover cancellation before delay, delayed capture, timed completion,
normal/exceptional harness exit, output finalization, distinct files, native options,
and diagnostic failures. They use explicit synchronization rather than timing sleeps.

For a standalone test, run the continuous [smoke job](memory_smoke.py) in process mode:

```sh
bin/flink run -py flink-python/dev/worker_profiling/memory_smoke.py
```

It allocates image-sized buffers, retains a bounded window, and compresses buffers
with zlib. Confirm that reports contain `allocate_image_buffer` and
`compress_image_buffer`, and that native mode contains zlib allocation frames.
After `.bin` appears, verify that the same PID keeps producing increasing sequence
numbers, then cancel the smoke job. This is a functional check, not a Torch/YOLO
benchmark or a production overhead guarantee.

The [formal-memory validation report](../../../output/playwright/pyflink-20261107/formal-memory/run.md)
records the latest real attach and startup results and remaining gaps.

See [Memray's API](https://bloomberg.github.io/memray/api.html) and
[native stack resolution](https://bloomberg.github.io/memray/native_mode.html).
