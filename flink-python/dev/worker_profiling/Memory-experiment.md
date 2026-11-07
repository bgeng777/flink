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

# Python worker memory capture experiment

This implements the startup experiment in Appendix A of the local profiling FLIP.
It is experimental: the environment variables below are not public Flink configuration.
It uses Memray's public `Tracker` API in the standalone Beam SDK worker process.
UDFs need no changes. Embedded and loopback workers are not instrumented.

## Deployment

Install Memray in the worker's diagnostic Python environment, using the existing
Python dependency or environment deployment mechanism. Memray is optional and is
not added to PyFlink's dependencies. The experiment is tested with Memray 1.20.0.

Set these environment variables on the TaskManager before starting the selected
diagnostic deployment. The existing `python.systemenv.enabled` option must allow
inheritance for this approach. If inheritance is disabled, set them in a wrapper
used as `python.executable`; this does not require any change to the UDF.

| Variable | Default | Meaning |
| --- | --- | --- |
| `PYFLINK_MEMRAY_OUTPUT_DIR` | Unset: disabled | Local directory on the TM for capture files |
| `PYFLINK_MEMRAY_DELAY` | `0` | Non-negative delay in seconds from the worker startup hook |
| `PYFLINK_MEMRAY_DURATION` | `60` | Positive duration in seconds after Tracker starts |
| `PYFLINK_MEMRAY_NATIVE_TRACES` | `false` | `true` adds native C/C++ allocation stacks |

For example, start recording after 120 seconds and capture for 60 seconds:

```sh
export PYFLINK_MEMRAY_OUTPUT_DIR=/flink/log/memory-profiles
export PYFLINK_MEMRAY_DELAY=120
export PYFLINK_MEMRAY_DURATION=60
export PYFLINK_MEMRAY_NATIVE_TRACES=true
bin/start-cluster.sh
```

The directory must be writable by the worker. It can be the same local directory
as `rest.profiling.dir`, but this experiment does not read that option or register
its files in REST profiling history. Retrieve the files through the deployment's
existing log/file access mechanism. There is no new REST endpoint or Web UI.

Each worker start schedules one capture. Restarts schedule another capture with a
distinct filename. Applying the environment to an entire TM instruments every new
standalone Python worker there; use a small diagnostic deployment and manage disk
space and retention explicitly. The CPU profiler's enable flag and active-session
limit do not control this experiment.

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

Files start as `worker-<pid>-<unique-id>.bin.inprogress`. Only successful Tracker
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
python -m memray flamegraph -o allocations.html /flink/log/memory-profiles/worker-123-id.bin
python -m memray stats /flink/log/memory-profiles/worker-123-id.bin
python -m memray tree /flink/log/memory-profiles/worker-123-id.bin
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

See [Memray's API](https://bloomberg.github.io/memray/api.html) and
[native stack resolution](https://bloomberg.github.io/memray/native_mode.html).
