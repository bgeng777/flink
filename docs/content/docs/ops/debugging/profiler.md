---
title: "Profiler"
weight: 3
type: docs
aliases:
  - /ops/debugging/profiler.html
  - /ops/debugging/profiler
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Profiler

Since Flink 1.19, we support profiling the JobManager/TaskManager process interactively with [async-profiler](https://github.com/async-profiler/async-profiler) via Flink Web UI, which allows users to create a profiling instance with arbitrary intervals and event modes, e.g ITIMER, CPU, Lock, Wall-Clock and Allocation.

- **CPU**: In this mode the profiler collects stack trace samples that include Java methods, native calls, JVM code and kernel functions.
- **ALLOCATION**: In allocation profiling mode, the top frame of every call trace is the class of the allocated object, and the counter is the heap pressure (the total size of allocated TLABs or objects outside TLAB).
- **Wall-clock**: Wall-Clock option tells async-profiler to sample all threads equally every given period of time regardless of thread status: Running, Sleeping or Blocked. For instance, this can be helpful when profiling application start-up time.
- **Lock**: In lock profiling mode the top frame is the class of lock/monitor, and the counter is number of nanoseconds it took to enter this lock/monitor.
- **ITIMER**: You can fall back to itimer profiling mode. It is similar to cpu mode, but does not require perf_events support. As a drawback, there will be no kernel stack traces.

{{< hint warning >}}

Any measurement process in and of itself inevitably affects the subject of measurement. In order to prevent unintended impacts on production environments, Profiler are currently available as an opt-in feature. To enable it, you'll need to set [`rest.profiling.enabled: true`]({{< ref "docs/deployment/config">}}#rest-profiling-enabled) in [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}). We recommend enabling it in development and pre-production environments, but you should treat it as an experimental feature in production.

{{< /hint >}}


## Requirements
Since the Profiler is powered by the Async-profiler, it is required to work on platforms that are supported by the Async-profiler.

|           | Officially maintained builds | Other available ports                     |
|-----------|------------------------------|-------------------------------------------|
| **Linux** | x64, arm64                   | x86, arm32, ppc64le, riscv64, loongarch64 |
| **macOS** | x64, arm64                   |                                           |

Profiling on platforms beyond those listed above will fail with an error message in the `Message` column.


##  Usage
Flink users can complete the profiling submission and result export via Flink Web UI conveniently.

For example,
- First, you should find out the candidate TaskManager/JobManager with performance bottleneck for profiling, and switch to the corresponding TaskManager/JobManager page (profiler tab).
- You can submit a profiling instance with a specified duration and mode by simply clicking on the button **Create Profiling Instance**. (The description of the profiling mode will be shown when hovering over the corresponding mode.)
- Once the profiling instance is complete, you can easily download the interactive HTML file by clicking on the link.

{{< img src="/fig/profiler_instance.png" class="img-fluid" width="90%" >}}
{{% center %}}
Profiling Instance
{{% /center %}}


## Standalone Python workers

Python workers in PyFlink process mode can be sampled using an administrator-installed
[py-spy](https://github.com/benfred/py-spy). Enable the existing profiler and configure the
absolute path to the executable on each TaskManager:

```yaml
rest.profiling.enabled: true
rest.python-profiling.cpu.executable: /opt/diagnostics/py-spy
```

This requires permission to inspect the worker process in the TaskManager's PID namespace.
On Linux, the same user ID alone may not be sufficient because of ptrace, Yama or container
restrictions. On macOS, py-spy requires elevated permissions. Flink does not install the tool
or change operating-system permissions. A tool or permission error is reported in the profiling result. In-process memory profiling has
additional risks described below.

In the TaskManager's **Profiler** tab, select **Python worker** as the target. A single available
worker is selected automatically; with multiple workers, select the process to sample. If the
selected worker exits, the selection is cleared rather than silently changing to another process.
**Include native stacks** and **Include idle threads** control the corresponding sampling options.
The history shows the worker PID and whether native or idle sampling was requested.

To use REST directly, call `GET /taskmanagers/:taskmanagerid/python-workers` to find live workers. Select the returned
`id`, then submit to the existing profiling endpoint:

```text
POST /taskmanagers/:taskmanagerid/profiler
```

```json
{"duration": 10, "workerId": "<id from worker listing>", "type": "CPU", "includeIdle": false, "includeNative": false}
```

Python requests require `type` (`CPU` or `MEMORY`) and omit the JVM `mode` field. The sampling rate is 50 Hz;
`includeIdle` adds waiting threads to the default active-thread sampling. `includeNative` enables
py-spy's native extension stacks, including C/C++ or Cython code where the installed tool and
platform support them. Both options default to false. Native symbol and line-number resolution
depend on the available debug symbols and source files. On Linux AArch64, use a py-spy build with native
unwinding support; the integration has been validated with py-spy 0.4.3 and CPython 3.11.
The py-spy 0.4.1 AArch64 wheel does not support native capture. An unsupported native request fails
with the tool's error message; Flink does not retry it without native sampling. Flame widths represent
sample proportions, not exact CPU time. A worker may serve multiple operators, so its profile
describes the whole process. Worker discovery also lists associated operators with `jobId`,
`operatorId`, `operatorName`, `subtaskIndex` (zero-based), and `attemptNumber`. Use these fields in
the worker selector to distinguish processes on the same TaskManager. A shared worker can list
several operators; its profile does not isolate an individual operator. An empty association list
can mean registration is pending or failed, not that the worker is idle.
Embedded/thread mode and local loopback workers are not included.

Read status from `GET /taskmanagers/:taskmanagerid/profiler?target=python`.
Without `target`, the endpoint continues to return JVM history only. Python sessions use the same
`RUNNING`, `FINISHED` and `FAILED` states and history retention as JVM sessions.
Only one REST capture can be active per Flink JVM, including when multiple TaskManagers share that JVM.
The optional `worker` field records the sampled identity; `type`, `includeNative` and, for CPU,
`includeIdle` record the requested options. On success, download `outputFile` through
`GET /taskmanagers/:taskmanagerid/profiler/:filename`; CPU results are SVG attachments; memory results are Memray `.bin` attachments.
For CPU capture, Flink measures elapsed time from sampler launch and requests a graceful stop
with SIGINT at the deadline. Tool initialization can shorten the actual sampling window. A bounded
finalization period lets py-spy write the result; the session stays RUNNING until it completes.
An unresponsive sampler is terminated and the session fails. Linux/macOS use `/bin/kill` to send
SIGINT; this does not signal the Python worker.

The existing `rest.profiling.duration-max`, `rest.profiling.history-size` and `rest.profiling.dir`
settings apply. History capacity is shared by JVM and Python sessions before filtering.
An unavailable Python profiler returns HTTP 503; a concurrent capture returns HTTP 409.
Missing tools, permission failures, zero samples or worker exit appear in the failed session message.

Set `rest.profiling.dir` to the desired local directory on each TaskManager, for example
`/flink/log/profiling`. Python captures are stored there as `python-<uuid>.svg`; the TaskManager
must be able to create and write to the directory. The directory is also used for JVM results.
History eviction deletes completed session files. Failed CPU captures clean up temporary files;
memory captures retain incomplete files when Tracker shutdown cannot be confirmed.
TaskManager restart does not restore history from files. A persistent volume preserves remaining
files but does not restore profiling sessions; export files separately for long-term retention.

### Memory capture

Install Memray in the Python worker environment. For attach, also configure an absolute Memray CLI
path on the TaskManager; the CLI, worker Python and Memray versions must be compatible. Flink does
not install Memray or its debugger dependencies. The prototype has been validated with Memray 1.20.0,
CPython 3.11.18 and Beam 2.75.0 on Linux AArch64. Tool upgrades require real capture/download checks;
these results do not establish a production overhead guarantee.

Both memory modes require `rest.profiling.enabled: true` and write to `rest.profiling.dir`.
They use Memray inside the worker process. Do not run another Memray Tracker, a manual Memray
attach, or PyFlink cProfile concurrently in the same worker. Profiling reports allocations recorded
during the capture window, not all RSS, container memory or GPU memory.

For one capture per worker startup, configure the job's Python options:

```yaml
python.memory-profiling.startup.enabled: true
python.memory-profiling.startup.delay: 120 s
python.memory-profiling.startup.duration: 60 s
python.memory-profiling.startup.native: false
```

Startup capture uses Memray's public Tracker API without debugger injection. It waits for the delay,
records for the duration, closes the Tracker, and lets the worker continue. The defaults are disabled,
zero delay, 60 seconds and no native stacks. Duration must not exceed `rest.profiling.duration-max`.
Each restarted worker schedules another capture. Native crashes or stalls can still affect the worker;
Python timers are not hard deadlines. Missing dependencies and invalid settings are logged and do not
prevent normal worker startup.

Completed startup files are named `python-memory-startup-<pid>-<uuid>.bin` in the shared result directory.
Their filenames are logged and can be downloaded using the existing TaskManager profiler file endpoint.
Startup captures do not enter REST session history or consume its active-session slot. Deployment
operators own their retention and disk-space management. The earlier `PYFLINK_MEMRAY_*` environment
inputs are private transport details, not supported configuration or aliases.

For an on-demand capture, explicitly enable attach before starting the workers:

```yaml
rest.python-profiling.memory.executable: /opt/diagnostics/bin/memray
rest.python-profiling.memory.attach-enabled: true
```

{{< hint warning >}}
Memray attach/detach injects code through a debugger and may deadlock or crash the worker, causing
job recovery. It requires suitable debugger and process-inspection permissions. Attach is disabled
by default. The TaskManager Profiler UI requires acknowledgment of this risk before submission;
automated REST callers must make the same operational decision.
{{< /hint >}}

Choose **Python worker**, **Memory**, the worker and the duration in the Profiler tab. Worker discovery
reports `memoryProfilingAvailable` only for a prepared, compatible worker. A pending or active startup
capture rejects attach with HTTP 409. An unavailable or disabled attach facility returns HTTP 503.
For REST, submit:

```json
{"workerId": "<id from worker listing>", "type": "MEMORY", "duration": 30, "includeNative": false}
```

`includeIdle` is invalid for memory requests, including when set to false. Flink attaches, waits for
the requested window, and explicitly detaches. A target-side timer provides a fallback; timer expiry
alone does not prove that Tracker shutdown succeeded. Flink reports FINISHED and publishes the `.bin`
only after a successful detach and file finalization. If stopping cannot be confirmed, the session
fails, preserves an `.inprogress` file, and keeps the shared REST slot reserved until that original
worker exits. Flink does not kill the worker to recover the profiler or publish incomplete files
through the download endpoint.

Keep the binary and render it offline in a compatible environment with matching native libraries
and symbols. No report renderer runs on the worker or TaskManager:

```sh
python -m memray flamegraph -o allocations.html python-memory-<uuid>.bin
python -m memray stats python-memory-<uuid>.bin
python -m memray tree python-memory-<uuid>.bin
```


## Troubleshooting
1. **Failed to profiling in CPU mode: No access to perf events. Try --fdtransfer or --all-user option or 'sysctl kernel.perf_event_paranoid=1'** \
That means `perf_event_open()` syscall has failed. By default, Docker container restricts the access to `perf_event_open` syscall. The recommended solution is to fall back to ITIMER profiling mode. It is similar to CPU mode, but does not require `perf_events` support. As a drawback, there will be no kernel stack traces.

2. **Failed to profiling in Allocation mode: No AllocTracer symbols found. Are JDK debug symbols installed?** \
The OpenJDK debug symbols are required for allocation profiling. See [Installing Debug Symbols](https://github.com/async-profiler/async-profiler?tab=readme-ov-file#installing-debug-symbols) for more details.

{{< hint info >}}

You can refer to the [Troubleshooting](https://github.com/async-profiler/async-profiler?tab=readme-ov-file#troubleshooting) page of async-profiler for more cases.

{{< /hint >}}
