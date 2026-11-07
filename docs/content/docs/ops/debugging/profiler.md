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
rest.python-profiling.executable: /opt/diagnostics/py-spy
```

This requires permission to inspect the worker process in the TaskManager's PID namespace.
On Linux, the same user ID alone may not be sufficient because of ptrace, Yama or container
restrictions. On macOS, py-spy requires elevated permissions. Flink does not install the tool
or change operating-system permissions. A failed diagnostic request does not fail the job.

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
{"duration": 10, "workerId": "<id from worker listing>", "includeIdle": false, "includeNative": false}
```

Omit the JVM `mode` field when selecting a Python worker. The sampling rate is 50 Hz;
`includeIdle` adds waiting threads to the default active-thread sampling. `includeNative` enables
py-spy's native extension stacks, including C/C++ or Cython code where the installed tool and
platform support them. Both options default to false. Native symbol and line-number resolution
depend on the available debug symbols and source files. On Linux AArch64, use a py-spy build with native
unwinding support; the integration has been validated with py-spy 0.4.3 and CPython 3.11.
The py-spy 0.4.1 AArch64 wheel does not support native capture. An unsupported native request fails
with the tool's error message; Flink does not retry it without native sampling. Flame widths represent
sample proportions, not exact CPU time. A worker may serve multiple operators, so its profile
describes the whole process. Embedded/thread mode and local loopback workers are not included.

Read status from `GET /taskmanagers/:taskmanagerid/profiler?target=python`.
Without `target`, the endpoint continues to return JVM history only. Python sessions use the same
`RUNNING`, `FINISHED` and `FAILED` states, history retention and concurrency limit as JVM sessions.
The optional `worker` field records the sampled identity; `includeNative` and `includeIdle` record the requested options. On success, download `outputFile` through
`GET /taskmanagers/:taskmanagerid/profiler/:filename`; Python results are SVG attachments.
The existing `rest.profiling.duration-max`, `rest.profiling.history-size` and `rest.profiling.dir`
settings apply. History capacity is shared by JVM and Python sessions before filtering.
An unavailable Python profiler returns HTTP 503; a concurrent capture returns HTTP 409.
Missing tools, permission failures, zero samples or worker exit appear in the failed session message.

Set `rest.profiling.dir` to the desired local directory on each TaskManager, for example
`/flink/log/profiling`. Python captures are stored there as `python-<uuid>.svg`; the TaskManager
must be able to create and write to the directory. The directory is also used for JVM results.
History eviction deletes the corresponding files, and failed captures clean up temporary files.
TaskManager restart does not restore history from files. A persistent volume preserves remaining
files but does not restore profiling sessions; export files separately for long-term retention.

## Troubleshooting
1. **Failed to profiling in CPU mode: No access to perf events. Try --fdtransfer or --all-user option or 'sysctl kernel.perf_event_paranoid=1'** \
That means `perf_event_open()` syscall has failed. By default, Docker container restricts the access to `perf_event_open` syscall. The recommended solution is to fall back to ITIMER profiling mode. It is similar to CPU mode, but does not require `perf_events` support. As a drawback, there will be no kernel stack traces.

2. **Failed to profiling in Allocation mode: No AllocTracer symbols found. Are JDK debug symbols installed?** \
The OpenJDK debug symbols are required for allocation profiling. See [Installing Debug Symbols](https://github.com/async-profiler/async-profiler?tab=readme-ov-file#installing-debug-symbols) for more details.

{{< hint info >}}

You can refer to the [Troubleshooting](https://github.com/async-profiler/async-profiler?tab=readme-ov-file#troubleshooting) page of async-profiler for more cases.

{{< /hint >}}
