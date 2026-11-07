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

# FLIP-XXX: On-demand CPU Profiling for Python Workers

[中文](FLIP-draft.md) | **English**

| Item | Value |
| --- | --- |
| Status | Local discussion draft; scope and interfaces have not undergone community discussion or voting |
| Discussion thread | To be started |
| Vote thread | To be started |
| JIRA | To be created or linked |
| Release | To be determined |
| Updated | 2026-11-07 |

This proposal adds timed stack sampling for standalone Python workers managed by Flink to help
identify CPU hotspots. Target selection, status queries, and flame graph downloads are provided
through the existing TaskManager Profiler page. The recommended tool is an administrator-installed
py-spy executable, reusing Flink's profiling sessions, concurrency limit, results directory, and
download infrastructure. Memory issues are another part of the diagnostic need, but Memray integration
is a separate experiment in Appendix A, outside this iteration's delivery commitment.
The main text specifies the recommended public contracts; Appendix B distinguishes the prototype
from changes still required. The document follows the [FLIP Template][flip-template].

## Motivation

In PyFlink's process execution mode, user functions and the associated data processing run in
separate Python worker processes. In workloads such as real-time multimodal processing, these workers
also perform image decoding, feature transformation, and model inference. For example, a job may load
a YOLO model through PyTorch and continuously preprocess incoming images and run inference on them.
Image decoding, tensor conversion, data encoding and decoding, and model computation increase CPU
usage. Model parameters, images, tensors, and intermediate batch results increase process memory
requirements. These costs can originate in both Python code and native code within dependencies.
[PyTorch execution-time and tensor-memory profiling][pytorch-profiling]

These workloads have two typical diagnostic scenarios:

- **High CPU usage accompanied by lower throughput or higher processing latency.** Users need to
  identify the call paths that account for the time spent, such as image decoding and preprocessing,
  data conversion and encoding or decoding, user functions, or native computation within a framework,
  before they can decide what to optimize.
- **Sustained growth or high peaks in Python worker memory usage, putting the container at risk of
  OOM.** Users need to identify the call paths responsible for allocations during the relevant period
  and the allocations that remain unfreed. They need to relate this evidence to model loading,
  intermediate images and tensors, and batch buffering to understand the sources of memory usage.

CPU utilization and process memory metrics reveal resource pressure, but are insufficient to locate
its cause in code. Users also need call stacks or allocation stacks from the period in which the
problem occurs, together with results that make hotspots easy to inspect, save, and share. Allocation
records must be interpreted alongside process memory metrics; allocations that remain unfreed at the
end of a capture are not, by themselves, evidence of a memory leak.

Flink's existing diagnostic capabilities do not fully address these needs. [FLIP-375][flip-375]
provides JVM profiling initiation, status queries, and result downloads for JobManagers and
TaskManagers, but cannot directly show the call stacks of separate Python workers. PyFlink's existing
`python.profile.enabled` integration uses cProfile to collect function-call statistics during bundle
processing and writes them to logs. This integration helps analyze function execution time, but does
not provide a complete workflow for choosing capture windows on demand while workers are running,
downloading profiling files, and viewing visual reports. It also does not provide memory allocation
analysis. Tracking individual function calls can add substantial overhead to call-intensive workloads,
affecting throughput and backpressure. Online diagnostics therefore need to account for the impact of
profiling itself on the job.

The Python ecosystem already offers mature profiling tools, but using them in a distributed deployment
still involves operational work. Users typically need to access the target Pod or host, identify the
corresponding Python worker, prepare the tools and required permissions, run a capture, and retrieve
the results. Tools that collect data inside the process also need their capture lifecycle to be
managed without scattering diagnostic code across user functions. These steps lack a consistent
integration with Flink's runtime management, leaving routine CPU and memory investigations dependent
on multiple manual operations.

This proposal aims to reduce the effort required to trace Python worker resource problems to specific
code. Users should be able to obtain results that can be saved and analyzed visually, with explicit
enablement and bounded capture windows, without modifying their user functions. Profiling should
minimize disruption to normal processing and reuse Flink's existing process management and profiling
capabilities, making Python worker diagnostics a routine part of job operations.

### Goals and non-goals

The initial scope is standalone Python workers on Linux, on the same host and in the same PID namespace
as the TaskManager (TM). The intended users are operators and developers with existing cluster
diagnostic privileges. After identifying CPU pressure in a Python process through existing monitoring,
a user selects the corresponding worker on the TM page, captures stacks for a specified period, and
downloads a report to locate hotspots, without modifying the UDF. Administrators must prepare the
tool, permissions, and enablement configuration in advance. This does not promise configuration-free
attachment to arbitrary running processes that were not prepared for profiling.

| In scope | Out of scope |
| --- | --- |
| Worker discovery and selection, timed sampling, optional native stacks and idle threads, capture status, SVG downloads | Memory allocation capture, GPU kernel/device-memory analysis, dedicated I/O analysis |
| A configurable TM-local results directory and reuse of existing history and cleanup | Central artifact storage, cross-TM aggregation, continuous profiling, automated diagnosis |
| Process-wide results and explicit deployment instructions | Exclusive operator attribution, automatic task-to-worker mapping, new process metrics |

A worker may serve multiple operators; a report covers the process and cannot be attributed exclusively
to one operator. Embedded/thread execution, client-side loopback, remote workers, and child processes
created by user code are outside the initial scope. macOS prototype experiments do not establish
release support. Specific Linux architectures, CPython versions, and py-spy versions must be validated
with real captures before they are listed as supported.

## Public Interfaces

### Configuration

Add only one tool-path option and reuse the existing configuration. No public Java/Python UDF API or
profiler SPI is introduced.

| Configuration | Default | Contract |
| --- | --- | --- |
| `rest.python-profiling.executable` | None; new | Absolute path to the administrator-installed py-spy executable, not a generic command or plugin entry point. Python sampling is unavailable when the path is not configured. |
| `rest.profiling.enabled` | `false`; existing | Main profiling switch. Python sampling also requires the tool path. |
| `rest.profiling.duration-max` | `300 s`; existing | Maximum capture duration per request. |
| `rest.profiling.history-size` | `10`; existing | Combined limit on JVM and Python sessions retained for a TM, using the existing rolling eviction policy. |
| `rest.profiling.dir` | `java.io.tmpdir`; existing | Local results directory used by the process performing capture. Python results are written to this directory on the TM hosting the target worker. |

For example, set the following in the cluster configuration:

```yaml
rest.profiling.enabled: true
rest.python-profiling.executable: /opt/bin/py-spy
rest.profiling.dir: /flink/log/profiling
```

An absolute results path is recommended; existing path-resolution semantics for `rest.profiling.dir`
are preserved. Configuration is a deployment-level contract. Capture requests cannot change the tool
path or output directory. The initial version requires TMs and workers to start with profiling enabled;
it adds neither dynamic configuration reload nor retrospective discovery of unregistered workers.

### Result directory and retention

A successful capture creates `<rest.profiling.dir>/python-<uuid>.svg`, with the filename generated by
Flink. JVM profiling continues to use the same configuration option. Users can retrieve files directly
from the target TM's directory or through the existing REST/Web UI download path. The JobManager
forwards downloads; it is not the persistent storage location for Python artifacts. The TM must be
able to create and write to the directory. Directory creation, write, and disk-space failures must
fail the capture; diagnostic control logic must not cancel the job because of these failures.

History eviction deletes the corresponding local result file. The capture lifecycle cleans up
failed captures' temporary files. Cleanup applies only to files owned by this functionality; it must
not scan or empty the entire configured directory. The existing BLOB download cache is reused;
eviction does not promise immediate revocation of already downloaded or cached copies.
History remains in memory. A restarted TM does not scan the directory to reconstruct history.
A persistent volume can preserve files that have not been cleaned up, but cannot restore historical
sessions. Long-term archival is handled by the deployment environment; this FLIP adds no remote
storage or archival configuration.

### Worker discovery interface

Add `GET /taskmanagers/:tm/python-workers`, returning live candidate workers known to this TM.
Being listed does not guarantee that the operating system permits sampling; permission and tool
execution failures can still occur during capture.

```json
{
  "workers": [
    {"id": "<opaque-worker-handle>", "pid": 12345}
  ]
}
```

`id` is an opaque handle bound to this TM and this worker process incarnation, used in creation
requests. `pid` is for display and manual correlation with monitoring. When a worker exits or restarts,
its old handle becomes invalid and never automatically refers to a successor process. Return an
empty array when no live workers are available. Internal start identifiers, Beam worker IDs, and the
registration-file format are not public fields. Users cannot submit arbitrary PIDs.
The internal discovery approach is described under Proposed Changes; the interface does not commit
to a particular registration mechanism.

### Capture requests

Extend the existing `POST /taskmanagers/:tm/profiler`. Existing JVM requests continue to use `mode`
and `duration`. A Python request is:

```json
{
  "workerId": "<opaque-worker-handle>",
  "duration": 30,
  "includeNative": false,
  "includeIdle": false
}
```

| Field | Contract |
| --- | --- |
| `workerId` | Required for Python requests. A valid handle returned by this TM's listing; mutually exclusive with the JVM `mode`. |
| `duration` | Required positive integer in seconds, no greater than `rest.profiling.duration-max`. |
| `includeNative` | Optional, default `false`. Request native call stacks associated with Python threads. |
| `includeIdle` | Optional, default `false`. Include idle threads in the samples. |

The latter two fields accept only booleans and apply only to Python requests. No arbitrary tool
arguments, output-format option, or sampling-rate option is added. Unsupported explicitly requested
sampling options cause failure without silent fallback. The JobManager profiler does not accept
Python worker requests.

Accepted requests continue to return HTTP 200 and session information, using `RUNNING`, `FINISHED`,
and `FAILED`. Acceptance does not mean samples have already been collected; tool startup failure may
immediately put the session in `FAILED`. Python sessions retain `status`, `triggerTime`, `finishedTime`,
`duration`, `message`, and `outputFile`, adding `worker` (the `id` and `pid` above) and the two effective
sampling options. Python sessions omit the JVM-only `mode` and existing `profilingMode` alias,
matching optional enum fields in the generated schema. Stack samples are not presented as JVM CPU
events. Time and filename fields retain their existing meanings. `outputFile` contains the generated
filename; clients cannot specify a local path.

### History and downloads

| Endpoint | Contract |
| --- | --- |
| `GET /taskmanagers/:tm/profiler` | Continue to return JVM history, without introducing Python sessions to existing clients. |
| `GET /taskmanagers/:tm/profiler?target=jvm` | Same as a request without the parameter. |
| `GET /taskmanagers/:tm/profiler?target=python` | Explicitly return Python history in the existing `profilingList` wrapper. |
| `GET /taskmanagers/:tm/profiler/:filename` | Reuse the existing file download path. |

`target` is a new optional query parameter accepting only `jvm` and `python`, defaulting to `jvm`.
Python results appear only in Python creation responses and explicitly selected Python queries.
Existing JVM clients therefore do not receive a new null `mode` when the feature is enabled.
History retains its existing ordering. The combined size limit is applied before target filtering;
it does not retain `history-size` entries for each type. Python sessions can evict older JVM sessions
when Python capture is enabled. Configuration documentation must explain this resource-sharing behavior.

JVM and Python captures on a TM share the existing active-session limit. No queue, cancellation
endpoint, or cluster-wide scheduler is added. Python SVGs are downloaded as attachments with `nosniff`
and a restricted CSP, rather than embedded in the Dashboard as inline script. Existing JVM report
download behavior is preserved.

### Failure semantics

The following contracts apply to new Python requests and use Flink's standard error response
structure. They do not change error handling for existing JVM requests.

| Condition | Result |
| --- | --- |
| Invalid parameter type, combination, duration, or `target` | HTTP 400. |
| TM or worker handle missing, or handle invalid before creation | HTTP 404. |
| Another profiling session is active at creation | HTTP 409; do not queue or overwrite it. |
| Python tool not configured or platform known to be unsupported | HTTP 503; existing JVM functionality remains usable. |
| Permission failure, tool failure, worker exit, timeout, or result-write failure after acceptance | Session becomes `FAILED` with diagnostic information; reap the sampler and clean temporary artifacts. |

When the main switch is disabled, preserve the existing behavior in which profiling endpoints are
unavailable. Discovery failures must return errors rather than masquerade as a normal empty worker
list. Diagnostic control logic must not terminate the application worker to end capture. Sampling
can still briefly pause the target process, and its impact must be measured.

### Web UI

Reuse the TM Profiler page: choose JVM/Python, select a worker, specify a duration, capture, and
download. Python mode shows the PID and handle, offers native/idle advanced options, and explains
that results are process-wide. A single worker may be selected by default. Clear the selection when
that worker becomes invalid; never switch automatically to another process. Show history for the
selected target type. When the shared session limit rejects a request, explain that another capture
is already running. Distinguish missing configuration, no workers, and discovery failure. Python
errors must not block JVM operations. The page needs neither a local-path input nor a separate viewer.

## Proposed Changes

### Reuse and ownership

[FLIP-375][flip-375] established the request, session, and result-download infrastructure. This
proposal adds Python targets and tool execution to that infrastructure instead of creating another
profiling service.

| Module | Responsibility |
| --- | --- |
| Python environment preparation and worker startup in `flink-python` | Keep process lifecycle management in Beam and provide the TM with the minimum metadata needed to identify the actual Python worker. |
| REST and RM/TM RPC in `flink-runtime` | Validate public requests, forward them, and filter history by target, reusing the existing routing. |
| Profiling in `flink-runtime` | Resolve targets, execute py-spy under existing session limits, record terminal state, and clean up resources. |
| Existing file and BLOB infrastructure | Download results from the configured directory by generated filename. |
| `flink-core`, Web UI, and `docs` | Configuration definitions, integration into the existing page, and generated configuration and REST documentation. |

No capture hook is added to per-record processing, UDF invocation, or bundle handling.
`flink-runtime` does not depend on Beam/Python execution implementation, and third-party profiler APIs
do not need to become a Flink SPI. No sampler is launched when disabled. When enabled but not sampling,
only the management metadata needed for worker discovery is retained.

### Worker identity and process management

Reuse the existing Python environment and startup path. Beam continues to own process creation,
exit, and restart management. The TM needs the PID and process incarnation of the Python process
actually running the SDK harness, not the PID of a shell wrapper. Prefer obtaining this information
at existing startup/process-management integration points. Add a minimal metadata bridge only if
Beam's current public interfaces cannot provide it.

The prototype has standalone workers write registration records in the TM's private working
directory before entering the harness and remove them at exit. This supplies the actual PID without
adding a second process manager, heartbeat service, or user-facing registration endpoint. Public
handles are separate from internal records; start identifiers and Beam IDs remain internal. If
obtaining equivalent information directly from the launcher is simpler, that mechanism can replace
the record-writing approach without changing the public interface.

The TM checks process liveness and incarnation during listing, creation, capture, and before publishing
results, and verifies that the target belongs to its managed processes. PID reuse must not silently
retarget an old handle. py-spy ultimately attaches by PID, so validation and attachment are not atomic.
This residual race must be explained during review; neither the registration directory nor a handle
is a multi-tenant security isolation mechanism. This iteration does not add a worker control protocol
to eliminate that tool limitation.

### Sampling and lifecycle

Requests pass through JM REST, the RM, and the TM to the existing `ProfilingService`. Waiting for the
external tool uses the profiling executor, without blocking TM RPC or data-processing threads.
Launch py-spy with fixed argv, without a shell. Validate the following command at a default of 50 Hz,
adding `--native` or `--idle` as requested:

```text
/configured/py-spy record --pid <resolved PID> --duration <seconds>
  --rate 50 --threads --format flamegraph --output <generated path>
```

Flame graph widths represent proportions of captured stack samples. By default, threads that the
tool considers idle are excluded, but this is still not precise CPU-time accounting. Enabling idle
also includes waiting. Native mode does not guarantee coverage of native thread pools without Python
thread state and does not capture GPU kernels. These limitations matter particularly for workloads
such as PyTorch and must be demonstrated in support documentation and real examples. They do not
justify promising complete attribution of model performance.

Publish `FINISHED` only after successful tool exit and a complete artifact containing valid samples.
The prototype's non-empty SVG check alone does not establish successful capture. The implementation
must use validated tool completion information or result checks to identify zero-sample results.
Reap the sampler when the worker becomes invalid, capture times out, or the service shuts down,
while preserving the application's normal worker lifecycle. Timeout grace periods and file/log size
limits are internal resource controls, not new configuration options. Do not wait indefinitely for
tool exit or buffer unbounded output. Log cleanup failures without preventing a terminal session state.

### Deployment, access, and tool support

Administrators preinstall py-spy in the TM environment. It is not bundled in the Flink distribution;
Flink neither runs pip installation nor elevates privileges automatically. Release documentation
lists validated py-spy versions, Linux architectures, and CPython versions, distinguishing ordinary
sampling from native mode. Do not assume that the sampling tool supports every Python version
supported by PyFlink. PID visibility, UID, Yama, seccomp, and related conditions require real container
validation; sharing a UID does not guarantee permission. At activation, INFO logs record effective
enablement, tool path, results directory, and sampling parameters. Tool failures retain bounded
diagnostic information.

Reuse the deployment access boundary of existing Flink REST. Users of this entry point should already
have diagnostic privileges for the target TM. Unlike the JVM profiler, this adds external program
execution and therefore needs explicit constraints: the executable and directory come from
administrator configuration; requests accept only discovered workers and typed parameters; downloads
resolve controlled filenames. Requests cannot supply shell commands, arbitrary PIDs, or file paths.
Reports can contain function names and source paths. Apply the same access management to the results
directory and downloads as to other diagnostic artifacts. This FLIP adds no authentication system
and does not claim new isolation between mutually untrusted jobs.

## Compatibility, Deprecation, and Migration Plan

- Disabled by default. Existing JVM creation requests, default history queries, and report downloads
  retain their formats. Python metadata is returned only through explicit Python operations.
- JVM and Python share session concurrency and history capacity. Enabling Python capture may reject
  a simultaneous JVM capture or evict older records. This is explicit resource-sharing behavior.
- Existing `python.profile.enabled` and cProfile behavior remain unchanged. There is no mandatory
  migration or automatic tool switching.
- No changes to UDF APIs, per-record execution, checkpoints/savepoints, or state serialization, and
  no new Flink Java/Python runtime dependencies.
- Enabling configuration requires restarting the relevant processes through existing deployment
  procedures. Once enabled and discoverable, workers can be sampled on demand while running,
  without restarting the job for each CPU capture.
- No API deprecation or state migration. New configuration, query parameters, response fields, and
  conditional constraints must be covered by generated configuration and REST/OpenAPI documentation
  and stability tests.

## Test Plan

Acceptance centers on complete user workflows and real sampling. Simulated SVGs or backend unit tests
alone cannot establish that the feature is usable.

| Scenario | Acceptance evidence |
| --- | --- |
| A real Linux CPU job | Select a worker from the list, capture through REST and the page, download the SVG, and locate a predetermined UDF hotspot; business output remains correct. |
| A representative multimodal workload | Image preprocessing and a known C extension call; record the actual resolution of native stacks and distinguish Python threads from native thread pools. |
| Lifecycle and sharing | Multiple workers, shared workers, restarts, and PID/old-handle invalidation; sampling never switches targets, and history consistently identifies process-wide scope. |
| File lifecycle | Set a TM-local directory and verify artifact location and download contents. Cover unwritable directories, disk-write failures, history eviction, failed captures' temporary files, preservation of unrelated files, and absence of history recovery after restart. |
| Errors and cleanup | Missing permission, missing tool/unsupported options, zero samples, timeout, worker exit, and TM shutdown; verify visible failure, sampler cleanup, and successful admission of subsequent captures. |
| Compatibility | Exercise the changed service with existing JVM request/response examples. The default list contains no Python records; explicit queries follow the new contract. Verify shared capacity, concurrency limits, and unchanged JVM downloads. |
| REST and UI | Distinguish 400/404/409/503 from session FAILED. Reject invalid paths/handles; distinguish empty lists from discovery failure; do not switch targets when a selected worker becomes invalid. |
| Overhead | Alternate disabled, enabled-but-idle, and capturing conditions on the same workload. Record throughput, end-to-end latency, and worker/TM/sampler CPU and memory. Report ordinary and native modes separately. |

Unit tests cover protocol and lifecycle. MiniCluster integration tests cover REST/RPC/history/downloads.
Linux jobs verify real tool behavior. Overhead experiments record environment, versions, sampling rate,
thread count, duration, repetitions, and baseline variability. Publish measurements rather than
presenting tool benchmarks as guarantees for application workloads. Disabled and enabled-but-idle
conditions should show no repeatable throughput regression. Review the sampling budget against
representative workloads.

Before starting a vote, provide at least one real Linux capture and resolve scope and interface
decisions in Open Questions. Before merging, complete the key acceptance scenarios above, affected
module checks, configuration/REST documentation generation, and full verification required by the
project. List only tested combinations in the support matrix. Prototype tests do not replace tests
for the new directory, filtering, and error contracts.

## Rejected Alternatives

| Alternative | Reason for the recommendation |
| --- | --- |
| Only improve cProfile export and visualization | cProfile can already produce inputs for visualization and remains useful for controlled function analysis. This iteration prioritizes on-demand process-wide sampling of running workers with limited disruption. cProfile is not an automatic fallback for permission failures. |
| Use the JVM profiler to explain a separate Python worker | It samples a different process and cannot obtain Python stacks from that worker. |
| Create a separate control service and results store for each tool | Existing sessions, limits, status, directory, and BLOB downloads are sufficient for the initial version. |
| Mix JVM/Python records into default history and require old clients to handle null mode | This turns prototype implementation details into a compatibility burden. An explicit query parameter lets callers opt in. |
| Add dedicated Python creation/history endpoints | Also viable. The current recommendation extends existing requests and target filtering to keep management together. If REST/OpenAPI cannot clearly express conditional responses, choose separate routes before the vote while retaining the same internal infrastructure. |
| Accept arbitrary PIDs, tool arguments, or output paths | This shifts deployment and target-validation responsibility to callers and expands the authority of the diagnostic entry point. |
| Ship Memray attach, Scalene, and a generic profiler SPI in the first version | In-process tools have different preparation and stopping requirements. There is insufficient evidence for a stable common interface; first complete the appendix experiment. |

See the [tool-selection research](Profiler-selection-research.md) and
[cross-framework comparison](Distributed-framework-profiling-research.md) for supporting analysis.

## Open Questions

These decisions require explicit community trade-offs. Recommendations are not approved conclusions.

| ID | Decision | Recommendation and remaining evidence |
| --- | --- | --- |
| D1 | Whether the initial scope should be CPU only | Review the CPU feature independently. Keep the memory experiment outside this iteration's public commitment and leave room for a later proposal. |
| D2 | Whether worker ID/PID is sufficient to select the target | Start with a TM-level entry point and manual correlation through existing logs/monitoring. Demonstrate usability in a real deployment with multiple jobs and workers. |
| D3 | Conditional REST responses and history compatibility | Default to JVM only; explicitly request `target=python`. Check generated schemas and old client examples before voting; choose dedicated Python routes if necessary. |
| D4 | Supported tool matrix and acceptable sampling overhead | Administrators install py-spy. Start with real results from one specified Linux environment and review coverage and overhead separately for default/native modes. |

## Appendix A: Memory capture experiment

This experiment is outside this iteration's public interfaces and delivery commitment. Its purpose
is to establish whether useful allocation stacks can be collected within a specified window without
changing UDFs, while the same worker continues processing after capture closes. It adds no memory
REST endpoints, UI, centralized scheduling, or public Flink configuration.

Distinguish out-of-process sampling from in-process capture by where collection logic runs. `attach`
does not imply out-of-process collection: Memray attach starts a Tracker inside the target. This
experiment uses Memray's public `Tracker` API at the existing standalone worker startup entry point,
running the Beam harness in the same process. The first step provides four experimental inputs:
delay, duration, output directory, and native traces. For example, start 120 seconds after startup,
capture for 60 seconds, once per worker start. The startup integration owns and closes the Tracker,
recording actual start/end times and completion status. It does not stop capture by killing or
restarting the worker. Timed stopping still depends on Python getting an opportunity to execute;
only a `.bin` file whose closure the tool has confirmed is marked complete.

Prefer reusing Python dependency preparation and environment propagation. A future formal integration
should share `rest.profiling.dir`, but the current startup-hook experiment does not read that
Flink setting and is not managed by REST session limits, history cleanup, or downloads. The experiment
specifies a directory directly, retrieves files through existing deployment procedures, and generates
Memray reports offline. Extract common Memray/Scalene logic only after actual integrations establish
it. Do not design an SPI in advance or add an arbitrary-code-execution interface for this purpose.

Interpret allocation stacks alongside RSS/PSS and the capture window. Enabling native traces adds
C/C++ stacks; disabling it does not mean native allocations are not recorded. The experiment cannot
explain all container memory or GPU memory and does not guarantee complete reports after OOM/SIGKILL.
Validate known Python/native allocations, delayed windows, continued processing after closure, file
size, and throughput impact, starting with selected small diagnostic jobs. Dependency and directory
preparation failures should preserve normal startup, but native tracing failures cannot be guaranteed
to leave the target unaffected.
[Memray Tracker API][memray-api] · [Native mode][memray-native]

Beam 2.75.0's container profiler_agent already wraps Memray, but it is part of a Go boot path that
PyFlink currently does not use. Its timed stop interrupts and restarts the SDK worker. This proposal
learns from its preparation and artifact handling without adopting that stopping strategy. Dask's
runtime Tracker integration suggests that existing control paths can be explored later; it does not
establish a runtime protocol commitment for this version.

The local experiment is now implemented in `beam_boot` using four `PYFLINK_MEMRAY_*` environment
variables. One background thread owns Tracker and cancels or closes capture on normal harness exit.
Only successfully closed `.bin.inprogress` files become `.bin` artifacts. All 14 unit tests pass.
Immediate and delayed/native captures on Linux standalone identify known allocations, and the same
worker continues processing after closure. See the [memory experiment](Memory-experiment.md).

## Appendix B: Prototype status and references

The local implementation uses upstream baseline
`8d01380ff5bf9892462ef9808b1b56fd9723f54d`. This revision implements public descriptor reduction,
JVM-only default history and explicit Python filtering, error mapping, strict parameter types,
zero-sample failure detection, and result-directory lifecycle tests. Python responses omit JVM-only
mode fields to match optional fields in the generated schema.

On 2026-11-07, a local Linux AArch64 container ran standalone JM/TM processes and a real PyFlink worker.
The environment was CPython 3.11.18, Beam 2.75.0, Java 17.0.21, and py-spy 0.4.3. Ordinary and native
sampling both produced real flame graphs. Ordinary reports contain Python preprocessing and the
compression call site; native reports additionally resolve zlib's `deflate`. Downloaded bytes match
files in the configured TM directory. JVM ITIMER capture also succeeds. Browser-triggered capture
was exercised and screenshots saved; visual review found and fixed compressed Target/Mode columns.
Review fixes also connect Python capture cancellation and completion to the owning TM's shutdown,
while preserving the shared profiling service. Creation responses now refresh the selected target's
history instead of inserting a potentially stale response into it.

| Area | Evidence |
| --- | --- |
| Backend, protocol, and lifecycle | 69 runtime tests pass, covering TaskExecutor, MiniCluster REST/RPC/download integration, shutdown cleanup, late worker lookups, and explicit error statuses. |
| Page and interface calls | 37 frontend tests cover late creation responses after target changes and duplicate history entries; production build and affected-file lint checks pass. |
| Compatibility and generated documentation | The REST stability snapshot passes; all 83 existing calls in the upstream baseline retain their fields and types. Configuration and REST/OpenAPI documentation is updated through project generators. |
| Real tool | Ordinary/native capture succeeds on Linux AArch64 with py-spy 0.4.3. The 0.4.1 wheel's unsupported native mode is correctly reported as FAILED. |
| Remaining coverage | Full-repository `clean verify`, additional platforms/CPython combinations, the Cython fast path, Torch/YOLO workloads, and controlled overhead benchmarks. Memray remains a separate experiment. |

See the [standalone validation report](../../../output/playwright/pyflink-20261107/run.md) for evidence, screenshots, and reproduction inputs.
This is functional validation: sample proportions are not precise CPU time and do not establish
production overhead guarantees. The prototype's 50 Hz rate, 5-second grace period, 20 MiB graph limit,
1 MiB tool-log limit, and 4 KiB error summary remain internal implementation parameters.
Historical prototype records are in the [README](README.md#evidence-and-limitations).

- [Prototype CPU architecture](architecture.png) and [draw.io source](architecture.drawio) illustrate reuse; the current main text defines the public interfaces.
- [cProfile and py-spy selection research](Profiler-selection-research.md).
- [Distributed Python framework comparison](Distributed-framework-profiling-research.md), covering Spark, Ray, Dask, Beam/Dataflow, PyTorch, and Celery.
- [Ray 2.60.0 / Memray 1.20.0 source review](Ray-profiling-research.md).
- VVR reference repository `/Users/kenken/ali/review/flink`, reviewed at `1.20-vvr-11.10-SNAPSHOT`, `d4d3f87aed6f8397505c71961ad8d0a57792d311`. Borrow worker-level sampling without porting its entire control path.
- [FLINK-39153](https://issues.apache.org/jira/browse/FLINK-39153) and the [metrics draft](https://docs.google.com/document/d/1ovs29Pup8bRpV4qY8wI611nYV55zf771tJXQksPwehw/edit) concern a separate metrics effort and are not prerequisites for this proposal.
- [py-spy behavior reference](https://github.com/benfred/py-spy/blob/v0.4.2/README.md); release support must still be based on validated versions.

[flip-template]: https://cwiki.apache.org/confluence/spaces/FLINK/pages/65145553/FLIP+Template
[flip-375]: https://cwiki.apache.org/confluence/spaces/FLINK/pages/272927211/FLIP-375%2BBuilt-in%2Bcross-platform%2Bpowerful%2Bjava%2Bprofiler
[memray-api]: https://bloomberg.github.io/memray/api.html
[memray-native]: https://bloomberg.github.io/memray/native_mode.html
[pytorch-profiling]: https://docs.pytorch.org/tutorials/recipes/recipes/profiler_recipe.html
