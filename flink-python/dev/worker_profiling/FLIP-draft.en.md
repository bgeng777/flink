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

# FLIP-XXX: CPU and Memory Profiling for Python Workers

[中文](FLIP-draft.md) | **English**

| Item | Value |
| --- | --- |
| Status | Local discussion draft; scope and interfaces have not been discussed or voted on by the community |
| Discussion thread | To be started |
| Vote thread | To be started |
| JIRA | To be created or linked |
| Target release | To be determined |
| Updated | 2026-11-07 |

This proposal adds CPU and memory profiling for Flink-managed standalone Python workers.
CPU uses external py-spy sampling. Memory supports both direct Tracker integration at worker startup
and on-demand Memray attach with explicit risk acknowledgement in the UI. Both memory modes are
part of the formal proposal, with optional, deployment-provided dependencies.

Reuse Flink's existing profiling routes, sessions, result directory, and file/BLOB downloads wherever
the trigger permits. No new worker-control protocol or generic profiler framework is proposed.
This document describes the target contracts; Appendix A separates implemented prototype evidence
from work still required. It follows the [FLIP Template][flip-template].

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

The first release covers standalone Python workers on Linux, on the same host and in the same PID
namespace as the TaskManager (TM). Administrators prepare the tools and permissions in advance.
Users do not modify UDFs. A worker may serve several operators; reports describe the whole process.

| Capability in scope | First-release behavior |
| --- | --- |
| CPU profiling | Select a running worker in the existing TM Profiler page and capture with external py-spy sampling. |
| Memory profiling at startup | Enable a startup hook that uses Memray's public `Tracker` API in the worker, with an optional delay and one recording window per worker start. |
| On-demand memory profiling | Select a running worker and use Memray `attach/detach`, following Ray's integration pattern. This mode is separately enabled and carries an explicit injection-risk warning. |
| Results | CPU flamegraph SVG and native Memray `.bin` recordings in the configured TM directory; reuse existing file/BLOB downloads. Memray generates HTML, stats, and tree reports offline. |

Both memory modes are formal features of this proposal. They have different activation and lifecycle
contracts. Automatic memory HTML rendering, a new viewer, continuous profiling, GPU analysis,
operator-exclusive attribution, remote storage, and a generic profiler SPI are outside the first
release. Embedded/thread mode, client loopback, remote workers, and user-created subprocesses are
also outside scope. Publication support requires validation of concrete tool/CPython/Linux combinations.

## Public Interfaces

### Configuration and optional dependencies

Flink provides the integration and maintains its contracts. Tools remain optional deployment
dependencies: py-spy must be executable by the TM; Memray must be importable by the worker's actual
Python environment. Installing a package in a different interpreter in the same Pod is insufficient.
On-demand memory profiling additionally requires a configured Memray CLI and its injection
prerequisites. CPU and memory dependencies are independent; worker registration/discovery must be
available when either capability is configured, not gated solely by the py-spy path. Flink does not install packages, invoke sudo, or change host/container permissions.

| Configuration | Default | Contract |
| --- | --- | --- |
| `rest.profiling.enabled` | `false`, existing | Master switch for all profiling modes in this proposal. |
| `rest.profiling.dir` | `java.io.tmpdir`, existing | Shared local result directory for JVM, Python CPU, and both memory modes. |
| `rest.profiling.duration-max` | `300 s`, existing | Maximum requested recording duration, including startup memory windows; not a hard deadline for injected code. |
| `rest.profiling.history-size` | `10`, existing | Combined REST-session history per resource ID, including JVM, Python CPU, and memory attach sessions. |
| `rest.python-profiling.cpu.executable` | Unset, new | Absolute path to the administrator-installed py-spy executable; not a generic command entry point. |
| `rest.python-profiling.memory.executable` | Unset, new | Absolute path to the Memray CLI used by the TM for attach/detach. Not required for direct startup-API capture. |
| `rest.python-profiling.memory.attach-enabled` | `false`, new | Explicit deployment opt-in for injecting Memray into running workers. Requires the master switch and Memray CLI. |
| `python.memory-profiling.startup.enabled` | `false`, new | Job-level opt-in for one startup memory recording in each standalone worker used by the job. |
| `python.memory-profiling.startup.delay` | `0 s`, new | Non-negative delay from the worker startup hook to Tracker activation. |
| `python.memory-profiling.startup.duration` | `60 s`, new | Positive recording duration, bounded by the TM's maximum requested duration. |
| `python.memory-profiling.startup.native` | `false`, new | Include C/C++ allocation stacks in the startup recording. |

For example, cluster configuration can provide both tools while leaving attach disabled:

```yaml
rest.profiling.enabled: true
rest.profiling.dir: /flink/log/profiling
rest.python-profiling.cpu.executable: /opt/diagnostics/bin/py-spy
rest.python-profiling.memory.executable: /opt/diagnostics/bin/memray
rest.python-profiling.memory.attach-enabled: false
```

A diagnostic job can independently enable startup capture:

```yaml
python.memory-profiling.startup.enabled: true
python.memory-profiling.startup.delay: 120 s
python.memory-profiling.startup.duration: 60 s
python.memory-profiling.startup.native: true
```

Use the existing Python configuration, dependency preparation, and managed environment propagation.
The TM passes the effective absolute result directory to the worker so archives/virtualenv working
directories do not change its meaning. No second memory-result-directory setting is introduced.
Tool paths, enablement, and directories are deployment/job configuration, not request parameters.
This version requires worker startup after enablement; it adds no dynamic configuration reload.

### Worker discovery

Reuse the proposed `GET /taskmanagers/:tm/python-workers` route:

```json
{"workers": [{"id": "<opaque-worker-handle>", "pid": 12345,
              "memoryProfilingAvailable": true,
              "operators": [{"jobId": "<job-id>", "operatorId": "<operator-id>",
                             "operatorName": "infer-images", "subtaskIndex": 0,
                             "attemptNumber": 0}]}]}
```

`id` identifies this worker process instance on this TM; `pid` is display metadata. A restarted worker
gets a new handle. Clients cannot submit arbitrary PIDs. Internal start tokens, Beam IDs, dependency
versions, and registration files are not public fields.

`operators` lists the currently associated Flink Python operators: job ID, operator ID, original
operator name, zero-based subtask index, and attempt number. A shared worker may list several
operators. Fused operators are reported as runtime operators, not individual UDFs. This is a discovery
snapshot; an empty list can mean registration is pending or failed, not that the process is idle.
These fields help users select a process. Capture still covers the whole worker and does not attribute
a shared process's CPU or memory exclusively to an individual operator.

`memoryProfilingAvailable` describes whether the worker has been prepared for on-demand memory
profiling, including the deployment gate and a successful Memray import in its actual environment.
It does not guarantee injection permissions or native-stack support. Preparation errors are logged;
they do not prevent ordinary worker startup. Startup capture in progress is a busy condition, not a
reason to silently attach a second Tracker. CPU remains independently available.

### REST capture requests and responses

Extend the existing `POST /taskmanagers/:tm/profiler`. JVM requests retain `mode` and `duration`.
New Python requests explicitly select `type`:

```json
{"workerId": "<opaque-worker-handle>", "type": "CPU", "duration": 30,
 "includeNative": false, "includeIdle": false}
```

```json
{"workerId": "<opaque-worker-handle>", "type": "MEMORY", "duration": 30,
 "includeNative": false}
```

| Field | Contract |
| --- | --- |
| `workerId` | Required for Python requests; a valid handle on this TM; mutually exclusive with JVM `mode`. |
| `type` | Required for Python requests: `CPU` or `MEMORY`. `MEMORY` here means on-demand attach. |
| `duration` | Required positive integer seconds, no greater than `rest.profiling.duration-max`. |
| `includeNative` | Optional boolean, default `false`, for either Python type. |
| `includeIdle` | Optional boolean, default `false`, for CPU only; its presence on a memory request is invalid. |

No arbitrary tool arguments, output paths, sampling rates, rendering formats, or allocator-tracing
options are added. Unsupported explicit options fail without fallback. JobManager profiling does not
accept Python worker requests. Startup capture is configured on the job, not submitted through REST.

Acceptance returns HTTP 200 with the existing `RUNNING`, `FINISHED`, or `FAILED` session model.
Keep `status`, `triggerTime`, `finishedTime`, `duration`, `message`, and `outputFile`. Python responses
add `worker`, `type`, and the applicable boolean options; omit JVM-only `mode` and `profilingMode`.
`outputFile` remains one generated filename: SVG for CPU or `.bin` for memory. Actual capture
timestamps and tool versions are recorded in the tool artifact/logs; requested duration is not a
promise of exact elapsed time.

`FINISHED` for memory requires confirmation that Tracker has stopped and closed the recording.
An attach CLI returning, a timer expiring, or a file being readable is insufficient. A valid empty
allocation window is not treated as CPU-style “zero samples” failure.

### History, downloads, and concurrency

Keep existing endpoints: `GET /taskmanagers/:tm/profiler` returns JVM history;
`?target=jvm` is equivalent; `?target=python` returns CPU and memory attach sessions, distinguished
by `type`. `GET /taskmanagers/:tm/profiler/:filename` serves either artifact through the existing
TM file upload and transient BLOB route. There is no new memory history or download service.

REST sessions inherit the current **Flink JVM-wide single-active-session limit**. Several TMs, or a
JM and TMs, in one JVM share the slot. A conventional deployment has one TM per JVM. History remains
grouped by resource ID, with a combined per-resource count before target filtering; Python sessions
can evict older JVM sessions. UI/409 messages must describe this actual scope.

Startup recordings are worker-initiated and do not acquire the REST session slot. They are not
automatically imported into REST history in the first release. Several workers may record
simultaneously. Logs provide the generated filename, PID, start/end time, and completion status;
the user can retrieve a completed recording from the TM directory or the existing download URL
using that filename. This explicitly avoids a new worker-to-TM session-reporting protocol.

The TM must not attach Memray while a startup recording on that worker is pending or active.
Reuse private worker registration metadata for these state transitions; no commands or heartbeat
service are introduced. A memory mode must not run concurrently with another Memray Tracker or
cProfile in the same worker. Administrators must not run a separate manual Memray session while
Flink controls that worker's memory capture; the CLI has no Flink session ownership token.

### Result directory and retention

Flink generates `python-<uuid>.svg`, `python-memory-<uuid>.bin`, or
`python-memory-startup-<uuid>.bin` under `rest.profiling.dir`. Write recordings to a temporary name
and publish the final filename only after confirmed closure. Do not delete/truncate a recording
whose writer may still be active.

REST history eviction cleans up its owned artifacts. Known-closed failed capture files are cleaned
up; stop-unconfirmed recordings remain explicitly incomplete. Startup output is not covered by
REST history retention: deployment operators own its archival/removal, including incomplete files
after crashes. Every worker restart may generate another file. This policy must be documented with
startup enablement; the feature is off by default.

Only owned artifacts are removed; never clear the whole directory. Existing BLOB caching remains:
eviction does not revoke downloaded/cached copies. History is not reconstructed after TM restart,
and persistent volumes preserve files rather than sessions. No remote-store or archive subsystem is
introduced.

Python downloads expose only final artifact names, not in-progress recordings.
Python artifacts download as attachments with `nosniff`; SVG additionally receives a restrictive CSP,
and `.bin` uses `application/octet-stream`. Existing JVM download behavior is preserved. Users can run
the matching Memray environment's `flamegraph`, `stats`, or `tree` commands offline. For native reports,
retain the matching interpreter/libraries and debug symbols. Automatic rendering is deferred so that
neither a renderer subprocess in the worker nor a second artifact schema is needed initially.

### Failure semantics and attach warning

| Condition | REST behavior |
| --- | --- |
| Invalid types, combinations, duration, or history target | HTTP 400. |
| Missing/expired TM or worker handle | HTTP 404. |
| Shared REST session slot occupied, or known conflicting memory capture | HTTP 409; no queue or overwrite. |
| Tool/worker dependency unavailable, platform unsupported, or memory attach disabled | HTTP 503; unrelated capabilities remain usable. |
| Accepted capture encounters tool, permission, worker-exit, timeout, or storage failure | `FAILED` with diagnostic detail and cleanup status. |

Startup configuration/dependency failures are logged and skip recording without changing ordinary
UDF startup. This is a control-layer policy, not isolation from failures inside native profiler code.

**Memray attach injects executable code into a running worker and can crash or deadlock it, potentially
causing job failure or recovery.** Memray's documentation recommends this debugging mechanism for
development environments. The administrator gate defaults to false. The Profiler page shows this
warning and requires an explicit confirmation before a memory attach request; REST documentation
states the same risk. No extra “acknowledge risk” request field is necessary.

Startup Tracker capture avoids debugger injection but still changes allocation behavior, consumes
resources, and depends on Python execution opportunities to stop. Neither mode guarantees a hard
deadline or a complete recording after OOM/SIGKILL. These limits remain visible in user documentation.
[Memray attach caveats][memray-attach]

### Web UI

Reuse the TM Profiler page: select JVM/Python, worker, CPU/Memory, duration, and applicable options.
Memory means attach in this page and requires the warning confirmation. Show capability-unavailable,
no-worker, discovery-error, and busy states distinctly. A stale selection is cleared, not silently
replaced. History labels CPU versus memory and offers “Download flamegraph” versus “Download memory
recording”. Creation responses refresh the currently selected history.

Startup recording is configured with the job and reports its filename in logs; the UI explains that
it does not appear as a REST-created session. No path input, separate memory page, or custom viewer
is added.

## Proposed Changes

### Reuse and ownership

| Existing module/path | Responsibility |
| --- | --- |
| Python dependency/environment preparation and Beam worker boot | Pass resolved configuration; retain Beam process lifecycle; register actual worker identity and memory preparation/startup status. |
| Existing REST → RM → TM RPC route | Validate and forward CPU/memory requests and history queries. |
| Existing `ProfilingService` | Own REST sessions, the JVM-wide admission slot, status transitions, history and cleanup. |
| Python profiling execution | Run py-spy or Memray attach/detach with fixed argv on background executors; manage tool subprocesses. |
| Existing `FileType.PROFILER` / BLOB path | Serve the generated SVG or binary recording from the effective result directory. |
| Worker startup hook | Own one direct-API Tracker window without UDF/bundle hooks or a separate control endpoint. |

No local command socket, new worker-control RPC, plugin registry, or generic profiler SPI is proposed.
CPU and memory use two direct implementations behind the existing profiling entry point.
`flink-runtime` does not depend on Beam implementation classes. Disabled memory profiling does not
import Memray; enabled preparation does not start tracking until the selected trigger occurs.

### Worker identity and registration cleanup

Beam remains responsible for worker creation, exit and restart. The TM needs the real SDK harness
PID, not a shell-wrapper PID. Reuse existing startup/process management and the prototype's small
private registration bridge where Beam cannot expose that identity directly.

Validate liveness, process start identity, and ownership at discovery, creation, before invoking a
tool, and before publishing results. Both external tools ultimately attach by PID; validation and
attach are not atomic, so the residual PID-reuse race must be documented.

The worker removes its registration on normal exit. The TM also removes confirmed-stale owned
records during existing discovery scans, covering SIGKILL and repeated restarts. Preserve live or
unrecognized files; an inspection/read failure is not proof that a process is dead. Cleanup failure
is logged without hiding valid workers. No new heartbeat or cleanup service is needed.

Operator association reuses Beam's existing worker ID. The runner reads the actual ID from its
stage's instruction request handler; Python boot already receives `WORKER_ID`. JobID travels through
the existing environment preparation path to prevent cross-job collisions. Each Java operator writes
its own association after opening and removes it on close. Discovery joins these records to existing
process registrations by JobID + Beam worker ID. Shared workers naturally collect several operators,
without another process pool, reference count, or ownership service. Updates happen only at operator
lifecycle boundaries, never per record or bundle. Association records are written to a temporary
file and published atomically. If writing or publishing fails with an I/O error, the registration
call attempts to remove its temporary file immediately, preserving the original error for caller
logging. Cleanup does not depend on a later operator close or replace the original error if it fails.
The Beam 2.75.0 integration point is
`FnApiControlClient.getWorkerId()`; only `flink-python` depends on that Beam class. This association
follows Flink's current stage/environment lifecycle; future Beam environment rotation or load
balancing would require revalidation.

### CPU lifecycle

Keep the current py-spy path with fixed 50 Hz and optional native/idle flags. Acceptance, execution,
history, cleanup and downloads reuse existing profiling infrastructure. Require a successful tool
exit, an artifact, and validated positive-sample completion evidence before `FINISHED`.
Tool-output parsing such as `Samples: N` is a dependency on the validated tool version, not a stable
generic protocol. Invalid/zero-sample results fail rather than falling back to cProfile.

The CPU backend measures elapsed time from tool launch. At the deadline it sends SIGINT to py-spy
to request sampling termination and result generation, then allows a bounded finalization interval
(10 seconds in the current POC). An unresponsive sampler is terminated/killed and fails; a forced
kill is not a valid result. Initialization consumes part of this window, so actual sampling can be
shorter than requested. Retain the tool's duration as an additional exit condition: in validated
py-spy 0.4.3 it counts sampling intervals, and slow native unwinding can exceed the nominal duration.


Cancel and await the owning TM's sampler during shutdown without closing a JVM-shared profiling
service used by another TM. Late worker discovery must not start a capture after shutdown begins.
Do not block TM RPC or record-processing threads.

### Direct-API startup memory lifecycle

The standalone boot hook prepares the optional Memray import and schedules one capture. A worker
thread owns the `Tracker` context from entry to exit. Delay and duration are measured around the
actual window, and the same worker continues processing afterward. On normal harness return or a
Python exception, cancel an unstarted recording or close the active Tracker once.

Use the existing environment/archives/requirements preparation. The result path comes from the TM's
resolved profiling directory. The hook logs effective options, tool version, PID, actual times and
filename. Each restart follows the same configuration and creates a fresh recording. No recording
is resumed or appended to a predecessor's file.

Only the startup hook confirms its own closure; a `.bin.inprogress` file becomes a completed `.bin`
after `Tracker.__exit__` finishes. Abnormal termination may leave an incomplete file. Startup
capture and attach use different activation paths, but share Memray's allocation semantics and
artifact format.

### On-demand memory lifecycle, following Ray

Ray's reporter agent runs `memray attach`, waits, runs `detach`, and then renders a report. Adopt the
tool integration pattern while keeping Flink's asynchronous session and BLOB path. Ray's manager
does not provide a cooperative worker-control protocol to reuse. Beam 2.75's Fn API likewise has no
Memray start/stop instruction; its bundle profiler factory does not implement these process windows.
[Ray agent][ray-agent] · [Ray manager][ray-manager] · [Beam Fn API][beam-fn-api]

For a REST memory request:

1. Validate the worker instance, optional-dependency preparation, attach gate and session admission.
   A known startup window or cProfile conflict rejects the request.
2. Generate a unique temporary recording path and launch the configured Memray CLI with fixed argv.
   Validate a target-side `--duration` safeguard in the supported Memray version. Recommend requested
   duration plus a small cleanup grace, so normal TM-controlled detach happens before this fallback.
3. Wait for the requested window on a profiling executor, then invoke detach. The same cleanup
   attempt belongs in failure/cancellation handling. Bound CLI waits and reap tool/debugger child
   processes; do not terminate the business worker as a profiler fallback.
4. Successful detach is the normal closure confirmation, under the documented exclusive-control
   assumption. Then validate/publish the recording and mark the existing session `FINISHED`.
5. If stop cannot be confirmed, mark `FAILED` with an explicit stop-unconfirmed message. Do not
   publish success, unlink a possibly active recording, or automatically attach again. Keep the
   active-session reservation until stop is confirmed or the worker exits. TM shutdown has a bounded
   diagnostic wait; worker process termination remains part of the existing task/Beam lifecycle.

The target-side timer is a fallback if the TM disappears, not proof of completion or a hard bound.
Memray 1.20.0 clears its “last tracker” reference before finishing Tracker closure; therefore
“no previous attach” from detach is not, by itself, a success acknowledgement. Ray's behavior of
returning a detach warning and still rendering is not sufficient for Flink's `FINISHED` contract.
Ending a CLI subprocess also does not prove that target-side tracking stopped. These cases must be
tested against the supported version before merging this integration. [Memray lifecycle][memray-code]

An HTTP client disconnect does not cancel an already accepted asynchronous Flink session. Do not
add generic retries or copy Ray's sudo/setuid fallback. Users must give Flink exclusive control of
Memray for the capture window; its CLI cannot distinguish an unrelated manual attach session.

### Tool compatibility and interpretation

Publish a tested matrix of Flink, CPython, tool version, Linux architecture and native mode. Maintainers
own the invocation/API, stop, failure and artifact contracts for those combinations; operators own
installation and deployment permissions. Tool upgrades require a repeatable real-tool smoke test
that fails if capture fails, plus lifecycle tests. The current exploratory smoke script's successful
job exit alone is not a compatibility pass. No version-adapter framework is required.

Ray provides py-spy through `ray[default]` and Memray through `ray[observability]`; its node managers
check tool availability. Adopt optional preparation, not the assumption that an agent's PATH proves
the target interpreter can import Memray. Record the actual tool versions and require a validated
CLI/worker Memray combination. [Ray dependencies][ray-dependencies]

CPU flame widths are stack-sample proportions, not exact CPU time. Native mode may miss native
thread pools without Python thread state and does not capture GPU kernels. Memory recordings describe
allocations in the capture window, not all RSS/PSS/container or GPU memory; pre-window allocations
cannot be attributed retrospectively. Native traces add C/C++ stacks; native allocations can exist
without them. Allocations still live at window end are not automatically leaks.

Use the deployment's existing REST access boundary and protect artifacts, which may reveal code
paths. This proposal does not establish new isolation between mutually untrusted jobs.

## Compatibility, Deprecation, and Migration Plan

All new modes are opt-in. Existing JVM request/response shapes, default JVM history, file routes and
`python.profile.enabled` behavior are preserved. That option continues to mean the existing cProfile
feature; it is neither renamed nor silently replaced. Concurrent cProfile/Memray operation in one
worker is not supported.

The Python interfaces and configuration in this draft become public commitments when released.
The four `PYFLINK_MEMRAY_*` variables in the local startup prototype are not the proposed public
interface or aliases; migrate that unpublished prototype to the formal configuration. The local
CPU request without `type` also has no released compatibility commitment.

Implementation can be reviewed in separate CPU, startup-memory, and attach-memory changes under this
one FLIP. Memory remains in scope; attach acceptance requires its warning, disabled default and real
lifecycle evidence. Existing worker processes need restart to pick up dependency preparation and
startup settings. No checkpoint/savepoint format or UDF signature changes are involved.

## Test Plan

| Area | Required evidence |
| --- | --- |
| Protocol/compatibility | Old JVM clients and generated REST schemas; Python CPU/Memory discrimination; invalid combinations; 400/404/409/503 and download MIME/headers. |
| Identity/lifecycle | Multiple workers/TMs; PID reuse; SIGKILL registration cleanup; preserve live/unrecognized files; remove temporary files after association publication failure while preserving unrelated files; TM shutdown, queued capture and late discovery. |
| Startup API | Disabled path without Memray; missing package; delay/normal return/exception; exactly-once closure; restart behavior; continued processing; effective result directory even with archives or disabled ambient-env inheritance. |
| Attach | Real success/permission/missing-dependency paths; injection and detach failures; fallback timer race; interrupted CLI/TM; normal worker exit with the target-side timer still pending; stop-unconfirmed state; no business-worker kill or false completion. |
| Ownership | Startup pending/active versus attach; repeated requests; JVM-wide REST admission; document and test the exclusive Memray-control assumption. |
| Artifacts | Real CPU SVG and readable closed Memray binary; zero allocations; incomplete files; unwritable/full output location; REST retention versus explicit startup retention policy. |
| Representative load | CPU PyTorch inference after warmup, with one thread and the default native pool; compare no capture, CPU capture and memory capture; report throughput and tail latency plus visible/missing stacks. |
| User workflow | One TM with two unfused operators and Python managed memory disabled; choose by operator/subtask/attempt and verify distinct PID/hotspot pairs. Enable managed memory as a shared-worker control and verify both associations plus cleanup. Exercise the attach warning and download. |

Native Memray capture can produce large files. Verify growth, overhead and the ability to stop;
file-size monitoring can request stop but cannot impose a hard size cap on an unresponsive in-process
Tracker without affecting the worker. Operational storage limits and this limitation must be documented.
Select modest diagnostic windows and workloads for the initial support matrix.

## Rejected Alternatives

| Alternative | Reason |
| --- | --- |
| Exclude memory solely because Memray is an optional external dependency | Optional preparation is a valid deployment contract; Flink still owns integration and compatibility. |
| Add a cooperative worker command socket/RPC for this release | Startup API capture and the explicit-risk attach path cover the chosen modes without another control protocol. |
| Reinterpret Beam bundle/status/metrics messages as profiler commands | Beam has no matching operation; coupling diagnostics to those semantics would add protocol debt. |
| Separate REST service, history, artifact store, scheduler or profiler SPI | Existing Flink profiling paths already supply these responsibilities for on-demand requests. |
| Treat the attach process exit or a readable binary as completion | Recording continues in the target; stop confirmation is required. |
| Automatically render every memory report or expose every Memray option | Preserve one native binary artifact and use Memray's existing offline tools first. |
| Central scheduling and live REST history for startup windows | Requires additional reporting/admission coordination. The first release explicitly uses log/file discovery and deployment-owned retention for this mode. |

## Open Questions

These decisions need evidence and community agreement before voting; they are not implemented claims.

| ID | Decision and proposed direction |
| --- | --- |
| D1 | Validate Memray CLI/worker closure and fallback-timer behavior, including debugger cancellation and its effect on normal worker exit. Adopt attach only with the stated risk gate and truthful stop-unconfirmed semantics. |
| D2 | Confirm first-release Linux/CPython/tool combinations, acceptable overhead and recording sizes using real PyTorch workloads. |
| D3 | Use a minimal operator association list to select among workers on one TM; validate separate workers, shared workers, and cleanup while retaining process-level reports. |
| D4 | Confirm the deliberate first-release tradeoff: startup files use the common directory/download route but do not enter REST history or its retention. |

## Appendix A: Current implementation and remaining work

The local changes build on `7b5af6de1e1` (upstream baseline
`8d01380ff5bf9892462ef9808b1b56fd9723f54d`) and implement the revised POC contracts.
This is not a community-approved or released feature.

| Implemented and validated | Remaining work |
| --- | --- |
| `CPU`/`MEMORY` request types, worker memory availability, formal configuration, JVM-wide session limit and stale registration reclamation. | Broader tool, Python and platform combinations. |
| Reused REST/RPC/history/download/UI; previous validation passed 73 runtime, 16 Python memory, 8 environment-manager and 37 frontend tests; REST snapshot and generated configuration documentation updated. | Full-repository `clean verify`. |
| Real Memray 1.20.0 native attach/detach capture, separate enable flag and UI acknowledgment, stop-unconfirmed reservation and incomplete-download rejection. | More complete native crash/deadlock, permission and debugger failure experiments. |
| Formally configured startup Tracker; inherited legacy environment inputs cannot bypass disabled configuration; shared directory and download endpoint. | Deployment combinations such as archives, and practical disk-capacity management. |
| Both memory modes on Linux AArch64, CPython 3.11.18 and Beam 2.75.0 produce readable allocation stacks and matching downloads while jobs remain running; earlier py-spy 0.4.3 CPU/native evidence is retained. | Broader models, architectures and deployments; longer comparisons on isolated hardware. |

Subsequent validation used a 6 GiB VM with two independent JVM TMs and one slot each. Image-buffer
and CPU ResNet-18 jobs both passed simultaneous two-worker CPU/native Memray captures, cross-TM
handle isolation, same-TM admission conflicts and matching downloads while workers remained alive.
Three rounds of ordinary CPU/memory comparisons were completed with native pools limited to one
and with the default six threads, preserving throughput, UDF latency and per-thread CPU evidence.
Five active native threads were absent from py-spy SVGs in the default-pool experiments; identifying
the PyTorch call chain does not establish complete native CPU coverage.

The experiment also exposed py-spy's nominal-duration overrun. The POC now uses timed SIGINT,
bounded finalization and forced termination only as a failure fallback. Seventy runtime tests pass,
and the real REST request that previously timed out now completes. The three-round overhead matrix
predates that fix; native completion was revalidated afterward without rerunning the full matrix.
Thread-limit calibration and the test environment's Metaspace failure are retained in the report.

See the [two-TM/PyTorch/overhead/stop experiments](../../../output/playwright/pyflink-20261107/torch-multiworker/run.md),
[memory integration report](../../../output/playwright/pyflink-20261107/formal-memory/run.md), and
[memory usage instructions](Memory-experiment.md). These are local short-window observations, not
production overhead guarantees. Native fault injection, more versions and full-repository checks remain.

A subsequent single-TM, one-slot run disabled `python.fn-execution.memory.managed` and Python/operator
chaining. Two unfused operators produced separate workers; independent UDF PID records matched their
operator/subtask/attempt metadata. Individual CPU captures contained the selected hotspot and excluded
the other; Memray recorded the selected PID and allocation stack. A managed-memory control shared one
worker, listed both operators and captured both hotspots. Cancellation removed all association records;
stale handles returned 404 and overlapping captures returned 409. See the
[single-TM evidence and screenshots](../../../output/playwright/pyflink-20261107/single-tm-workers/run.md).

A subsequent fix cleans up temporary association files when registration fails. A rename-failure
regression first reproduced the leftover file before the fix, then passed afterward. This revision
passed 29 worker-profiling tests and 4 REST integration tests, plus Spotless, Checkstyle and diff
checks. The standalone experiment and full-repository tests were not rerun for this cleanup fix.

## References

- [Tool selection research](Profiler-selection-research.md).
- [Distributed Python framework comparison](Distributed-framework-profiling-research.md).
- [Ray 2.60.0 / Memray 1.20.0 source review](Ray-profiling-research.md).
- [FLIP process and public-interface commitments][flip-process].

[flip-template]: https://cwiki.apache.org/confluence/spaces/FLINK/pages/65145553/FLIP+Template
[flip-process]: https://cwiki.apache.org/confluence/spaces/FLINK/pages/65145551/Flink+Improvement+Proposals
[flip-375]: https://cwiki.apache.org/confluence/spaces/FLINK/pages/272927211/FLIP-375%2BBuilt-in%2Bcross-platform%2Bpowerful%2Bjava%2Bprofiler
[memray-api]: https://bloomberg.github.io/memray/api.html
[memray-native]: https://bloomberg.github.io/memray/native_mode.html
[memray-attach]: https://github.com/bloomberg/memray/blob/v1.20.0/docs/attach.rst
[memray-code]: https://github.com/bloomberg/memray/blob/v1.20.0/src/memray/commands/attach.py
[ray-agent]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_agent.py#L641-L664
[ray-manager]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py
[ray-dependencies]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/setup.py#L258-L279
[beam-fn-api]: https://github.com/apache/beam/blob/v2.75.0/model/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1/beam_fn_api.proto#L152-L169
[pytorch-profiling]: https://docs.pytorch.org/tutorials/recipes/recipes/profiler_recipe.html
