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

# Python worker profiling: local integration

The revised design ([English](FLIP-draft.en.md), [中文](FLIP-draft.md)) reuses Flink's existing profiling service,
REST/RM/TM route, history, concurrency limit and BLOB download. The current prototype implements worker discovery, py-spy execution, startup Memray capture, and opt-in Memray attach/detach. This is a local
branch, not an approved/released FLIP.

The design was compared with the local VVR flamegraph implementation on 2026-10-24.
Optional native-stack capture and a Python target selector in the existing Profiler UI
are implemented and validated on Linux AArch64. A local CPU ResNet-18 overhead comparison is available; broader isolated-hardware measurements remain.
The FLIP summarizes prototype evidence and remaining verification in Appendix A. Detailed local
reproduction instructions, test counts, and raw experiment links remain in this README.

## Delivery scope

The revised [English FLIP](FLIP-draft.en.md) and [Chinese FLIP](FLIP-draft.md) include
CPU profiling and both memory modes in the formal proposal: startup Tracker capture
and separately enabled, explicitly warned Memray attach. The proposed integration
reuses existing Flink profiling paths and does not add a worker-control RPC or SPI.

The revised configuration, memory REST contract and UI are implemented locally. The earlier
`PYFLINK_MEMRAY_*` inputs are private transport details, not supported user configuration.

| Work item | Current implementation |
| --- | --- |
| CPU profiling | Existing REST/RPC/history/download/UI reused, with explicit `type: CPU`; ordinary/native capture validated on Linux AArch64 with py-spy 0.4.3. |
| Startup memory capture | Formal Python configuration, Memray Tracker window, shared result directory and download; real configured capture validated. |
| On-demand memory attach | Separate opt-in flag, worker preparation, UI risk acknowledgment, existing session/history/download path and confirmed-detach publication; real native attach validated with Memray 1.20.0. |
| Lifecycle | Stale registration reclamation, startup/attach conflict, bounded CLI/debugger cleanup, stop-unconfirmed reservation, and incomplete-file download rejection. |

The first-release proposal exports a Memray binary for offline flamegraph/stats/tree
analysis. Startup files share the TM directory/download route but do not enter REST
history or its retention; this is the proposed first-release policy in the FLIP body.

See the [tool selection study](Profiler-selection-research.md),
[framework comparison](Distributed-framework-profiling-research.md), and
[Ray source review](Ray-profiling-research.md) for the underlying research.

## Build and test

From the repository root with the configured JDK and Maven wrapper:

```sh
./mvnw -pl flink-runtime,flink-python -am -Dfast \
  -Dtest=PythonWorkerProfilingTest,PythonProfilingMarshallingTest,PythonProfilingRestITCase,ProfilingServiceTest,TaskManagerProfilingHandlerTest,TaskManagerProfilingListHandlerTest,ProcessPythonEnvironmentManagerTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
PYTHONPATH="$PWD/flink-python" python3 -m unittest discover \
  -s flink-python/dev/worker_profiling -p 'test_*.py' -v
```

`PythonProfilingRestITCase` starts a real MiniCluster and exercises worker discovery,
POST on the existing profiler endpoint, filtered history and the existing BLOB/file
download. Its sampler creates an explicitly labelled TEST FIXTURE SVG. It does not
prove OS-level sampling works.

The separate Python `service.py` is retained only as an isolated test prototype;
it is not loaded by Flink and does not define the production API. Its cleanup-failure
regression now verifies terminal publication, and cancellation/worker-exit tests
wait for a real sampler process before checking reaping and partial-file deletion.

## Real PyFlink / py-spy smoke test

Use a distribution built from **this branch**, matching PyFlink dependencies,
`FLINK_HOME`, `JAVA_HOME`, and an installed py-spy on PATH:

```sh
./mvnw -pl flink-dist -am clean package -DskipTests -Dfast -Pskip-webui-build
export PYTHONPATH="$PWD/flink-python:$PYTHONPATH"
python3 flink-python/dev/worker_profiling/smoke.py \
  --directory /tmp/pyflink-worker-profile-smoke
```

For native stacks, append `--native`; use `--idle` to include idle threads. The script
asserts the requested flags are echoed in the profile metadata, so a stale distribution
cannot silently ignore them. Clean the `flink-dist` module when rebuilding its shaded JAR:
an incremental re-shade can retain classes from an older distribution. For the UI, run
`npm ci && npm run build` in `flink-runtime-web/web-dashboard` before packaging runtime-web,
or build without `-Pskip-webui-build`.

The script launches a real process-mode job and discovers its worker through
**Flink REST**, submits a capture to `/taskmanagers/:id/profiler`, checks the UDF PID
and four output rows, and writes `result.json`. It downloads an SVG only when Flink
reports FINISHED. A failed profiling session makes the script fail.

The original pre-integration evidence in `validation.json` used a separate local
controller and existing 2.3 Java jars; it must not be mistaken for a current-master
Flink REST smoke test. New integration results are recorded separately below.

## Cluster configuration and API

```yaml
rest.profiling.enabled: true
rest.python-profiling.cpu.executable: /absolute/path/to/py-spy
rest.profiling.dir: /flink/log/profiling
```

Registration uses the TaskManager's private working directory automatically, even
when Python system-environment inheritance is disabled. No manual registration
path, new runtime Environment API or operator-owner lease is required.

```text
GET  /taskmanagers/:tm/python-workers
POST /taskmanagers/:tm/profiler
     {"workerId": "<returned id>", "type": "CPU", "duration": 10, "includeIdle": false, "includeNative": false}
GET  /taskmanagers/:tm/profiler?target=python
GET  /taskmanagers/:tm/profiler/:filename
```

`id` is an opaque process-instance handle; `pid` is display metadata.
`memoryProfilingAvailable` indicates whether the worker was prepared for attach.
Memory requests use `type: MEMORY`, omit `includeIdle`, and download a Memray binary.
See [memory deployment and API instructions](Memory-experiment.md). Beam IDs and
start tokens remain internal.
JVM requests keep `{mode, duration}`. Python requests and responses omit JVM modes.
Default history queries return JVM sessions; `target=python` selects Python sessions. Both use the
existing RUNNING/FINISHED/FAILED states, history settings and active-session limit.
Capture rate is fixed at 50 Hz. The TaskManager Profiler tab now selects JVM or Python,
lists workers, and offers native/idle checkboxes. A sole worker is selected on entering Python
mode; multiple workers require a choice. An exited selection is cleared, never silently
replaced with another process. History shows the target PID and requested sampling mode.
Worker entries include an `operators` list with job ID, operator ID/name, subtask index and attempt
number. The selector shows operator names and attempts alongside the PID. Java operator open/close
updates association records; discovery joins them to Python's existing Beam worker ID, scoped by
JobID. Shared workers list all associated operators; reports still describe the whole process.

## Evidence and limitations

Latest follow-up validation (2026-11-08):

- Worker association publication failures clean up their temporary files using the existing Flink
  helper while preserving the original error. A rename-failure regression failed before the fix;
  all 29 worker-profiling tests and 4 REST integration tests then passed.
- CPU configuration was renamed to `rest.python-profiling.cpu.executable`, symmetric with memory.
  Two configuration tests, four REST integration tests using the new name, and one generated-doc
  completeness test passed. Generated configuration docs were refreshed; style checks passed.
- These follow-ups did not rerun the standalone experiments or full-repository `clean verify`.

Earlier integrated validation (2026-11-07):

- One TM/one slot with Python managed memory disabled and two unfused operators produces two
  workers. Operator metadata matches independent UDF PID records; targeted CPU captures exclude
  the other operator's hotspot, and Memray records the selected PID/allocation stack. With managed
  memory enabled, one worker lists both operators and captures both hotspots. Cancellation removes
  association records, expired handles return 404, and overlapping requests return 409.
  See the [single-TM report and screenshots](../../../output/playwright/pyflink-20261107/single-tm-workers/run.md).

- 73 targeted runtime tests, 16 Python memory tests, 8 Python environment-manager tests and
  37 frontend tests pass. The REST stability snapshot, generated config/REST documentation,
  Java format/Checkstyle and changed-file Python/frontend lint checks pass.
- The native Memray attach and formally configured startup capture finish on the dedicated
  standalone cluster. Both produce readable allocation records; downloaded bytes match local
  files, known allocation stacks appear, and the jobs remain running after capture.
- Inherited legacy environment inputs do not bypass the disabled startup option. Incomplete-file
  download tests cover both JM/TM endpoints, encoded trailing slashes and case variants.
- A subsequent 6 GiB VM run used two TMs and two workers. Both image-buffer and ResNet-18 jobs
  passed simultaneous captures, cross-TM isolation, same-TM conflicts and matching downloads.
- Three-round ResNet-18 comparisons with native pools limited to one and default six threads
  record throughput, UDF latency and native-thread visibility. The native CPU timeout found by
  this experiment was fixed with timed SIGINT and bounded finalization, then verified via REST.
  See the [experiment report](../../../output/playwright/pyflink-20261107/torch-multiworker/run.md)
  for raw evidence, calibration, environmental failures and the distinction between overhead
  measurements before the stop fix and its later completion test.

See the [formal-memory report](../../../output/playwright/pyflink-20261107/formal-memory/run.md).
The following CPU and startup baseline checks were completed before this revision:


- 69 runtime tests pass, including the TaskExecutor suite, real REST/RPC/download integration, 400/404/409/503,
  typed request validation, internal identity isolation, zero samples, output-directory failures,
  history eviction and preservation of unrelated files.
- TaskManager shutdown now cancels and waits for its own Python capture, including queued captures.
  Another TaskManager's capture and JVM profiling remain unaffected. Worker lookup completion during
  shutdown cannot launch a new capture. Late creation responses refresh the currently selected target.
- 37 frontend tests pass; the production build and affected-file lint checks pass.
- The generated REST stability snapshot passes. An audit against upstream baseline
  `8d01380ff5bf9892462ef9808b1b56fd9723f54d` retains all 83 existing call contracts.
- Standalone JobManager and TaskManager on Linux AArch64 run a real PyFlink worker. Ordinary
  and native captures finish, contain the known hotspots, and download identically to TM-local files.
  The existing JVM ITIMER capture also finishes, with separated JVM/Python histories.
- The browser workflow is exercised and screenshots are saved. Visual review found and fixed
  compressed Target/Mode columns in the profiler table.
- py-spy 0.4.1's AArch64 wheel rejects native sampling; Flink reports FAILED without fallback.
  The 0.4.3 wheel succeeds. Permission changes are limited to SYS_PTRACE on the dedicated test container.
- This is a functional synthetic-workload validation using PyFlink's slow Python path with Beam.
  It is not a Torch/YOLO benchmark, a Cython-path certification, or a production overhead guarantee.
  Full-repository `clean verify`, a wider support matrix, and controlled overhead measurements remain.

See the [standalone report](../../../output/playwright/pyflink-20261107/run.md),
[review-fix validation](../../../output/playwright/pyflink-20261107/fixes/run.md), and
[memory capture report](../../../output/playwright/pyflink-20261107/memory/run.md).

### Earlier prototype checks

The following records predate this revision. Linux gaps mentioned here are historical;
current results and remaining limitations are summarized above.

- 15 Python prototype tests pass, including the cleanup-failure red/green regression.
- 29 profiling runtime tests, 8 Python environment-manager tests and 1 REST API
  stability test pass. Coverage includes native/idle forwarding, no fallback for unsupported
  native requests, multiple workers, stale handles, independent output files, timeout/reaping,
  cleanup failures, queued cancellation and JVM compatibility. Sampler fixtures are labelled.
- 34 frontend tests pass, including worker selection, preserving a live selection, clearing
  an exited selection, discovery errors, and rendered native checkbox/button interactions.
  The production Angular build and affected-file ESLint check pass.
- A distribution built from this branch (2.4-SNAPSHOT) ran the actual PyFlink REST
  smoke test; [recorded result](integration-validation.json) matches all four UDF
  rows to worker PID 44753, distinct from client PID 44732. The worker exited cleanly.
- The earlier Python-only request returned FAILED with py-spy's macOS root requirement.
- On 2026-10-24, the clean rebuilt distribution ran a real PyFlink job with `--native`.
  The [native request evidence](native-integration-validation.json) confirms includeNative=true
  in the result, an explicit unsupported-platform error from the actual py-spy binary,
  all four correct UDF results, matching worker PID 83633 and clean worker exit.
  No real native flamegraph or sampling-overhead result has been claimed.
- `flink-runtime`, `flink-runtime-web`, `flink-dist` and `flink-docs` build successfully.
  The distribution was rebuilt from a clean module target and its actual ProfilingInfo class
  checked for includeNative. Configuration, REST HTML, OpenAPI and the REST stability snapshot
  were regenerated with the project generators.
- Spotless, Checkstyle, Python flake8 and `git diff --check` pass for the affected code.
  Validation is targeted; a full-repository `clean verify` has not been run.
- Linux still requires appropriate ptrace permissions; same UID is not sufficient
  in every deployment. No system settings, sudo or profiler fallback are used.
- The target is the whole worker process, potentially shared by multiple operators.
- PID/start-token checks do not make PID-only attach atomic. macOS start tokens
  have second precision. These limitations must be addressed before release.
- Size limits are checked periodically; nonempty SVG validation does not yet prove
  that useful stack samples were captured. Linux sampling/overhead validation remains.

## Startup memory capture

The separate [memory experiment](Memory-experiment.md) now provides optional Memray
capture in the standalone worker startup path, with delay, duration, native traces,
and a local output directory. It closes the Tracker while the same worker continues
processing. See the [memory smoke job](memory_smoke.py) for standalone validation.
