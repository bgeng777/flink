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
REST/RM/TM route, history, concurrency limit and BLOB download. The additional
runtime responsibilities are worker discovery and py-spy execution. This is a local
branch, not an approved/released FLIP.

The design was compared with the local VVR flamegraph implementation on 2026-10-24.
Optional native-stack capture and a Python target selector in the existing Profiler UI
are implemented and validated on Linux AArch64. Controlled sampling-overhead measurements remain.
The design records the reference baseline and adoption decisions in Appendix B.

## Delivery scope

The [FLIP-format draft](FLIP-draft.en.md) separates the proposed CPU feature from
the memory experiment in Appendix A. Its Open Questions section lists four
scope and interface decisions to discuss before finalizing the proposal. The
proposed community deliverable is CPU profiling; memory remains a separate local
experiment. The CPU backend is still under review in the
[cProfile/py-spy study](Profiler-selection-research.md);
the implementation status below refers to the existing py-spy prototype.

The revised public contracts are implemented: worker descriptors contain only `id` and
`pid`, default history is JVM-only, and `target=python` selects Python history. Python
responses omit JVM-only mode fields. Explicit error and result-directory lifecycle
contracts are covered by tests. See the [2026-11-07 standalone validation](../../../output/playwright/pyflink-20261107/run.md)
for real Linux captures, environment details, screenshots, and remaining limits.

| Work item | Status and acceptance |
| --- | --- |
| CPU flamegraphs with py-spy | Implemented and validated on Linux AArch64, CPython 3.11.18 and py-spy 0.4.3. Ordinary/native captures identify Python preprocessing and zlib deflate. Broader platform and overhead validation remains. |
| Memory capture at worker startup | Implemented with optional Memray 1.20.0. Immediate and delayed/native captures identify image-buffer and zlib allocations on Linux; the same worker continues processing after closure. See [experiment details](Memory-experiment.md). |

The memory experiment uses Memray's public Tracker API in the worker and retains
its binary capture for offline rendering. It requires starting the worker and
initially targets a selected small diagnostic job. It does not add a memory REST
endpoint, UI, artifact service or shared admission protocol to Flink.

Use existing deployment monitoring and worker IDs/PIDs during validation. Process
metrics, task associations, thread dumps, dynamic memory triggering and recovery
of interrupted captures are outside these deliverables. They do not block CPU
submission for community review.

The [Ray/Memray source study](Ray-profiling-research.md) remains reference material.
The [distributed framework comparison](Distributed-framework-profiling-research.md)
adds Spark, Dask, Beam/Dataflow, PyTorch and Celery. In-process collection can use
an existing worker control or execution path; the first memory experiment still
uses a startup window.
The [external-attach sequence](memory-lifecycle.png) is an earlier alternative,
not the current implementation plan. Validation results below apply only to the
existing py-spy implementation; memory evidence is recorded in the separate experiment report.

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
reports FINISHED. Read the profiling status/message: exit 0 means the experiment
and job completed, not necessarily that sampling succeeded.

The original pre-integration evidence in `validation.json` used a separate local
controller and existing 2.3 Java jars; it must not be mistaken for a current-master
Flink REST smoke test. New integration results are recorded separately below.

## Cluster configuration and API

```yaml
rest.profiling.enabled: true
rest.python-profiling.executable: /absolute/path/to/py-spy
rest.profiling.dir: /flink/log/profiling
```

Registration uses the TaskManager's private working directory automatically, even
when Python system-environment inheritance is disabled. No manual registration
path, new runtime Environment API or operator-owner lease is required.

```text
GET  /taskmanagers/:tm/python-workers
POST /taskmanagers/:tm/profiler
     {"workerId": "<returned id>", "duration": 10, "includeIdle": false, "includeNative": false}
GET  /taskmanagers/:tm/profiler?target=python
GET  /taskmanagers/:tm/profiler/:filename
```

`id` is an opaque process-instance handle; `pid` is display metadata. Beam IDs and
start tokens remain internal.
JVM requests keep `{mode, duration}`. Python requests and responses omit JVM modes.
Default history queries return JVM sessions; `target=python` selects Python sessions. Both use the
existing RUNNING/FINISHED/FAILED states, history settings and active-session limit.
Capture rate is fixed at 50 Hz. The TaskManager Profiler tab now selects JVM or Python,
lists workers, and offers native/idle checkboxes. A sole worker is selected on entering Python
mode; multiple workers require a choice. An exited selection is cleared, never silently
replaced with another process. History shows the target PID and requested sampling mode.
Full operator ownership remains deferred.

## Evidence and limitations

Latest validation (2026-11-07):

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
