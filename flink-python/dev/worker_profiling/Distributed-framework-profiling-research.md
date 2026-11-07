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

# 分布式 Python 框架的 profiling 接入对照

核对日期 2026-11-07。采用官方文档、固定版本源码及官方 PR，未运行这些框架的集群或 profiling 实验。
核对版本为 Spark 4.3.0、Ray 2.60.0、Dask/distributed 2026.8.0、Beam 2.75.0、PyTorch 2.15.0、Celery 5.7.0。
具体版本、入口、代码及限制见各框架章节。

## 主要结论

各框架没有统一选用某个 profiler。值得借鉴的是复用已有目标定位、执行配置、控制通道与产物交付。
“进程内采集”也不意味着每次必须重启或改启动器：Dask 的 Memray 和 Spark 的函数包装都支持在既有运行期接入点激活。

| 框架 | CPU 路径 | 内存路径 | 触发与归因 |
| --- | --- | --- | --- |
| PySpark 4.3.0 | cProfile，perf 结果可经 flameprof 渲染 SVG | memory_profiler 行级 RSS 变化，不是 allocator 事件日志 | 框架包装 RDD/UDF；session 配置影响后续执行，按执行标识汇总。 |
| Ray 2.60.0 | Dashboard 调节点侧 py-spy record/dump | Dashboard 调 Memray attach/detach/render | task/actor 定位 PID，图覆盖整个 worker。 |
| Dask 2026.8.0 | 默认 worker 内统计栈采样，Dashboard/Client.profile 查历史 | MemorySampler 看 RSS；Memray helper 看分配 | 复用任务线程映射和 Client.run，运行中直接创建/关闭 Tracker。 |
| Beam 2.75.0 | SDK 可按 bundle 开 cProfile | 旧路径 Heapy；新容器 profiler_agent 支持 Memray/TCMalloc | bundle/worker 两种范围；新启动器限时停止会重启 worker。 |
| PyTorch Distributed 2.15.0 | torch.profiler 观察算子、设备活动与通信 | 显式 profile_memory 观察张量分配 | 常规 context/schedule；experimental debug 服务也可按 rank 运行期触发。 |
| Celery 5.7.0 | 原生线程栈、task 运行信息；本次未发现统一 CPU 火焰图后端 | inspect 的 RSS、objgraph | 复用控制通道，注意 prefork 主进程与 task 子进程范围。 |

上表的依据分别在下述固定源码核对中。Dataflow 的 Cloud Profiler 是额外的托管路径，不与 Beam Memray 启动器混同。

## 对当前 Flink 草稿的修订建议

1. 保留进程外采样与进程内采集两条路径。触发方式另分为外部 attach、worker 已有控制/执行入口、启动配置。
   Memray debugger attach 是注入后进程内采集，不是 py-spy 式外部观察。
2. 进程内共通点定义为准备条件、目标、采集窗口、关闭确认、产物，而不要将共同机制永久绑定到启动器。
   当前首版仍可只实现启动窗口，后续若找到合适既有管理链路，再提供固定诊断命令。
3. Dask 是 Memray 合作式运行期启停最直接的参照，但其 Client.run 是既有任意 callable 能力。
   Flink 不应为了复用这一思路新增任意代码执行 RPC 或另一套 HTTP 服务。
4. Beam 的 profiler_agent 是启动包装的直接先例，但其 Go 容器 boot 与 PyFlink 自有启动路径不同。
   不能直接套用其选项；特别不能照搬通过终止/重启 worker 来停止采集的策略。
5. 保留三种数据的语义区别：任务阶段耗时、进程 RSS、调用/分配栈。报告能关联 task/operator 不代表资源就能精确归因。
6. 这些是架构参照，不改变当前 CPU 优先验证 py-spy、内存先完成一次有界 Tracker 实验的交付范围。

## PySpark profiling 核对

日期为 2026-11-07。官网当前正式版为 Spark **4.3.0**（2026-10-30 发布），固定源码 tag 为 `v4.3.0`，
commit `98bc0212c94705434471627068f41e374ccab143`。以前研究的 4.0.1 结论保留为历史证据，本次以 4.3.0 核对。
未运行 Spark 集群或 profiling 实验。[官方发布信息][sp-release] · [tag][sp-tag]

### 结论

PySpark 的主要内置 Python profiling 是**框架内置、worker 进程内、围绕执行函数启停**。
性能后端是 cProfile，内存后端是 memory_profiler，结果通过 accumulator 汇总到 driver/client，
按 RDD/UDF（新版本也支持 Python Data Source）标识查看，而非选一个 PID attach 后采整个进程。
这表明“进程内采集”不必等于“每次都要更换启动器或重启 worker”，框架可以预先内置合作式执行入口。

### 三条已有入口

| 入口 | 开关与采集 | 归因和结果 |
| --- | --- | --- |
| 传统 RDD/Python worker profiling | `spark.python.profile=true`；SparkContext 创建 collector，RDD 构建时附带 profiler，worker 执行 `profiler.profile(run_process)`。默认 BasicProfiler 使用 `cProfile.Profile().runcall(...)`。 | 各 partition 的 pstats 通过 accumulator 相加，按 RDD ID 聚合，`sc.show_profiles()` / `sc.dump_profiles(path)`；默认退出前打印，配置 dump 时写文件。 |
| Session UDF profiling（4.0 起） | 运行时 SQL 配置 `spark.sql.pyspark.udf.profiler=perf` 或 `memory`，框架在 worker 内包装 UDF 调用。 | 以 UDF result ID 汇总；`spark.profile.show/dump/render/clear`，UDF ID 可对应查询计划。 |
| Python Data Source profiling（4.2 起） | `spark.sql.pyspark.dataSource.profiler=perf` 或 `memory`，worker 检测 data-source worker 函数并复用相同包装逻辑。 | 复用结果 collector、显示和渲染，结果标识允许字符串。 |

传统路径见 [SparkContext 配置][sp-context]、[RDD 发送 profiler][sp-rdd]、[BasicProfiler][sp-basic]、
[worker 执行位置][sp-worker-run]；Session 和 Data Source 见 [SQLConf][sp-sqlconf]、[worker 包装][sp-worker-wrap]。

### Session CPU 与内存的实际实现

- `WorkerPerfProfiler` 持有 cProfile；进入 context manager 时 enable，退出时 disable，再把 pstats 存入 profiler accumulator。
  多次调用、多个 Python execution 的结果按 result ID 汇总，并非一份连续的进程栈时间线。
- `WorkerMemoryProfiler` 持有 `UDFLineProfilerV2`，包装目标函数/code，进入时 enable_by_count、退出时 disable_by_count，
  保存 code-map 行级内存统计。它继承 memory_profiler.LineProfiler，默认 backend 为 psutil。
  上游 psutil backend 测量 RSS，采用 Python trace 事件测量行级变化；**这不是 Memray 的 allocator 分配/释放日志**。
- UDF 包装发生在框架的执行代码中，用户不必在每个业务函数上手工添加 profiler 装饰器。
- `spark.conf.set` 可以在已有 session 中设置，影响后续发送到 worker 的执行配置；不需要为这个开关专门重新启动 worker。
  它也不是向当前正运行的 UDF/worker 发送任意时刻开始、结束的 attach 命令。

[WorkerPerfProfiler / WorkerMemoryProfiler][sp-session-workers] · [memory_profiler 扩展][sp-memory-ext] ·
[memory_profiler 测量实现][sp-memory-upstream] · [官方调用示例][sp-debugging]

### 返回、合并与可视化

性能结果是 pstats.Stats，使用 `PStatsParam.addInPlace` 相加。内存结果是 filename/line 对应的 code map，
使用独立合并逻辑；Spark 4.2 起新增 dictionary 形式的 V2 profile accumulator 通道，以便扩展并保持 Spark Connect 兼容。
不能把合并后的 UDF 内存数字解释为整个集群同一时刻的 RSS。[结果结构与合并][sp-session-results] · [基础合并][sp-basic]

`Profile.show` 明确说明结果来自所有 Python executions；展示按 UDF ID，不是进程 PID。
默认 `perf` renderer 是 flameprof，将 pstats.stats 渲染为 SVG；在 IPython 环境包装成 HTML，其他环境返回 SVG 字符串。
`memory` 没有默认 renderer，允许提供自定义 renderer；也可输出行号、内存量、增量和次数的文本。
`dump` 可分别落盘 pstats 与内存文本。[用户入口][sp-profile-api] · [渲染实现][sp-renderer]

这是**进程内采集 + 框架传播执行配置 + 既有结果回传/合并 + client 可视化**的完整先例。
不需要在每个 worker 上建立一个独立 profiling HTTP 服务，也不要求给 cProfile 授予 ptrace 权限。

### 依赖和限制

- cProfile 属于标准库。内存 profiling 需要集群 Python 环境安装 memory_profiler；flameprof 是渲染端额外依赖。
  4.3.0 的 worker memory wrapper 在找不到 memory_profiler 时返回原函数，collector 在没有依赖且没有结果时发出 warning。
  不能把配置为 memory 自动理解成一定采到了数据。[worker wrapper][sp-worker-wrap] · [collector][sp-session-collector] · [renderer][sp-renderer]
- 常见内置路径观察的是被包装的 Python 函数或 worker task 执行，并非自动记录模型加载前后整个进程生命周期、原生线程池全栈、分配事件或 GPU kernel。
- `perf` 与 `memory` 是 SQL 配置中的互斥选项；旧 UDF 配置同时启用 CPU/memory 会显式拒绝，源码理由是两种跟踪互相干扰。
  [SQL 配置][sp-sqlconf] · [旧 UDF 配置校验][sp-old-udf]
- 文档和源码存在范围细节差异。4.3.0 Debugging 页面仍写 generator UDF 不支持，但固定源码有若干 iterator eval_type 的 `next(iterator)` 包装路径。
  本次只能确认这些代码路径存在，没有实测所有 generator/iterator 组合，因此不将“所有 iterator 均不支持”或“所有 generator 均支持”作为结论。
  [文档][sp-debugging] · [iterator 路径][sp-worker-wrap]
- 在本次核对的官方文档、上述固定源码，以及 GitHub 官方 Spark 仓库对 `py-spy`、`memray`、`scalene` 的 code search（各返回零项）中，未找到内置这些工具的后端。
  这不排除用户自行在集群中部署外部工具，也不应泛化成“Spark 不允许 attach”。

### 对 Flink 的启示

1. 分类应以“采集逻辑在进程内还是进程外”为主，启动准备是单独维度。Spark 的 worker 已有执行期接入点，运行时配置可以激活后续任务的采集。
2. 值得复用的核心不是某个 profiler 名字，而是执行配置下发、函数/任务标识关联、结果回传与渲染分离。
3. Spark 的 UDF 级归因来自实际包装的执行范围。Flink 若用 py-spy 采整个共享 worker，不能仅借用一个 UDF/operator ID 就宣称精确算子归因。
4. Spark 的 `profiler_cls` 是传统 SparkContext 级扩展点；存在扩展接口不代表 Flink 首版需要同样公开一个通用 profiler SPI。

[sp-release]: https://spark.apache.org/downloads.html
[sp-tag]: https://api.github.com/repos/apache/spark/git/ref/tags/v4.3.0
[sp-context]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/core/context.py#L392-L402
[sp-rdd]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/core/rdd.py#L5319-L5342
[sp-basic]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/profiler.py#L164-L272
[sp-worker-run]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/worker.py#L4324-L4325
[sp-sqlconf]: https://github.com/apache/spark/blob/v4.3.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5041-L5061
[sp-worker-wrap]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/worker.py#L519-L616
[sp-session-workers]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/sql/profiler.py#L128-L225
[sp-memory-ext]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/memory_profiler_ext.py#L175-L184
[sp-memory-upstream]: https://github.com/pythonprofilers/memory_profiler/blob/master/memory_profiler.py#L116-L145
[sp-debugging]: https://spark.apache.org/docs/4.3.0/api/python/development/debugging.html
[sp-session-results]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/sql/profiler.py#L57-L125
[sp-profile-api]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/sql/profiler.py#L473-L653
[sp-renderer]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/sql/profiler.py#L665-L698
[sp-session-collector]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/sql/profiler.py#L284-L320
[sp-old-udf]: https://github.com/apache/spark/blob/v4.3.0/python/pyspark/sql/udf.py#L582-L611

## Dask/distributed profiling 核对

核对日期 2026-11-07。GitHub 最新正式 release 为 `2026.8.0`，发布于 2026-08-24，tag 解析到
commit `0a341df4782bc358a074fb12fe7ce277fe2313ce`。以下按此固定版本源码核对，未运行 Dask 集群。
[Release][da-release] · [Tag object][da-tag]

最重要的发现是，Dask 的 Memray 集成可以通过已有 worker 管理通道，在已经运行的 worker 内开启 Tracker。
它既不是 debugger 注入，也不要求为 profiling 重启 worker。这说明“进程内采集”不等于“只能启动时接入”。
[Memray helper][da-memray]

### CPU 与执行过程观察

| 能力 | 实际实现 | 用户入口与产物 |
| --- | --- | --- |
| Task stream | 框架记录任务执行、序列化、依赖获取、磁盘读取等阶段的开始/结束时间 | Dashboard task stream；不是函数调用 profiler。 |
| Worker statistical profile | 默认开启，名义间隔 10 ms 在 worker 内读取 `sys._current_frames()`，只选 `active_threads` 中执行任务的线程，计数调用栈 | Worker/Scheduler `/profile` 页面与 `Client.profile()`。 |
| 历史与关联 | 每 1 s 将当前样本归入历史、清空当前计数；按 `key_split(task key)` 分组 | 可以按 task key prefix、worker、时间窗口过滤，合并多个 worker 或分别查看。 |
| Low-level stacks | 默认关闭；打开 `distributed.worker.profile.low-level` 后调用可选 `stacktrace.get_thread_stack(tid, show_python=False)`，并入已有样本 | 配置依赖 `numba/stacktrace`；这里仅核实代码路径，不宣称当前所有 Python/平台都验证可用。 |
| performance_report | context manager 记录时间边界，结束时向 scheduler 请求报告 | 将任务流、worker profiles、带宽等已有信息组合成 HTML，不是临时启动另一个 profiler。 |

配置见 [distributed.yaml][da-config]；默认采样与任务关联见 [worker.py][da-worker-profile]；
查询入口见 [Client.profile][da-client-profile]；低层栈见 [profile.py][da-native]；报告见 [performance_report][da-report]。

因此 Dask 默认 CPU profile 是**进程内统计采样**，不使用 cProfile 逐调用跟踪，也不依赖 py-spy 外部 attach。
`Client.profile()` 主要查询已经积累的样本，不能理解为每调用一次就开启一次新的捕获。
样本由 worker event loop 的 PeriodicCallback 触发，仍依赖 worker/解释器能执行管理逻辑；
它不是独立于目标 GIL 的外部观察。图形计数也不是操作系统 CPU 周期计费。
[调度注册][da-worker-timers] · [采样路径][da-worker-profile]

官方性能页将 cProfile 描述为单线程分析工具，这里不将其泛化成“cProfile 从来不能做分布式分析”；
Dask 自己选择了适合其多线程 worker 和既有任务信息的统计采样实现。
[官方说明][da-perf-doc]

### 内存用量与内存分配是不同路径

`MemorySampler` 默认每 0.5 s 采集 `scheduler.memory.process`，形成集群级时间序列，支持多个运行窗口对齐比较。
`process` 是各 worker 进程内存的 RSS 口径；还可选择 MemoryState 中其他指标。它不会生成逐分配调用栈。
[MemorySampler 实现][da-memory-sampler] · [process/RSS 定义][da-memory-doc]

`managed` 是 Dask 通过 `sizeof` 估计的受管理数据占用，与 RSS 有区别；unmanaged 中可能混有
临时数据、解释器、allocator 保留及碎片等，不能把全部 unmanaged 都称为泄漏。
[Worker memory 文档][da-memory-doc]

需要分配栈时，Dask 另有 `distributed.diagnostics.memray`。

### Memray 的实际调用链

用户入口是 client 端 context manager：

```python
from distributed.diagnostics.memray import memray_workers

future = client.submit(my_function)
with memray_workers(directory="memray-profiles"):
    # 在此窗口观察已经运行的 worker
    future.result()
```

源码明确给出计算已经开始后进入该 context 的例子，也提供 `memray_scheduler()` 采集 scheduler 进程。
Memray 是可选依赖，目标环境必须可导入，生成 HTML 时目标侧还要能执行 memray CLI。
[API 与示例][da-memray]

| 阶段 | 2026.8.0 的行为 |
| --- | --- |
| 选择目标 | 查询 scheduler 的已有 worker 地址、名字和 ID。结果按 worker 保存，不声称只覆盖某个 task。 |
| 开始 | `client.run(_start_memray, ...)` 经 scheduler broadcast 的既有 `run` 通道，在 worker 内执行 Python 函数。 |
| 进程内生命周期 | `_start_memray` 将 `ExitStack` 存在 `dask_worker._memray`，进入 `memray.Tracker`，文件位于 worker 的 local_directory。 |
| 采集窗口 | client context 的 `yield` 期间 Tracker 持续工作；可将 `native_traces` 等参数传给 Tracker。 |
| 结束 | context `finally` 再执行 `_fetch_memray_profile`，关闭 ExitStack/Tracker，然后渲染或读取原始文件。 |
| 渲染位置 | 在原 worker 机器和 Python 环境生成报告，尤其为了 native 符号解析。默认命令参数为 `flamegraph --temporal --leaks`。 |
| 返回 | 通过既有 RPC 返回文件 bytes，保存到 client 本地目录；可以选择 `report_args=False` 下载原始 `.memray`，或按批并行取回 HTML。 |

[开始/关闭/报告代码][da-memray] · [Client.run 的广播实现与语义][da-client-run]

`Client.run` 是 Dask 早已存在的管理能力，在任务调度之外执行函数，并自动注入名为 `dask_worker` 的参数。
因此这套 Memray 集成没有新开 profiler RPC 服务，也无需修改用户函数。
不过，“没有修改启动命令”不等于纯进程外采集：Tracker 及 allocator hooks 仍在业务 worker 内运行。

### 对 Flink 共通机制的启发

1. 工具部署、控制通道、采集执行位置、是否需要重启是四个不同问题。
   Dask 展示了“预装依赖 + 既有管理通道 + 运行中进程内 Tracker”的组合。
2. 进程内公共机制应先定义窄的采集生命周期和结果约定，触发时机可以是启动窗口，也可以在具备控制通道后是运行中请求。
   不应因为当前 Flink 首个原型采用启动器，就将 Memray 固定为永远需要重启。
3. Dask 的 `Client.run` 支持任意 callable，不意味着 Flink 应为 profiling 新增任意代码执行接口。
   借鉴其复用管理通道的思路，Flink 仍需核对 Beam/worker 哪条既有链路适合承载明确、有限的诊断操作。
4. 目标关联复用 Dask 自己的 worker 地址与任务线程映射。对 Flink 的具体 PID/共享关系处理仍需单独核对，不能直接复制该映射。

### 实现边界，不能直接照搬

以下为静态代码观察，未做故障复现。

- `finally` 在 client 侧执行，worker 内没有此 helper 设置的独立限时停止。client 崩溃/失联不等于 Tracker 已关闭。
- 新采集会先关闭已有 `_memray`，没有明确的 session ownership 保护；渲染 subprocess 未设置独立 timeout/check。
- `workers` 参数虽用于选取和取回报告，开始调用 `client.run(_start_memray, ...)` 却没有传 `workers=`。
  该版本可能在所有 worker 上启动而只关闭所选子集。它不影响“合作式运行中启动”的架构证据，但不能直接当完整受控采集实现复用。
- 测试覆盖全部 workers、scheduler、HTML/原始结果与并行下载，但本次未看到针对选定 worker 子集和 client 断连的相应保护验证。

[helper 控制流][da-memray] · [测试][da-memray-tests]

[da-release]: https://github.com/dask/distributed/releases/tag/2026.8.0
[da-tag]: https://api.github.com/repos/dask/distributed/git/tags/0df4355113335ab3a391bb3f2f0507647e9938e1
[da-config]: https://github.com/dask/distributed/blob/2026.8.0/distributed/distributed.yaml#L107-L112
[da-worker-timers]: https://github.com/dask/distributed/blob/2026.8.0/distributed/worker.py#L797-L805
[da-worker-profile]: https://github.com/dask/distributed/blob/2026.8.0/distributed/worker.py#L2308-L2393
[da-native]: https://github.com/dask/distributed/blob/2026.8.0/distributed/profile.py#L552-L610
[da-client-profile]: https://github.com/dask/distributed/blob/2026.8.0/distributed/client.py#L4394-L4488
[da-report]: https://github.com/dask/distributed/blob/2026.8.0/distributed/client.py#L6027-L6097
[da-perf-doc]: https://distributed.dask.org/en/stable/diagnosing-performance.html
[da-memory-sampler]: https://github.com/dask/distributed/blob/2026.8.0/distributed/diagnostics/memory_sampler.py
[da-memory-doc]: https://distributed.dask.org/en/stable/worker-memory.html#using-the-dashboard-to-monitor-memory-usage
[da-memray]: https://github.com/dask/distributed/blob/2026.8.0/distributed/diagnostics/memray.py
[da-client-run]: https://github.com/dask/distributed/blob/2026.8.0/distributed/client.py#L3109-L3198
[da-memray-tests]: https://github.com/dask/distributed/blob/2026.8.0/distributed/diagnostics/tests/test_memray.py

## Ray、Beam/Dataflow 与 Celery 的 Python profiling 接入

调研日期 2026-11-07。仅核对官方文档、固定版本源码与官方 PR，未运行这些框架的 profiling。
Ray 以 2.60.0 提交 `ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d` 为基线，复用此前代码研究并核对当前文档。
Beam 使用 `v2.75.0` tag，Celery 使用 `v5.7.0` tag。PySpark 与 Dask 的核对见本文相应章节。

### 快速对照

| 框架与路径 | 实际后端 | 怎样开始 | 观察范围与结果 |
| --- | --- | --- | --- |
| Ray Dashboard CPU | py-spy | 已开启 Dashboard profiling 后，运行中按需请求，节点 Agent attach PID | 进程级；task/actor 入口帮助找 worker；返回 SVG/raw/speedscope 或栈文本。 |
| Ray Dashboard 内存 | Memray | 节点 Agent 调 attach，等待后 detach，再渲染 | 目标进程内分配跟踪；返回 HTML flamegraph/table。 |
| Beam SDK bundle profiling | cProfile / guppy-Heapy | PipelineOptions 提前开启，SDK harness 包围被选中的 bundle | instruction/bundle ID 命名的统计或堆信息，经 FileSystems 写结果位置。 |
| Beam Python 容器 profiler agent | Memray、TCMalloc、自定义启动器 | 配置 profiler_agent，在 SDK harness 启动时包装进程 | worker 级产物，支持本地报告及 GCS 上传；限时停止会终止并重启 worker。 |
| Dataflow Cloud Profiler | Google Cloud Profiler | 作业启动时配置服务选项，持续采集 | Cloud Profiler 中按 job/service 和时间筛选；官方 Python 集成不支持 heap profiling。 |
| Celery 原生诊断 | RSS、objgraph、线程栈等 | 既有 inspect 控制通道或信号 | worker 进程诊断，返回文本/本地图文件路径；本次未发现 Ray 式统一 CPU 火焰图后端。 |

这几条路径没有统一的跨框架 profiling 后端。相同的是利用框架已有的进程/任务定位、启动或控制链路承载采集，
再用框架已有的通信或文件存储交付结果。采集边界分别是 bundle、worker 或整个 job，不能混同为精确 task 资源归因。
下述各节提供表中事实的主资料。

### Ray 2.60.0 简核

当前官方 profiling 文档仍将 py-spy 列为 Dashboard 原生 CPU/栈工具，Memray 列为内存工具。
其 cProfile 章节链接到手动代码分析指南，不表示 Dashboard 提供可切换的 cProfile 后端。
Dashboard profiling 默认关闭，先在 head 启用，再按需触发 worker 上的采集。
[固定版本文档][ro-ray-doc-source] · [当前文档][ro-ray-doc]

已有实现链路是 Dashboard 请求到 Reporter Head，用 State API 或 actor 元信息定位 PID/worker/node，
再通过节点 Reporter Agent RPC 调 CLI。CPU 使用 py-spy record/dump；内存使用 Memray attach、等待、detach、render。
Memray attach 的工具进程在目标进程内开启 Tracker，不能把它解释为 py-spy 一样的纯外部采样。
[Head][ro-ray-head] · [Agent][ro-ray-agent] · [Tool manager][ro-ray-manager]

Task/actor 是选择入口，采样覆盖整个进程。Head 对 task 的运行状态以及同时在 worker 上运行的其他 task 做检查和提示，
不能把整张图直接归属某个 task。结果文件由 Agent 读取，经 RPC 返回 Head，再作为 SVG/HTML/文本响应给调用方。
[目标与响应处理][ro-ray-head]

与此前本地研究相比，本次核对未发现需要改写上述核心链路的文档变化。更多边界（取消、detach 失败、默认权限、
采样开销和共享进程）应继续引用既有 Ray 源码研究，不重复扩展本轮范围。

### Apache Beam 的两类正式接入

#### 1. 随 bundle 开启的进程内 profiling

`ProfilingOptions` 提供 profile_cpu、profile_memory、profile_location、profile_sample_rate。
其中 sample_rate 是 0–1 的 bundle 选择比例，默认 1.0，不是每秒采样栈的频率。
SDK harness 建立 profiler factory，处理选中 bundle 时用 instruction ID 创建上下文。
[Options][ro-beam-options] · [Harness 初始化][ro-beam-main] · [Bundle 包装][ro-beam-worker]

`apache_beam.utils.profiler.Profile` 的 CPU 后端直接使用 cProfile，内存后端使用 guppy-Heapy。
CPU 输出原始统计，内存输出相对堆信息；路径含 profile ID 和时间，通过 Beam FileSystems 从临时文件复制到目标位置。
这是 SDK 内部实现（源码注明不保证向后兼容），具有正式的框架调用链，不能与手工在 UDF 中插工具混为一谈。
[Profiler 实现][ro-beam-profiler]

SDK 另有 WorkerStatus 通道，返回缓存状态、活动 bundle 信息和线程栈；启用 enable_heap_dump 时附带 guppy heap dump。
它是已有控制通道上的进程内诊断，不是一个外部 attach profiler。
[WorkerStatus 实现][ro-beam-status]

#### 2. 容器启动器集成 Memray / TCMalloc

Beam 官方 PR [#38853][ro-beam-pr] 于 2026-06-12 合并，v2.75.0 tag 已包含相关 Options 和 Go 容器启动器代码。
`profiler_agent` 可以选择 memray、tcmalloc，或自定义 wrapper/binary；与旧的 profile_cpu/profile_memory 选项互斥。
[Options][ro-beam-options] · [容器 profiler 实现][ro-beam-agent]

Memray 分支在启动 SDK harness 时构造下面的命令形状，因而是启动时跟踪，不是对现存 worker attach。

```text
python -m memray run <extra args> -o <worker-id-and-time>.bin -m <SDK harness entrypoint>
```

产物目录关联 job ID、hostname、worker ID 和时间。后台任务可周期运行 Memray flamegraph，生成峰值及 leaks HTML，
并以 gcloud storage rsync 上传到 profile_location 的 GCS 路径。PR 还给出 GCS FUSE 的部署方式。
TCMalloc 分支通过 LD_PRELOAD 与 HEAPPROFILE 配置；自定义 agent 作为原 Python 命令的外层 wrapper。
[实现][ro-beam-agent] · [官方使用说明][ro-beam-pr]

**停止行为与我们的目标有实质区别。** `profiler_stop_after_sec` 到期时，容器 boot 向 worker 进程组发送 SIGINT。
worker 退出后写停止标记，并在没有 profiler 的情况下重启。PR 明确说明正在执行的 bundles 会失败并由 runner 重试。
这不是在原 worker 中关闭 Tracker 后继续执行。`profiler_stop_after_crash` 则控制第一次崩溃后不再启用 profiler。
[boot 生命周期][ro-beam-boot] · [PR 中的限制说明][ro-beam-pr]

这条路径位于 Beam Python 容器的 Go boot 程序。我们当前 PyFlink PROCESS 路径使用 Flink 自己的 shell/beam_boot.py，
不能仅因 Maven/Python Beam 依赖已经是 2.75.0，就认为这些容器选项会自动生效。
它提供了很直接的设计参照：在既有启动入口包装成熟工具、保留原始文件、后处理及上传分开。
其重启停止策略是否适合 Flink 需要另行评估，不应直接复制。

### Dataflow 的托管持续 profiling

Dataflow 官方为 Java/Python 提供 Cloud Profiler 集成，Python 要使用 Runner v2（新文档也称 Portable Runner），
在启动时传入 `--dataflow_service_options=enable_google_cloud_profiler`。
还需要项目 API、worker service account 权限和 quota。它是托管集成，不是要求用户登录每个 worker 手动调用工具。
[官方指南][ro-dataflow-profiler]

结果进入 Cloud Profiler UI，按时间和 service 筛选，service 对应 Dataflow job name。
该指南明确 Python heap profiling 尚不支持，因此不能把 Google Cloud Profiler 的通用“CPU 和内存”介绍
直接当作 Dataflow Python 两者都可用。上面的 Beam Memray 容器选项是另一条独立路径。
[能力与结果入口][ro-dataflow-profiler]

### Celery 5.7.0 补充

Celery 的 worker guide 记录现成 remote-control/inspect 通道，可针对 worker 节点发送命令。
主进程的 USR1 信号可输出活动线程栈，USR2 用于远程调试；这些不是持续 CPU 采样火焰图。
[Worker guide][ro-celery-guide]

原生 inspect 命令包括 memsample（当前 RSS）、memdump（此前 RSS 样本及 GC 后状态）与 objgraph（对象反向引用图）。
objgraph 在 worker 本地生成 PNG 并返回文件路径，依赖额外 objgraph 包。
RSS 实现读取 `psutil.Process(os.getpid())`，不能将其解释为自动覆盖 prefork pool 所有 task 子进程的内存。
[控制命令][ro-celery-control] · [内存诊断实现][ro-celery-debug]

本次检查范围是当前官方 worker 文档与 control/debug 实现。在这个范围内，没有发现与 Ray Dashboard 对等的
py-spy/Memray 统一采集、渲染、下载后端。不能由此断言 Celery 生态没有第三方 profiling 扩展；也不把外部手工 attach
或任务包装器视为 Celery 已经内置的产品能力。

### 对 PyFlink 的直接启示

- 按需进程栈采样的参照是 Ray。CPU 工具和 task/worker 定位由框架接入，不要求业务函数逐个加采集代码。
- 启动式内存采集已有 Beam 官方先例；最需要比较的是停止语义及产物交付，不能只比较是否用了 Memray。
- Bundle 内部 profiling 与整个 worker profiling 可以并存。Beam 的例子也说明 profile_sample_rate 的“采样”可能指选 bundle，不能误写为栈采样后端。
- 控制通道和存储可以复用现有设施，采集后端则按目标选择。没有证据要求为了 CPU 与内存建立一套新的跨框架通用 profiling 协议。

[ro-ray-doc-source]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/doc/source/ray-observability/user-guides/profiling.md
[ro-ray-doc]: https://docs.ray.io/en/latest/ray-observability/user-guides/profiling.html
[ro-ray-head]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py
[ro-ray-agent]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_agent.py
[ro-ray-manager]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py
[ro-beam-options]: https://github.com/apache/beam/blob/v2.75.0/sdks/python/apache_beam/options/pipeline_options.py
[ro-beam-main]: https://github.com/apache/beam/blob/v2.75.0/sdks/python/apache_beam/runners/worker/sdk_worker_main.py
[ro-beam-worker]: https://github.com/apache/beam/blob/v2.75.0/sdks/python/apache_beam/runners/worker/sdk_worker.py
[ro-beam-profiler]: https://github.com/apache/beam/blob/v2.75.0/sdks/python/apache_beam/utils/profiler.py
[ro-beam-status]: https://github.com/apache/beam/blob/v2.75.0/sdks/python/apache_beam/runners/worker/worker_status.py
[ro-beam-agent]: https://github.com/apache/beam/blob/v2.75.0/sdks/python/container/profiler.go
[ro-beam-boot]: https://github.com/apache/beam/blob/v2.75.0/sdks/python/container/boot.go
[ro-beam-pr]: https://github.com/apache/beam/pull/38853
[ro-dataflow-profiler]: https://docs.cloud.google.com/dataflow/docs/guides/profiling-a-pipeline
[ro-celery-guide]: https://docs.celeryq.dev/en/stable/userguide/workers.html
[ro-celery-control]: https://github.com/celery/celery/blob/v5.7.0/celery/worker/control.py
[ro-celery-debug]: https://github.com/celery/celery/blob/v5.7.0/celery/utils/debug.py

## PyTorch Distributed / DDP（v2.15.0）

核对固定发布版 v2.15.0（2026-10-28），它是面向模型执行的分布式框架，与通用任务调度器的观测范围不同。
[Release](https://github.com/pytorch/pytorch/releases/tag/v2.15.0)

常规入口为进程内 `torch.profiler.profile`，用 schedule/step 选择 warmup 与 active 窗口。
它记录 PyTorch 算子、设备活动和集体通信；`profile_memory` 记录张量分配/释放，不能替代整个 worker 的通用内存分配分析。
每个 rank 产出各自的 trace，`tensorboard_trace_handler` 要求 worker_name 唯一，默认 hostname+PID，
文件还带时间戳。结果可导出 Chrome trace JSON，也可导出内存时间线。
[API implementation](https://github.com/pytorch/pytorch/blob/v2.15.0/torch/profiler/profiler.py#L1042-L1075) ·
[Collective profiling](https://github.com/pytorch/pytorch/blob/v2.15.0/docs/source/distributed.md#L773-L786)

v2.15.0 另有 experimental `torch.distributed.debug`。所有 rank 在初始化 process group 后显式调用
`start_debug_server`，每个 rank 建立 WorkerServer，并在已有 store 中登记地址，rank 0 提供聚合入口。
之后 `/profile` 按需请求各 rank 的 `torch_profile` handler；handler 在目标进程内创建 profiler，
等待指定时长后返回 Chrome trace JSON，前端可在 Perfetto 查看。这条 handler 默认没有开启 `profile_memory`，
不能把常规 profiler API 的内存能力直接写成该 HTTP 入口已提供内存 profiling。
这属于预先接入控制能力后、运行期合作式触发，不是通过 debugger 向普通进程注入。
[Startup](https://github.com/pytorch/pytorch/blob/v2.15.0/torch/distributed/debug/__init__.py#L27-L100) ·
[Handler](https://github.com/pytorch/pytorch/blob/v2.15.0/torch/distributed/debug/_handlers.py#L9-L23) ·
[Frontend](https://github.com/pytorch/pytorch/blob/v2.15.0/torch/distributed/debug/_debug_handlers.py#L454-L469)

同一 experimental 页面还提供 py-spy stack dump，调用外部工具并仍需部署权限。
它体现外部检查和内部采集可以共用目标寻址与控制入口；不意味着 Flink 应再建一个 HTTP debug server。
[py-spy UI handler](https://github.com/pytorch/pytorch/blob/v2.15.0/torch/distributed/debug/_debug_handlers.py#L224-L247) ·
[Experimental contract](https://github.com/pytorch/pytorch/blob/v2.15.0/docs/source/distributed.md#L1248-L1263)

对 Flink 的启示是复用已有目标身份、控制通道和报告交付，采集生命周期由具体工具实现。
若问题已经定位到 torch 模型算子或 GPU/通信，再使用 torch 自己的 profiler，不将其扩大为本轮 PyFlink CPU/内存基础功能。
