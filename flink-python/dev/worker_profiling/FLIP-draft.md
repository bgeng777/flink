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

# FLIP-XXX：Python Worker CPU 与内存性能分析

**中文** | [English](FLIP-draft.en.md)

| 项目 | 内容 |
| --- | --- |
| 状态 | 本地讨论草稿；范围和接口尚未经社区讨论或投票 |
| 讨论邮件 | 待发起 |
| 投票邮件 | 待发起 |
| JIRA | 待创建或关联 |
| 目标版本 | 待定 |
| 更新日期 | 2026-11-07 |

本提案为 Flink 管理的 standalone Python worker 增加 CPU 与内存性能分析。
CPU 使用进程外 py-spy 采样；内存同时支持启动时直接集成 Tracker，以及带明确风险提示和页面确认的按需 Memray attach。
两种内存方式均纳入正式提案，工具由部署环境作为可选依赖提供。

根据触发方式尽量复用 Flink 已有 profiling 路由、会话、结果目录和文件/BLOB 下载，
不新增 worker 控制协议或通用 profiling 框架。正文描述目标约定，附录 A 区分原型证据与尚待实现的内容。
章节沿用 [FLIP Template][flip-template]。

## Motivation

在 PyFlink 的进程执行模式下，用户函数及相关的数据处理运行在独立的 Python worker 中。
在实时多模态等作业中，这些 worker 还需要执行图片解码、特征转换和模型推理。
例如，作业通过 PyTorch 加载 YOLO 模型，对持续到达的图片进行预处理和推理。
图片解码、张量转换、数据编解码和模型计算增加了 CPU 开销；模型参数、图片、张量及批次中间结果
则增加了进程内存需求。这些开销既可能来自 Python 代码，也可能来自依赖库内部的原生代码。
[PyTorch 执行时间与张量内存分析][pytorch-profiling]

这类作业有两个典型的诊断场景：

- **CPU 开销较高，吞吐下降或处理延迟上升。** 用户需要知道时间主要消耗在哪些调用路径上，
  例如图片解码和预处理、数据转换与编解码、用户函数，或框架内部的原生计算，才能确定优化方向。
- **Python worker 内存持续增长或出现较高峰值，导致容器面临 OOM 风险。** 用户需要了解
  关键时段内存从哪些调用路径分配、哪些分配仍未释放，并结合模型加载、图片与张量中间结果、
  批次缓冲等业务行为，分析内存占用的来源。

CPU 使用率和进程内存指标能够反映资源压力，但不足以将问题定位到具体代码。
用户还需要与问题发生时段对应的调用栈或内存分配栈，以及便于查看热点、保存和共享的分析结果。
内存分配记录需要结合进程内存指标解读；采集结束时仍未释放的分配，并不直接等同于内存泄漏。

Flink 已有的诊断能力尚未完整覆盖这些需求。[FLIP-375][flip-375] 为 JobManager 和 TaskManager
提供了 JVM profiling 的触发、状态查询和结果下载，但无法直接展示独立 Python worker 的调用栈。
PyFlink 现有的 `python.profile.enabled` 使用 cProfile，在 bundle 处理期间收集函数调用统计并输出到日志。
这一接入方式有助于分析函数耗时，但尚未提供运行中按需选择采集窗口、下载分析文件和查看可视化结果的完整流程，
也不提供内存分配分析。逐函数调用跟踪在调用密集的负载上还可能显著增加执行开销，影响吞吐与背压，
因此在线诊断需要考虑采集本身对作业的干扰。

Python 生态已有成熟的性能分析工具，但在分布式部署中使用它们仍有操作成本。
用户通常需要进入目标 Pod 或主机、识别对应的 Python worker、准备工具及所需权限、执行采集并取回结果。
对于需要在进程内采集的工具，还需要安排其启停，避免将诊断逻辑散落到业务函数中。
这些步骤缺少与 Flink 运行管理的统一衔接，使一次常规的 CPU 或内存问题定位依赖较多手工操作。

本提案希望降低从 Python worker 资源异常定位到具体代码的成本：使用户无需修改业务函数，
即可在明确启用和限定采集窗口的条件下，获得可保存、可视化分析的诊断结果。
采集应尽量降低对正常处理的干扰，并复用 Flink 已有的进程管理和 profiling 能力，
让 Python worker 的诊断成为作业运维中的常规操作。

### Goals and non-goals

首版覆盖 Linux 上与 TaskManager（TM）同机、同 PID namespace 的 standalone Python worker。
管理员预先准备工具和权限，用户无需修改 UDF。一个 worker 可能服务多个算子，报告描述整个进程。

| 正式范围 | 首版行为 |
| --- | --- |
| CPU 分析 | 在现有 TM Profiler 页面选择运行中的 worker，由 py-spy 在进程外采样。 |
| 启动期内存分析 | 启动 hook 在 worker 内调用 Memray 公共 `Tracker` API，可延迟开始；每次 worker 启动只录一个窗口。 |
| 按需内存分析 | 在页面选择运行中的 worker，参考 Ray 使用 Memray `attach/detach`；单独启用并明确提示注入风险。 |
| 产物 | CPU 火焰图 SVG 和 Memray 原始 `.bin` 录制文件写入 TM 配置目录，复用现有文件/BLOB 下载；HTML、stats、tree 由 Memray 离线生成。 |

两种内存方式都属于本提案的正式功能，启用方式和生命周期不同。
首版不包含内存报告自动渲染、新查看器、持续 profiling、GPU 分析、算子独占归因、远程存储或通用 profiler SPI。
嵌入式/thread、客户端 loopback、远程 worker 和用户创建的子进程也不在范围内。
具体工具、CPython 与 Linux 组合需要经过真实验证后才能列为发布支持。

## Public Interfaces

### Configuration and optional dependencies

Flink 提供接入并维护相应约定，工具作为可选部署依赖。
py-spy 必须可被 TM 执行，Memray 必须能被 worker 实际使用的 Python 环境导入；
仅在同一 Pod 的另一个解释器中安装不够。按需内存分析还需要配置 Memray CLI 并满足注入所需条件。
CPU 与内存依赖相互独立；任一方式配置后都应支持 worker 登记/发现，不能只用 py-spy 路径作为总门槛。
Flink 不安装包、不调用 sudo，也不修改宿主机或容器权限。

| Configuration | Default | Contract |
| --- | --- | --- |
| `rest.profiling.enabled` | `false`，已有 | 本提案所有 profiling 方式的总开关。 |
| `rest.profiling.dir` | `java.io.tmpdir`，已有 | JVM、Python CPU 和两种内存采集共用的本地结果目录。 |
| `rest.profiling.duration-max` | `300 s`，已有 | 单次请求的最大录制时长，也约束启动窗口；不是注入代码的硬截止时间。 |
| `rest.profiling.history-size` | `10`，已有 | 每个 resource ID 的 REST 会话合计上限，包含 JVM、Python CPU 和内存 attach 会话。 |
| `rest.python-profiling.cpu.executable` | 无，新增 | 管理员安装的 py-spy 绝对路径，不是通用命令入口。 |
| `rest.python-profiling.memory.executable` | 无，新增 | TM 执行 attach/detach 使用的 Memray CLI 绝对路径；启动期直接调用 API 不需要此项。 |
| `rest.python-profiling.memory.attach-enabled` | `false`，新增 | 部署层显式允许向运行中的 worker 注入 Memray；同时要求总开关和 Memray CLI。 |
| `python.memory-profiling.startup.enabled` | `false`，新增 | 作业级启用：该作业使用的每个 standalone worker 启动时录制一次。 |
| `python.memory-profiling.startup.delay` | `0 s`，新增 | 从 worker 启动 hook 到开启 Tracker 的非负延迟。 |
| `python.memory-profiling.startup.duration` | `60 s`，新增 | 正数，受 TM 最大请求时长约束。 |
| `python.memory-profiling.startup.native` | `false`，新增 | 启动录制包含 C/C++ 分配栈。 |

例如，集群可准备两个工具，但保持 attach 关闭：

```yaml
rest.profiling.enabled: true
rest.profiling.dir: /flink/log/profiling
rest.python-profiling.cpu.executable: /opt/diagnostics/bin/py-spy
rest.python-profiling.memory.executable: /opt/diagnostics/bin/memray
rest.python-profiling.memory.attach-enabled: false
```

诊断作业可独立启用启动录制：

```yaml
python.memory-profiling.startup.enabled: true
python.memory-profiling.startup.delay: 120 s
python.memory-profiling.startup.duration: 60 s
python.memory-profiling.startup.native: true
```

复用现有 Python 配置、依赖准备及受管理的环境变量传递。TM 将生效的绝对结果目录传给 worker，
避免 archives/virtualenv 工作目录改变路径含义；不再新增内存结果目录配置。
工具路径、启用状态与目录来自部署或作业配置，不能由采集请求指定。
本版要求 worker 在启用后启动，不增加动态配置重载。

### Worker discovery

沿用提议中的 `GET /taskmanagers/:tm/python-workers`：

```json
{"workers": [{"id": "<opaque-worker-handle>", "pid": 12345,
              "memoryProfilingAvailable": true,
              "operators": [{"jobId": "<job-id>", "operatorId": "<operator-id>",
                             "operatorName": "infer-images", "subtaskIndex": 0,
                             "attemptNumber": 0}]}]}
```

`id` 绑定该 TM 和本次 worker 进程实例，`pid` 用于展示；重启后使用新句柄，不接受客户端任意 PID。
内部启动标识、Beam ID、依赖版本和登记文件均不是公共字段。

`operators` 列出当前关联的 Flink Python 算子，包括作业 ID、算子 ID、原始算子名、从 0 开始的
subtask 索引和 attempt 编号。共享 worker 可关联多个算子；融合后的算子按运行时算子列出，不拆解每个 UDF。
列表是发现时的快照，登记尚未完成或失败时可以为空，不能据此认定进程空闲。
页面用这些字段辅助选进程，采集仍覆盖整个 worker，不能把共享进程的 CPU/内存结果归属于某一个算子。

`memoryProfilingAvailable` 表示该 worker 已准备好按需内存分析，包括部署开关和实际 Python 环境中
Memray 导入成功；不保证注入权限或 native 栈支持。准备失败记录日志，不阻断普通 worker 启动。
启动录制正在进行属于忙状态，不能据此再 attach 一个 Tracker。CPU 能力保持独立。

### REST capture requests and responses

扩展现有 `POST /taskmanagers/:tm/profiler`。原 JVM 请求继续使用 `mode` 和 `duration`。
新增 Python 请求显式选择 `type`：

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
| `workerId` | Python 请求必填，必须是该 TM 上的有效句柄；与 JVM `mode` 互斥。 |
| `type` | Python 请求必填：`CPU` 或 `MEMORY`；REST 中的 `MEMORY` 表示按需 attach。 |
| `duration` | 必填，正整数秒，不超过 `rest.profiling.duration-max`。 |
| `includeNative` | 可选布尔值，默认 `false`，适用于两种 Python 类型。 |
| `includeIdle` | 可选布尔值，默认 `false`，仅限 CPU；内存请求中出现该字段即非法。 |

不增加任意工具参数、输出路径、采样率、渲染格式或 Python allocator tracing 参数。
显式选项不受支持时失败，不静默降级。JM profiler 不接受 Python worker 请求。
启动采集由作业配置触发，不通过此 REST 请求提交。

受理后返回 HTTP 200 和现有 `RUNNING`、`FINISHED`、`FAILED` 会话模型。
保留 `status`、`triggerTime`、`finishedTime`、`duration`、`message`、`outputFile`。
Python 响应增加 `worker`、`type` 和适用的布尔选项，省略 JVM 专用 `mode` 及 `profilingMode`。
`outputFile` 仍是单个生成文件名：CPU 对应 SVG，内存对应 `.bin`。
实际录制时间和工具版本写入产物或日志；请求时长不代表精确的实际耗时。

内存 `FINISHED` 要求确认 Tracker 已停止并关闭录制文件。attach CLI 返回、计时结束、
文件可读取都不能单独作为完成证据。有效的零分配窗口不沿用 CPU 的“零样本失败”规则。

### History, downloads, and concurrency

保留现有入口：`GET /taskmanagers/:tm/profiler` 默认返回 JVM 历史，`?target=jvm` 与之等价；
`?target=python` 返回 CPU 和内存 attach 会话，以 `type` 区分。
`GET /taskmanagers/:tm/profiler/:filename` 复用 TM 文件上传与 transient BLOB 链路下载两种产物。
不新增内存专用历史或下载服务。

REST 会话继承现有 **Flink JVM 级单活动会话限制**。同一 JVM 中的多个 TM，以及同进程的 JM/TM，
共享名额；常规部署通常是一个 TM 一个 JVM。历史仍按 resource ID 分组，先限制该 resource 的合计数量，
再按目标筛选；Python 会话可以淘汰较早的 JVM 会话。UI 和 409 提示应准确描述此作用域。

启动录制由 worker 自行触发，不申请 REST 会话名额，首版也不自动导入 REST 历史；多个 worker 可以同时录制。
日志提供生成的文件名、PID、实际起止时间和完成状态。用户可从 TM 目录取回完整文件，
也可凭日志中的文件名使用已有下载 URL。这里明确不新增 worker→TM 会话上报协议。

同一 worker 的启动内存窗口尚在等待或录制时，TM 不得再次 attach Memray。
复用私有 worker 登记元信息表达这些状态变化，不增加命令或心跳服务。
同一 worker 不支持同时运行多个 Memray Tracker，也不支持 Memray 与 cProfile 并行采集。
Flink 控制内存采集窗口期间，管理员不得另外启动手工 Memray 会话；CLI 没有 Flink 会话所有权 token。

### Result directory and retention

在 `rest.profiling.dir` 下生成 `python-<uuid>.svg`、`python-memory-<uuid>.bin` 或
`python-memory-startup-<uuid>.bin`。先写临时文件，确认关闭后再发布最终文件名。
不能删除或截断仍可能有 writer 活跃的录制文件。

REST 历史淘汰清理其拥有的产物；已知关闭的失败文件可清理，停止不明的文件保留为明确的未完成产物。
启动文件不受 REST 历史保留策略管理，由部署侧负责归档和清理，包括崩溃残留；每次 worker 重启都可能产生新文件。
启用说明必须同时写明这一策略，启动采集默认关闭。

只清理本功能拥有的文件，不清空整个目录。复用既有 BLOB 缓存，淘汰不承诺撤销已下载或缓存的副本。
TM 重启不恢复历史，持久卷保留文件而非会话；不新增远程存储或归档子系统。

Python 下载只提供最终产物名，不提供录制中的文件。
Python 产物按附件下载并设置 `nosniff`；SVG 额外设置受限 CSP，`.bin` 使用 `application/octet-stream`。
原 JVM 下载行为保持不变。用户在匹配的 Memray 环境离线执行 `flamegraph`、`stats` 或 `tree`；
native 报告需保留匹配的解释器、共享库和调试符号。
首版暂不自动渲染，避免在 worker 中增加渲染子进程或为同一会话增加第二套产物字段。

### Failure semantics and attach warning

| 条件 | REST 行为 |
| --- | --- |
| 类型、组合、时长或历史 target 非法 | HTTP 400。 |
| TM 或 worker 句柄不存在、失效 | HTTP 404。 |
| 共享 REST 会话名额占用，或已知存在冲突的内存采集 | HTTP 409，不排队、不覆盖。 |
| 工具或 worker 依赖不可用、平台不支持、attach 未启用 | HTTP 503；其他能力仍可使用。 |
| 受理后发生工具、权限、worker 退出、超时或存储错误 | `FAILED`，包含诊断信息及清理状态。 |

启动配置或依赖错误记录日志并跳过采集，不改变普通 UDF 启动；这是控制层的处理约定，
不是对 profiler 原生代码故障的隔离保证。

**Memray attach 会向运行中的 worker 注入可执行代码，可能导致进程崩溃或死锁，并触发作业失败或恢复。**
Memray 官方建议将该调试机制用于开发环境。部署开关默认关闭；Profiler 页面在内存 attach 前
展示风险并要求显式确认，REST 使用说明也应明确同样的风险，不再增加“确认风险”的请求字段。

启动 Tracker 方式避开 debugger 注入，但仍会改变分配行为、消耗资源，并依赖 Python 执行机会停止。
两种方式均不保证硬截止时间，也不保证 OOM/SIGKILL 后存在完整产物。发布文档需保留这些边界。
[Memray attach 风险说明][memray-attach]

### Web UI

复用 TM Profiler 页面：选择 JVM/Python、worker、CPU/Memory、时长及适用选项。
页面中的 Memory 指 attach，必须展示风险并确认；分别呈现能力不可用、无 worker、发现失败和忙状态。
旧 worker 失效时清空选择，不自动改选。历史区分 CPU/内存，提供“下载火焰图”或“下载内存录制文件”。
创建成功后刷新当前目标历史。

启动采集通过作业配置，文件名写入日志；页面说明其不属于 REST 创建的历史会话。
不增加路径输入、独立内存页面或自定义查看器。

## Proposed Changes

### Reuse and ownership

| 既有模块/链路 | 责任 |
| --- | --- |
| Python 依赖、环境准备和 Beam worker boot | 传递解析后的配置；保留 Beam 进程生命周期；登记真实进程身份及内存准备/启动采集状态。 |
| 现有 REST → RM → TM RPC | 校验和转发 CPU/内存请求及历史查询。 |
| 现有 `ProfilingService` | 管理 REST 会话、JVM 级名额、状态、历史与清理。 |
| Python profiling 执行 | 在后台 executor 以固定 argv 调用 py-spy 或 Memray attach/detach，管理工具子进程。 |
| 既有 `FileType.PROFILER` / BLOB | 从生效的结果目录提供生成的 SVG 或录制文件。 |
| worker 启动 hook | 直接持有一个 Tracker 窗口，不在 UDF/bundle 热路径加 hook，也不提供额外控制端点。 |

本提案不新增本地命令 socket、worker 控制 RPC、插件注册表或通用 profiler SPI。
CPU 与内存通过现有 profiling 入口分派到两个直接实现；`flink-runtime` 不依赖 Beam 实现类。
内存功能关闭时不导入 Memray；启用准备不等于立刻开启跟踪，采集只在对应触发条件满足时开始。

### Worker identity and registration cleanup

Beam 继续负责 worker 创建、退出和重启；TM 需要实际 SDK harness PID，而不是 shell 包装器 PID。
复用现有启动/进程管理；Beam 无法直接暴露身份时，沿用原型中的小型私有登记桥接。

发现、创建、执行工具前和发布结果前均验证进程存活、启动身份和 TM 管理归属。
两个外部工具最终按 PID attach，验证与 attach 不是原子操作，需明确残余 PID 复用竞态。

worker 正常退出删除登记；TM 在已有发现扫描中回收已确认失效的自有记录，覆盖 SIGKILL 和重复重启。
保留存活登记与未知文件；检查/读取失败不等于进程已死。清理失败记录日志，不能隐藏其他有效 worker。
不新增心跳或清理服务。

算子关联复用 Beam 已有的 worker ID：runner 从现有 stage 的 instruction request handler 读取
实际 worker ID，Python boot 沿用已有 `WORKER_ID`。通过现有环境准备链路增加 JobID，避免跨作业
ID 冲突。Java 算子打开后写入自己的关联记录，关闭时删除；发现流程按 JobID + Beam worker ID
合并到既有进程登记。共享 worker 自然得到多个关联，不另建进程池、引用计数或所有权服务。
关联记录先写临时文件再原子发布。写入或发布发生 I/O 错误时，在登记调用内尽力清理临时文件，
保留原始错误供调用方记录日志；不依赖之后的算子关闭，也不因清理失败覆盖原始错误。
文件更新只发生在算子生命周期边界，不进入逐条记录或 bundle 热路径。
当前 Beam 2.75.0 的接入点是 `FnApiControlClient.getWorkerId()`，仅 `flink-python` 依赖该 Beam 类。
该关联针对当前 Flink 的 stage/environment 生命周期；若以后启用 Beam 环境轮换或负载均衡，需要重新验证。

### CPU lifecycle

保持当前 py-spy 路径，固定 50 Hz，按请求追加 native/idle。
受理、执行、历史、清理、下载复用已有基础设施。工具成功退出、存在产物且具有经验证的正样本完成信息后，
才发布 `FINISHED`。例如 `Samples: N` 是对验证版本的输出约定，不是稳定的通用协议。
无效或零样本结果失败，不回退到 cProfile。

CPU 后端从工具启动开始按实际经过时间计时；到期向 py-spy 发 SIGINT 请求停止采样并生成结果，
另给有界收尾时间（当前 POC 为 10 秒）。仍不退出才终止/强杀采样器并标记失败，不能将强杀当作完整结果。
初始化会占用该窗口，实际采样时间可能短于请求时长。保留工具自身的 duration 作为额外退出条件；
已验证 py-spy 0.4.3 的该参数按采样轮数结束，慢 native 展开可能超出名义时长。


TM 退出时取消并等待其所属采样器清理，不关闭被其他 TM 共用的 JVM 级 profiling service。
迟到的 worker 查询不能在关闭开始后新建采集；工具等待不阻塞 TM RPC 或数据处理线程。

### Direct-API startup memory lifecycle

standalone boot hook 准备可选 Memray 导入并安排一次采集。一个 worker 线程从进入到退出持有
`Tracker` context，按实际窗口计时，结束后同一 worker 继续处理。
harness 正常返回或抛出 Python 异常时，取消尚未开始的窗口，或关闭正在运行的 Tracker 一次。

复用现有环境、archives 和 requirements 准备。目录来自 TM 解析后的 profiling 配置。
hook 记录生效参数、工具版本、PID、实际时间和文件名；重启按同一配置重新生成文件，不续写前一次结果。

启动 hook 负责确认自身采集已关闭，`Tracker.__exit__` 完成后才将 `.bin.inprogress` 改名为完整 `.bin`。
异常终止可能留下未完成文件。启动和 attach 的触发链不同，但使用相同 Memray 分配语义和产物格式。

### On-demand memory lifecycle, following Ray

Ray Reporter Agent 调用 `memray attach`、等待、`detach`，随后生成报告。
采用这种工具接入方式，同时保留 Flink 异步会话和 BLOB 链路。Ray manager 没有提供可复用的协作式
worker 控制协议；Beam 2.75 Fn API 也没有 Memray 启停指令，其 bundle profiler factory 不等价于进程窗口。
[Ray Agent][ray-agent] · [Ray Manager][ray-manager] · [Beam Fn API][beam-fn-api]

REST 内存请求按以下步骤执行：

1. 检查 worker 实例、依赖准备、attach 开关和会话名额；已知启动窗口或 cProfile 冲突应拒绝。
2. 生成唯一临时录制路径，以固定 argv 启动配置的 Memray CLI。
   在支持版本中验证目标侧 `--duration` 兜底计时；建议设为请求时长加少量清理宽限，
   使正常 TM 控制的 detach 先于兜底发生。
3. 在 profiling executor 等待请求窗口，然后执行 detach；失败/取消分支也负责这一清理尝试。
   限制 CLI 等待并回收工具/debugger 子进程，不以杀死业务 worker 作为 profiler 降级方案。
4. 在已声明的独占控制前提下，以成功 detach 作为正常关闭确认，再校验和发布录制文件，标记 `FINISHED`。
5. 无法确认停止时，标记 `FAILED` 并明确提示“停止未确认”；不报告成功、不删除可能仍有 writer 的文件，
   也不自动重新 attach。在确认停止或 worker 退出之前保留该活动会话名额。
   TM 关闭只做有界诊断等待，worker 终止仍由既有任务/Beam 生命周期负责。

目标侧计时是 TM 消失后的兜底，不是完成证据或硬上限。
Memray 1.20.0 在 Tracker 关闭完成之前就清空 “last tracker” 引用，
因此 detach 的“没有先前 attach”错误不能单独解释为成功。
Ray 在 detach 失败后返回 warning 并继续渲染的行为，不足以满足 Flink 的 `FINISHED` 约定。
结束 CLI 子进程也不等于目标内跟踪已经停止；这些路径必须在支持版本上实际验证后再合并。
[Memray 生命周期实现][memray-code]

HTTP 客户端断开不取消已经受理的 Flink 异步会话。不增加通用重试，不复制 Ray 的 sudo/setuid 降级。
采集窗口内 Memray 控制权必须独占；CLI 无法辨别另一个手工 attach 是否属于不同会话。

### Tool compatibility and interpretation

发布经过验证的 Flink、CPython、工具版本、Linux 架构和 native 模式矩阵。
Flink 维护者负责这些组合上的调用/API、停止、失败与产物约定；部署方负责安装和权限。
工具升级必须重新运行可重复的真实工具 smoke 和生命周期测试，采集失败必须让验收失败；
当前探索脚本中“作业正常退出”本身不算工具兼容性通过。不需要版本适配框架。

Ray 将 py-spy 放在 `ray[default]`、Memray 放在 `ray[observability]`，节点 manager 检查工具可用性。
借鉴可选依赖准备，不把 Agent PATH 中有命令当成目标解释器能导入 Memray。
记录实际版本，并要求 Memray CLI 与 worker 包采用已验证组合。[Ray 依赖][ray-dependencies]

CPU 图的宽度是栈样本比例，不是精确 CPU 时间；native 模式可能看不到没有 Python 线程状态的原生线程池，
也不采集 GPU kernel。内存文件描述窗口内的分配，不解释全部 RSS/PSS、容器内存或 GPU 显存，
也不能回溯归因采集开始之前的分配。native 开关增加 C/C++ 栈，关闭时仍可能记录原生分配。
窗口结束仍未释放的记录不自动等同于泄漏。

沿用部署已有 REST 访问约束，保护可能含源码路径的产物。本提案不建立互不信任作业间的新隔离机制。

## Compatibility, Deprecation, and Migration Plan

所有新增方式均显式启用。保留 JVM 请求/响应、默认 JVM 历史、文件路由和 `python.profile.enabled` 行为；
该配置继续指现有 cProfile，不重命名或静默替换。同一 worker 不支持 cProfile 与 Memray 同时采集。

本稿新增 Python 接口和配置在发布后形成公共约定。
本地启动原型的四个 `PYFLINK_MEMRAY_*` 环境变量不是拟议公共接口或兼容别名，
需迁移到正式配置；尚未发布的无 `type` CPU 请求也不承担已发布兼容责任。

同一 FLIP 下可以分 CPU、启动内存、attach 内存三个实现变更评审，内存仍在正式范围。
Attach 合并要求落实风险提示、默认关闭和真实生命周期证据。
既有 worker 需重启以应用依赖准备和启动配置；不改变 checkpoint/savepoint 格式或 UDF 签名。

## Test Plan

| 范围 | 必需证据 |
| --- | --- |
| 协议/兼容 | 原 JVM 客户端和生成 REST schema；Python CPU/Memory 区分；非法组合；400/404/409/503；下载 MIME/headers。 |
| 身份/生命周期 | 多 worker/TM、PID 复用、SIGKILL 登记回收、保留存活/未知文件；关联发布失败时清理临时文件并保留无关文件；TM 退出、排队采集与迟到查询。 |
| 启动 API | 关闭时不依赖 Memray；缺包、延迟、正常/异常退出、只关闭一次、重启、继续处理；archives 或禁止继承环境变量时目录仍正确。 |
| Attach | 真实成功、权限与缺包；注入/detach 失败；兜底计时竞态；CLI/TM 中断；兜底计时尚未结束时 worker 正常退出；停止不明；不主动杀业务 worker、不误报完成。 |
| 所有权 | 启动等待/采集与 attach 冲突；重复请求；JVM 级 REST 名额；明确并验证 Memray 独占控制前提。 |
| 产物 | 真实 SVG 和已关闭可读的 Memray binary；零分配、未完成文件、不可写/空间不足；REST 保留策略与显式启动文件保留策略。 |
| 代表负载 | 预热后的 CPU PyTorch 推理，覆盖单线程及默认原生线程池；对比不采集、CPU 采集、内存采集的吞吐和尾延迟，列明可见/遗漏栈。 |
| 操作流程 | 单 TM、两个未融合算子、关闭 Python managed memory；按算子/subtask/attempt 选择，核对独立 PID 和热点。开启 managed memory 作为共享 worker 对照，验证两个关联和关闭清理；验证 attach 提示与下载。 |

Native Memray 文件可能很大，需验证增长、开销和停止能力。
文件大小监控可以请求停止，但无法在不影响 worker 的前提下为失去响应的进程内 Tracker 强制施加硬文件上限。
部署存储约束及此限制必须写入说明；初始支持矩阵从适度窗口和小规模诊断负载取得证据。

## Rejected Alternatives

| Alternative | Reason |
| --- | --- |
| 仅因 Memray 是外部可选依赖而排除内存 | 可选准备是合理部署约定；Flink 仍维护接入和兼容性。 |
| 首版新增协作式 worker 命令 socket/RPC | 启动 API 与明确风险的 attach 已覆盖选定的两种方式，无需另一套控制协议。 |
| 把 Beam bundle/status/metrics 消息改作 profiler 命令 | 没有匹配操作，复用不相符语义会形成协议维护负担。 |
| 独立 REST 服务、历史、产物库、调度器或 profiler SPI | 既有 Flink profiling 链路已能承担按需请求的这些责任。 |
| 把 attach 进程退出或 binary 可读当成完成 | 目标内可能仍在录制，必须确认停止。 |
| 首版自动渲染所有内存报告或暴露全部 Memray 参数 | 先保留一种原生 binary，利用 Memray 既有离线工具。 |
| 为启动窗口增加集中调度和实时 REST 历史 | 需要额外上报/准入协调；首版明确使用日志/文件定位及部署侧保留策略。 |

## Open Questions

以下需要证据和社区共识，不代表已经实现：

| ID | 决策及建议 |
| --- | --- |
| D1 | 验证 Memray CLI/目标关闭及兜底计时行为，包括 debugger 取消及其对 worker 正常退出的影响；attach 必须具备风险开关和真实的停止未确认语义。 |
| D2 | 用实际 PyTorch 负载确定首版 Linux/CPython/工具组合、可接受开销与文件规模。 |
| D3 | 采用最小算子关联列表，帮助在单 TM、多 worker 时选择目标；验证独立 worker、共享 worker 和关闭清理，报告仍为进程级。 |
| D4 | 确认首版取舍：启动文件共用目录/下载，但不进入 REST 历史及其保留策略。 |

## Appendix A: Current implementation and remaining work

本地变更基于 `7b5af6de1e1`（上游基线 `8d01380ff5bf9892462ef9808b1b56fd9723f54d`），
已按本稿调整 POC；不代表功能已获社区批准或发布。

| 已实现并验证 | 仍需完成 |
| --- | --- |
| `CPU`/`MEMORY` 请求类型、worker 内存可用性、正式配置、JVM 级会话限制、失效登记回收。 | 更广的工具、Python 和平台版本组合。 |
| 复用 REST/RPC/历史/下载/UI；上一轮 73 项 runtime、16 项 Python memory、8 项环境管理、37 项前端测试通过；REST snapshot 和配置文档已更新。 | 全仓库 `clean verify`。 |
| Memray 1.20.0 native attach/detach 真实采集，风险开关与 UI 确认、停止不明保留名额、未完成文件拒绝下载。 | 注入原生崩溃/死锁、权限及不同 debugger 故障的更完整实验。 |
| 正式配置驱动启动 Tracker，继承的旧环境变量不能绕过关闭配置；结果使用共用目录和下载端点。 | archives 等部署组合及实际磁盘容量管理验证。 |
| Linux AArch64、CPython 3.11.18、Beam 2.75.0 的两种内存采集均生成可读分配栈，下载一致且作业继续运行；已有 py-spy 0.4.3 CPU/native 证据。 | 更广的模型、架构和部署组合；独立硬件上的更长开销对照。 |

后续已在 6 GiB VM 上启动两个独立 JVM TM、每个 TM 一个 slot，完成图像与 CPU ResNet-18
作业的双 worker 并行 CPU/native Memray 采集，验证跨 TM 句柄隔离、同 TM 冲突、下载和 worker 存活。
单线程限制和默认 6 线程分别完成三轮普通 CPU/内存对照；有原始吞吐、UDF 延迟和每线程 CPU 证据。
默认线程池中 5 个活跃原生线程未进入 py-spy SVG，因此定位到 PyTorch 调用链不代表覆盖全部原生 CPU 消耗。

实验还发现 native py-spy 的时长结束会迟于名义窗口。已改成到期 SIGINT、有界收尾、强杀兜底；
70 项 runtime 测试通过，原先超时的真实 REST 请求修复后完成。三轮开销矩阵采自此修复前，
修复后单独验证 native 完成，未重复完整开销矩阵。配置校准和 Metaspace 故障均保留在报告中。

证据见 [双 TM / PyTorch / 开销与停止实验](../../../output/playwright/pyflink-20261107/torch-multiworker/run.md)、
[内存接入报告](../../../output/playwright/pyflink-20261107/formal-memory/run.md) 和[内存使用说明](Memory-experiment.md)。
这些是本地短窗口观察，不提供生产开销保证；原生故障注入、其他版本和全仓库验证仍待完成。

后续单 TM、单 slot 实验关闭 `python.fn-execution.memory.managed` 及 Python/Java 算子融合，
两个未融合算子产生独立 worker；UDF 自报 PID 与算子/subtask/attempt 关联一致。分别采集的 CPU
火焰图包含目标热点且不包含另一算子的热点，Memray 也验证了目标 PID 和分配栈。
开启 managed memory 的对照实验中，一个 worker 列出两个算子，火焰图包含双方热点。
取消作业后关联记录全部清理；旧句柄返回 404，并发请求返回 409。见
[单 TM 验证报告及截图](../../../output/playwright/pyflink-20261107/single-tm-workers/run.md)。

随后补齐关联登记失败时的临时文件清理。重命名失败回归测试先在修复前复现残留，再在修复后通过；
本轮 29 项 worker profiling 测试和 4 项 REST 集成测试通过，Spotless、Checkstyle 及 diff 检查通过。
本次未重跑 standalone 端到端实验或全仓库测试。

## References

- [工具选型研究](Profiler-selection-research.md)。
- [分布式 Python 框架对照](Distributed-framework-profiling-research.md)。
- [Ray 2.60.0 / Memray 1.20.0 源码研究](Ray-profiling-research.md)。
- [FLIP 流程和公共接口约定][flip-process]。

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
