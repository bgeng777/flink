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

# FLIP-XXX：Python Worker 按需 CPU 性能分析

**中文** | [English](FLIP-draft.en.md)

| 项目 | 内容 |
| --- | --- |
| 状态 | 本地讨论草稿；范围和接口尚未经社区讨论或投票 |
| 讨论邮件 | 待发起 |
| 投票邮件 | 待发起 |
| JIRA | 待创建或关联 |
| 目标版本 | 待定 |
| 更新日期 | 2026-11-07 |

本提案为 Flink 管理的独立 Python worker 增加限时调用栈采样，用于定位 CPU 热点，
并在现有 TaskManager Profiler 页面提供目标选择、状态查询和火焰图下载。
推荐使用管理员预装的 py-spy，复用 Flink 已有的 profiling 会话、并发限制、结果目录与下载链路。
内存问题是同一诊断需求的另一部分，但 Memray 接入仅作为独立实验列于附录 A，不属于本轮交付承诺。
正文描述推荐的公共约定；原型实现及尚需补齐的差异见附录 B。
文档采用 [FLIP Template][flip-template] 的章节结构。

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

首版面向 Linux 上与 TaskManager（TM）同机、同 PID namespace 的 standalone Python worker。
目标用户是已有集群诊断权限的运维人员和开发者。用户从现有监控确认某个 Python 进程存在 CPU 压力后，
在 TM 页面选择对应 worker，采集一段时间的调用栈，下载报告定位热点，无需修改 UDF。
管理员需要预先准备工具、权限和启用配置；这不是对任意未准备好的运行中进程进行零配置 attach 的承诺。

| 本轮交付 | 本轮不交付 |
| --- | --- |
| worker 发现与选择、限时采样、可选原生栈和等待线程、采集状态、SVG 下载 | 内存分配采集、GPU kernel/显存分析、I/O 专项分析 |
| TM 本地结果目录配置，复用现有历史和清理机制 | 集中产物存储、跨 TM 聚合、持续 profiling、自动诊断 |
| 进程级结果和明确的部署说明 | 算子级独占归因、任务到 worker 的自动映射、新的进程指标 |

一个 worker 可能服务多个算子；报告覆盖该进程，不代表某个算子的独占开销。
嵌入式/thread 模式、客户端 loopback、远程 worker 和用户自行创建的子进程不在首版支持范围。
macOS 原型实验不构成发布支持。具体 Linux 架构、CPython 和 py-spy 版本组合须经真实验证后列入文档。

## Public Interfaces

### Configuration

只新增一个工具路径配置，其余复用已有配置，不新增 Java/Python UDF 公共 API 或 profiler SPI。

| Configuration | Default | Contract |
| --- | --- | --- |
| `rest.python-profiling.executable` | 无，新增 | 管理员安装的 py-spy 的绝对路径，不是通用命令或插件入口。路径未配置时 Python 采样不可用。 |
| `rest.profiling.enabled` | `false`，已有 | profiling 总开关；Python 采样还要求配置工具路径。 |
| `rest.profiling.duration-max` | `300 s`，已有 | 单次请求的最大采集时长。 |
| `rest.profiling.history-size` | `10`，已有 | 同一 TM 保留的 JVM 和 Python 会话合计上限，沿用滚动淘汰。 |
| `rest.profiling.dir` | `java.io.tmpdir`，已有 | 执行采集的进程所使用的本地结果目录；Python 结果写入目标 worker 所在 TM 的该目录。 |

例如，在集群配置中指定：

```yaml
rest.profiling.enabled: true
rest.python-profiling.executable: /opt/bin/py-spy
rest.profiling.dir: /flink/log/profiling
```

建议配置绝对结果路径；保留既有 `rest.profiling.dir` 的路径解析语义。
配置属于部署级约定，不支持通过采集请求更改工具路径或输出目录。
首版要求 TM 及 worker 在启用配置后启动；不增加动态重载或对未登记 worker 的追溯发现。

### Result directory and retention

成功采集生成 `<rest.profiling.dir>/python-<uuid>.svg`，文件名由 Flink 生成。
同一配置项也继续供 JVM profiling 使用。用户可以直接从目标 TM 目录取文件，
也可以通过现有 REST/Web UI 下载；JobManager 负责转发下载，不是 Python 产物的持久存储位置。
TM 必须能够创建和写入该目录。创建目录、写入或磁盘空间不足导致的错误应表现为采集失败，诊断控制逻辑不得因此取消作业。

历史淘汰会删除对应的本地结果文件，失败采集的临时文件由采集生命周期清理。
只清理本功能拥有的文件，不扫描或清空整个配置目录。
复用已有 BLOB 下载缓存；文件淘汰不承诺立即撤销已下载副本或缓存副本。
历史记录仍驻留内存，TM 重启后不自动扫描目录恢复历史；挂载持久卷只能保留尚未被清理的文件，不能恢复历史会话。
长期归档由部署环境完成，本 FLIP 不新增远程存储或归档配置。

### Worker discovery interface

新增 `GET /taskmanagers/:tm/python-workers`，返回本 TM 已知且存活的候选 worker。
出现在列表中不保证操作系统一定允许采样；权限和工具执行错误仍可能在采集时发生。

```json
{
  "workers": [
    {"id": "<opaque-worker-handle>", "pid": 12345}
  ]
}
```

`id` 是绑定到该 TM 和本次 worker 进程实例的不透明句柄，供创建请求使用；`pid` 用于展示和人工核对监控。
worker 退出或重启后，旧句柄失效，不自动指向后继进程。没有存活 worker 时返回空数组。
内部启动标识、Beam worker ID 和登记文件格式不作为公共字段；用户不能提交任意 PID。
进程发现的内部实现见 Proposed Changes，不将某一种登记机制写成接口承诺。

### Capture requests

扩展已有 `POST /taskmanagers/:tm/profiler`。原 JVM 请求继续使用 `mode` 和 `duration`。
Python 请求如下：

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
| `workerId` | Python 请求必填，必须是该 TM 列表返回的有效句柄；与 JVM `mode` 互斥。 |
| `duration` | 必填，正整数，单位为秒，不超过 `rest.profiling.duration-max`。 |
| `includeNative` | 可选，默认 `false`，请求 Python 线程所对应的原生调用栈。 |
| `includeIdle` | 可选，默认 `false`，将等待线程纳入样本。 |

后两个字段只接受布尔值，仅适用于 Python 请求。不增加任意工具参数、输出格式或采样率参数。
显式请求的采样选项不受支持时返回失败，不静默降级。JobManager profiler 不接受 Python worker 请求。

成功受理继续返回 HTTP 200 与会话信息，沿用 `RUNNING`、`FINISHED`、`FAILED`。
受理不代表已经取得样本；启动工具失败也可使会话立即进入 `FAILED`。
Python 会话保留 `status`、`triggerTime`、`finishedTime`、`duration`、`message`、`outputFile`，
新增 `worker`（上述 `id`、`pid`）及两个实际采用的采样选项。
Python 会话省略仅适用于 JVM 的 `mode` 及既有 `profilingMode` 别名，
与生成 schema 中的可选枚举字段保持一致，不将栈样本伪装成 JVM CPU 事件。
时间和文件名字段沿用现有含义；`outputFile` 返回生成的文件名，不允许客户端指定本地路径。

### History and downloads

| Endpoint | Contract |
| --- | --- |
| `GET /taskmanagers/:tm/profiler` | 保持返回 JVM 历史；不向旧客户端混入 Python 会话。 |
| `GET /taskmanagers/:tm/profiler?target=jvm` | 与不带参数相同。 |
| `GET /taskmanagers/:tm/profiler?target=python` | 显式返回 Python 历史，使用已有 `profilingList` 包装。 |
| `GET /taskmanagers/:tm/profiler/:filename` | 复用现有文件下载链路。 |

`target` 为新增可选查询参数，只接受 `jvm`、`python`，默认 `jvm`。
Python 结果只出现在 Python 创建响应和显式选择 Python 的查询中。
因此旧 JVM 客户端不会因启用本功能而收到新的空 `mode`。
历史按现有顺序返回；先执行合计数量限制，再按目标过滤，不是每类各保留 `history-size` 条。
启用 Python 采集后，其会话可以淘汰较早的 JVM 会话；这项资源共享行为需要在配置文档中明确。

同一 TM 的 JVM 与 Python 采集共享已有活动会话限制；不增加队列、取消接口或集群级调度。
Python SVG 按附件下载，设置 `nosniff` 和受限 CSP，不以内联脚本形式嵌入 Dashboard。
原有 JVM 报告下载行为保持不变。

### Failure semantics

以下是新增 Python 请求的约定，复用 Flink 的标准错误响应结构；不借此改变现有 JVM 请求的错误处理。

| Condition | Result |
| --- | --- |
| 参数类型、组合、时长或 `target` 非法 | HTTP 400。 |
| TM 或 worker 句柄不存在、创建前已失效 | HTTP 404。 |
| 创建时已有活动 profiling 会话 | HTTP 409，不排队或覆盖已有会话。 |
| 未配置 Python 工具或已知平台不支持 | HTTP 503；已有 JVM 功能仍可使用。 |
| 受理后权限不足、工具失败、worker 退出、超时或结果写入失败 | 会话进入 `FAILED`，包含可诊断的信息，回收采样器并清理临时产物。 |

总开关关闭时沿用现有 profiling 接口不可用的行为。
发现服务故障应返回错误，不能伪装成正常的空 worker 列表。
诊断控制逻辑不得以终止业务 worker 来结束采集；采样仍可能短暂停顿目标进程，其影响需要实测。

### Web UI

复用 TM Profiler 页面，操作顺序为选择 JVM/Python、选择 worker、指定时长、采集、下载。
Python 模式显示 PID 和句柄，提供 native/idle 高级选项及进程级结果说明。
只有一个 worker 时可以默认选中；选中的 worker 失效时清空选择，不能自动切换到另一个进程。
显示当前目标类型的历史；共享会话限制导致拒绝时，提示已有采集正在进行。
区分未配置、没有 worker 和发现失败，Python 相关错误不阻断 JVM 操作。
页面无需提供本地路径输入，也不新增独立查看器。

## Proposed Changes

### Reuse and ownership

[FLIP-375][flip-375] 已建立请求、会话与结果下载链路。本提案在此基础上增加 Python 目标和工具执行，
不再建立另一套 profiling 服务。

| Module | Responsibility |
| --- | --- |
| `flink-python` 环境准备与 worker 启动 | 沿用 Beam 管理进程生命周期，向 TM 提供识别真实 Python worker 所需的最小元信息。 |
| `flink-runtime` REST 与 RM/TM RPC | 校验公开请求、转发、按目标筛选历史；复用既有路由。 |
| `flink-runtime` profiling | 在现有会话限制下解析目标、执行 py-spy、记录终态及清理资源。 |
| 既有文件与 BLOB 链路 | 按生成文件名下载配置目录内的结果。 |
| `flink-core`、Web UI、`docs` | 配置定义、既有页面接入、生成配置与 REST 文档。 |

不在每条记录、UDF 调用或 bundle 处理路径中增加采集 hook。
`flink-runtime` 不依赖 Beam/Python 执行实现，也不需要把第三方 profiler API 暴露为 Flink SPI。
未启用时不启动采样器；已启用但没有采集时，仅保留 worker 发现所需的管理元信息。

### Worker identity and process management

复用现有 Python 环境和启动链，Beam 继续拥有进程的创建、退出与重启管理。
TM 需要的是实际运行 SDK harness 的 Python PID 及其进程实例标识，而不是 shell 包装器 PID。
优先在既有启动/进程管理接入点取得这些信息；若当前 Beam 对外接口无法提供，才增加最小元信息桥接。

现有原型使用 standalone worker 在进入 harness 前写入 TM 私有工作目录、退出时删除的登记记录。
它补足真实 PID，不引入第二套进程管理、心跳服务或用户可调用的注册接口。
公开句柄与内部记录分离，启动标识和 Beam ID 留在内部。
如果通过启动器直接取得同等信息更简单，可以替换记录写入方式，公共接口不变。

TM 在列表、创建、采集过程中及结果发布前检查进程存活和实例身份，并校验目标属于本 TM 管理范围。
PID 被复用不能使旧句柄静默切换目标。py-spy 最终按 PID attach，因此检查与 attach 并非原子操作；
这个残余竞态必须在评审中说明，不把登记目录或句柄视为多租户安全隔离机制。
本轮不为消除该工具边界而设计额外的 worker 控制协议。

### Sampling and lifecycle

请求沿 JM REST、RM、TM 到达现有 `ProfilingService`；外部工具等待使用 profiling executor，
不阻塞 TM RPC 或数据处理线程。py-spy 以固定 argv 启动，不经过 shell。
默认以 50 Hz 验证以下命令，按请求追加 `--native` 或 `--idle`：

```text
/configured/py-spy record --pid <resolved PID> --duration <seconds>
  --rate 50 --threads --format flamegraph --output <generated path>
```

火焰图宽度表示采集的栈样本比例。默认排除被工具判定为 idle 的线程，也不等同于精确 CPU 时间计量；
启用 idle 后还会包括等待。native 模式不能保证覆盖没有 Python 线程状态的原生线程池，更不采集 GPU kernel。
这些限制对 PyTorch 等负载尤其重要，应在支持说明及真实样例中展示，不能据此承诺完整模型性能归因。

工具成功退出、产物完整且含有效样本后才发布 `FINISHED`。
非空 SVG 只是原型检查，不足以证明采集成功；实现需使用经验证的工具完成信息或结果检查，识别零样本结果。
worker 失效、采样超时或服务关闭时回收采样器，保持业务 worker 的正常生命周期。
超时宽限、文件/日志限额是内部资源控制，不新增对应配置；不得无限等待工具退出或缓存其输出。
清理失败记录日志，不阻止会话进入终态。

### Deployment, access, and tool support

py-spy 由管理员在 TM 环境预装；不打包进 Flink 发行物，不运行 pip 安装，也不自动提权。
发布文档列出实际验证过的 py-spy、Linux 架构和 CPython 版本组合，以及普通采样与 native 模式的差异。
不假设所有 PyFlink 支持的 Python 版本天然都被采样工具支持。
PID 可见性、同 UID、Yama 和 seccomp 等条件需要在容器环境实际验证；相同 UID 不是权限保证。
启用时在 INFO 日志记录生效状态、工具路径、结果目录和采样参数，工具失败保留有界诊断信息。

沿用现有 Flink REST 的部署访问边界；能使用该入口的人员应已有目标 TM 的诊断权限。
与 JVM profiler 相比，这里新增了外部程序执行，必须明确约束：可执行文件和目录来自管理员配置，
请求只接受已发现的 worker 及类型化参数，下载只能解析受控文件名，不接受 shell、任意 PID 或文件路径。
报告可能包含函数名与源码路径，结果目录和下载入口应使用与其他诊断产物相同的访问管理。
本 FLIP 不新增鉴权体系，也不宣称能在互不信任的作业之间提供新的隔离保证。

## Compatibility, Deprecation, and Migration Plan

- 默认关闭。已有 JVM 创建请求、默认历史查询和报告下载保持原有格式；Python 元信息仅通过显式 Python 操作返回。
- JVM 与 Python 共用会话并发和历史容量；启用 Python 后可能拒绝同时发起的 JVM 采集或淘汰较早记录。这是明确的共享资源行为。
- 现有 `python.profile.enabled` 和 cProfile 行为保持不变，无强制迁移或自动工具切换。
- 不修改 UDF API、每条记录的执行逻辑、checkpoint/savepoint 或状态序列化，不新增 Flink Java/Python 运行依赖。
- 配置生效需要按现有部署方式重启相关进程；已启用且可发现的 worker 可在运行中按需采样，无需为每次 CPU 采集重启作业。
- 没有 API 弃用或状态迁移。新增配置、查询参数、响应字段和条件约束需进入生成的配置文档、REST/OpenAPI 文档及稳定性测试。

## Test Plan

验收以完整用户流程和真实采样为核心，不能仅凭模拟 SVG 或后端单元测试宣布功能可用。

| Scenario | Acceptance evidence |
| --- | --- |
| Linux 真实 CPU 作业 | 从 worker 列表选择目标，经 REST 和页面采集，下载 SVG 并找到预先设定的 UDF 热点；业务输出正确。 |
| 代表性多模态负载 | 图片预处理及一个已知 C 扩展调用；记录 native 栈实际覆盖到哪一层，区分 Python 线程与原生线程池。 |
| 生命周期与共享 | 多 worker、共享 worker、重启、PID/旧句柄失效；采样不切换目标，历史始终说明进程级范围。 |
| 文件生命周期 | 指定 TM 本地目录，验证产物位置及下载内容；覆盖不可写、磁盘写入失败、历史淘汰、失败临时文件、无关文件保留及重启后无历史恢复。 |
| 错误与清理 | 无权限、工具不存在/不支持选项、零样本、超时、worker 退出、TM 关闭；验证失败可见、采样器回收和可继续发起后续采集。 |
| 兼容性 | 使用旧 JVM 请求/响应样例调用修改后的服务；默认列表无 Python 记录，显式列表符合新约定；验证共享容量、并发限制和 JVM 下载不变。 |
| REST 与 UI | 400/404/409/503 与会话 FAILED 的区分；非法路径/句柄拒绝；空列表和发现失败不同；用户选中目标失效后不自动换目标。 |
| 开销 | 同一负载交替运行未启用、启用未采样、采样三个条件，记录吞吐、端到端延迟及 worker/TM/采样器的 CPU 和内存；普通/native 模式分别报告。 |

单元测试覆盖协议与生命周期，MiniCluster 集成测试覆盖 REST/RPC/历史/下载，Linux 作业验证真实工具行为。
开销实验记录环境、版本、采样率、线程数、持续时间、重复次数和基线波动；公开测量结果，不把工具基准当作业务开销保证。
默认关闭和启用未采样时不应出现可重复的吞吐退化；采样预算结合代表性工作负载在评审中确认。

发起投票前至少提供一个真实 Linux 采集样例，并收敛 Open Questions 中的范围和接口决定。
合并前完成上述关键验收、受影响模块检查、配置/REST 文档生成与项目要求的完整验证。
支持矩阵只列已测组合。原型已有测试不能替代新增目录、筛选和错误契约的测试。

## Rejected Alternatives

| Alternative | Reason for the recommendation |
| --- | --- |
| 仅增强 cProfile 导出和可视化 | cProfile 本身可生成可视化输入，仍适合受控函数分析；但本轮优先满足运行中按需、较低干扰的进程级采样。它不作为权限失败时的自动替代工具。 |
| 用 JVM profiler 解释独立 Python worker | 采样对象不同，不能得到该进程内的 Python 栈。 |
| 给每种工具建立独立控制服务和结果存储 | 已有会话、限制、状态、目录与 BLOB 下载足以承载首版。 |
| 默认历史直接混合 JVM/Python，要求旧客户端处理空 mode | 将原型实现细节变成兼容性负担；新增查询参数让调用者明确选择。 |
| 新增 Python 专用创建/历史 endpoint | 也是可行方案；当前推荐扩展现有请求和目标筛选以集中管理。若 REST/OpenAPI 无法清楚表达条件响应，应在投票前改用独立路由，内部仍复用同一链路。 |
| 接受任意 PID、命令参数或输出路径 | 把部署和目标校验责任转移给调用者，扩大诊断入口权限。 |
| 首版同时提供 Memray attach、Scalene 和通用 profiler SPI | 进程内工具涉及不同的准备与停止条件，尚不足以形成稳定的共同接口；先完成附录实验。 |

工具选型和跨框架证据分别见[选型研究](Profiler-selection-research.md)与[框架对照](Distributed-framework-profiling-research.md)。

## Open Questions

以下是需要社区明确取舍的决定；推荐值不是已获批准的结论。

| ID | Decision | Recommendation and remaining evidence |
| --- | --- | --- |
| D1 | 首轮范围是否仅包括 CPU | CPU 功能单独评审，内存实验不进入本轮公共承诺；保留后续独立提案的空间。 |
| D2 | worker ID/PID 是否足以完成目标选择 | 首版提供 TM 级入口，使用现有日志/监控人工关联；需要在多作业、多 worker 的真实部署中证明可操作性。 |
| D3 | 条件 REST 响应与历史兼容方案 | 默认仅 JVM，显式 `target=python`；投票前核对生成的 schema 和旧客户端样例，必要时选择独立 Python 路由。 |
| D4 | 工具支持矩阵和可接受采样开销 | 管理员预装 py-spy，以一个明确 Linux 环境的真实结果为起点，分别评审默认/native 模式的覆盖与开销。 |

## Appendix A: Memory capture experiment

该实验不属于本轮公共接口或交付承诺。其目的在于验证：无需修改 UDF，能否在指定窗口内获得有用的分配栈，
且关闭采集后原 worker 继续工作。尚不新增内存 REST、UI、集中调度或公开 Flink 配置。

进程外采样与进程内采集应按采集逻辑所在位置区分；`attach` 并不等同于进程外，Memray attach 会在目标内开启 Tracker。
本实验使用 Memray 公共 `Tracker` API，在现有 standalone worker 启动入口运行同一进程中的 Beam harness。
第一步提供 delay、duration、output directory、native traces 四项实验输入：例如启动后 120 秒开始，采集 60 秒，每次启动一次。
Tracker 由启动入口持有并关闭，记录实际开始/结束时间及完成状态；不通过杀死或重启 worker 停止采集。
计时结束仍依赖 Python 执行机会，只有工具确认关闭后的 `.bin` 才标为完整结果。

优先复用 Python 依赖准备和环境传递。待正式接入时建议共用 `rest.profiling.dir`，
但当前启动入口实验未读取该 Flink 配置，也不受 REST 会话限额、历史清理和下载管理。
实验阶段直接指定目录并按既有部署方式取回文件，离线生成 Memray 报告。
Memray/Scalene 的共用逻辑应由实际接入证明后再提取，不预先设计 SPI，也不为此新增任意代码执行接口。

分配栈需要结合 RSS/PSS 和采集窗口解读；原生栈开关增加 C/C++ 栈，不代表关闭时就不记录原生分配。
实验不能解释全部容器内存或 GPU 显存，也不保证 OOM/SIGKILL 后得到完整报告。
验证覆盖已知 Python/原生分配、延迟窗口、关闭后持续处理、文件大小及吞吐影响；从选定的小规模诊断作业开始。
依赖和目录准备失败应保留普通启动路径，但不能承诺原生跟踪故障完全不影响目标进程。
[Memray Tracker API][memray-api] · [Native mode][memray-native]

Beam 2.75.0 的容器 profiler_agent 已有 Memray 包装，但属于 PyFlink 当前未经过的 Go boot 路径，
其定时停止会中断并重启 SDK worker。本方案借鉴准备和产物处理，不复制这种停止策略。
Dask 的运行中 Tracker 接入说明未来可研究既有控制链路，但不构成首版的运行期协议承诺。

本地实验现已接入 `beam_boot`：使用四项 `PYFLINK_MEMRAY_*` 环境变量，单个后台线程持有 Tracker，
正常退出时取消或结束采集；仅成功关闭的 `.bin.inprogress` 改名为 `.bin`。14 项单元测试通过。
Linux standalone 上的立即采集、延迟 native 采集均能定位已知分配，关闭后同一 worker 继续处理。
部署方式和边界见[内存实验说明](Memory-experiment.md)。

## Appendix B: Prototype status and references

当前本地实现的上游基线为 `8d01380ff5bf9892462ef9808b1b56fd9723f54d`。
本次修订已实现公共字段收缩、默认 JVM 历史与显式 Python 筛选、错误映射、严格参数类型校验、
零样本失败判定和结果目录生命周期测试。Python 响应省略 JVM 专用模式字段，与生成的可选字段 schema 对齐。

2026-11-07 在本地 Linux AArch64 容器启动 standalone JM/TM，运行真实 PyFlink worker。
环境为 CPython 3.11.18、Beam 2.75.0、Java 17.0.21、py-spy 0.4.3。
普通与 native 采样均生成真实火焰图；普通报告包含 Python 预处理函数和压缩调用入口，
native 报告进一步显示 zlib 的 `deflate`。下载字节与 TM 配置目录中的文件一致，JVM ITIMER 采集也成功。
页面触发采集已验证并截图；截图检查发现的 Target/Mode 列宽问题已修复。
后续 review 修复将 Python 采集的取消和完成等待接入所属 TM 的退出流程，保留共享 profiling service；
页面创建成功后复用当前目标的历史刷新，避免迟到响应混入其他目标。

| Area | Evidence |
| --- | --- |
| 后端、协议与生命周期 | 69 个 runtime 测试通过，包含 TaskExecutor、MiniCluster REST/RPC/下载、TM 退出清理、迟到查询和明确错误状态。 |
| 页面与接口调用 | 37 个前端测试通过，涵盖切换目标后的迟到创建响应和重复记录；生产构建及相关 lint 检查通过。 |
| 兼容性与生成文档 | REST 稳定性快照通过；相对上游基线的 83 个已有调用保持原有字段和类型。配置与 REST/OpenAPI 文档由项目生成器更新。 |
| 真实工具 | Linux AArch64 + py-spy 0.4.3 的普通/native 采样成功；0.4.1 wheel 的 native 不支持错误被正确记录为 FAILED。 |
| 尚未覆盖 | 完整仓库 `clean verify`、更多平台/CPython 组合、Cython 快路径、Torch/YOLO 负载和受控开销基准。Memray 仍为独立实验。 |

证据、截图和复现输入见[standalone 验证报告](../../../output/playwright/pyflink-20261107/run.md)。
这是功能验证，不将样本比例当作精确 CPU 时间，也不据此承诺生产开销。
原型的 50 Hz、5 秒超时宽限、20 MiB 图文件、1 MiB 工具日志及 4 KiB 错误摘要仍为内部实现参数。
历史原型记录见 [README](README.md#evidence-and-limitations)。

- [原型 CPU 架构图](architecture.png)及 [draw.io 源文件](architecture.drawio)，用于说明复用关系；公共接口以当前正文为准。
- [cProfile 与 py-spy 选型研究](Profiler-selection-research.md)。
- [分布式 Python 框架对照](Distributed-framework-profiling-research.md)，含 Spark、Ray、Dask、Beam/Dataflow、PyTorch 与 Celery。
- [Ray 2.60.0 / Memray 1.20.0 源码研究](Ray-profiling-research.md)。
- VVR 参考仓库 `/Users/kenken/ali/review/flink`，核对基线 `1.20-vvr-11.10-SNAPSHOT`、`d4d3f87aed6f8397505c71961ad8d0a57792d311`；只借鉴 worker 级采样，不移植其整套控制链路。
- [FLINK-39153](https://issues.apache.org/jira/browse/FLINK-39153) 及[指标草案](https://docs.google.com/document/d/1ovs29Pup8bRpV4qY8wI611nYV55zf771tJXQksPwehw/edit) 是独立指标方向，不作为本提案前置条件。
- [py-spy 行为参考](https://github.com/benfred/py-spy/blob/v0.4.2/README.md)，发布支持仍以实际验证版本为准。

[flip-template]: https://cwiki.apache.org/confluence/spaces/FLINK/pages/65145553/FLIP+Template
[flip-375]: https://cwiki.apache.org/confluence/spaces/FLINK/pages/272927211/FLIP-375%2BBuilt-in%2Bcross-platform%2Bpowerful%2Bjava%2Bprofiler
[memray-api]: https://bloomberg.github.io/memray/api.html
[memray-native]: https://bloomberg.github.io/memray/native_mode.html
[pytorch-profiling]: https://docs.pytorch.org/tutorials/recipes/recipes/profiler_recipe.html
