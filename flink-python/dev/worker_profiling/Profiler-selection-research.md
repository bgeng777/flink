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

# cProfile 与 py-spy：PyFlink 选型研究

调研日期为 2026-11-06。采用官方文档、项目源码、GitHub API，并补充本地 cProfile 小实验。
实验未运行真实 PyFlink/PyTorch 作业，也未完成两种工具的同环境性能对照。
结论中的“成熟、被广泛集成”是基于下面的多个独立项目，不代表测得了市场占有率或活跃用户数。

## 结论

cProfile 是有长期历史、被交互式开发工具、IDE 和分布式计算框架实际集成的 profiler。
不能因为个人未使用过它，就认为它冷门或已经淘汰。对 PyFlink 最有参考价值的例子是
PySpark 4.0.1，其 Python UDF profiling 基于 cProfile，并已有 flameprof 可视化路径。
这使“增强现有 cProfile 的导出和可视化”成为有实际先例的方案，而不只是理论备选。
[PySpark 源码][spark-basic] · [PySpark 可视化实现][spark-sql]

py-spy 同样具有实际工程采用和持续维护证据。Ray 将其集成为 Dashboard 的按需 CPU/栈查看后端，
Apache Airflow 的性能分析工具也调用 py-spy。它的采用场景更贴近“观察已经运行的 Python 进程”。
这些采用事实支持工具成熟度，但不能证明在我们的多模态作业里采样开销可接受或原生线程覆盖完整。
[Ray 源码][ray-manager] · [Airflow 工具][airflow-kit]

## cProfile 的直接使用证据

| 项目 | 实际采用方式 | 证据等级与对选型的含义 |
| --- | --- | --- |
| CPython | Python 2.5 引入 cProfile，2.5 发布于 2006-09-19。Python 3.11 文档推荐多数用户使用 cProfile，强调其为 C 实现、开销合理。 | 官方历史与推荐，证明长期标准库地位，不是独立用户数。[历史][python-history] · [3.11 文档][python-311] |
| IPython / IPython kernel | `%prun` / `%%prun` 背后的实现明确 `import cProfile as profile`，实例化 `Profile()` 并通过 `runctx()` 执行，支持返回 pstats 和保存统计文件。 | 官方命令加真实实现。用户可能通过 Notebook/IPython 使用 cProfile，而没有直接写 `import cProfile`。[命令][ipython-doc] · [实现][ipython-source] |
| PyCharm | 官方文档将 profiler 选择顺序列为 vmprof、yappi、cProfile。没有前两个额外后端时使用标准 cProfile，支持打开 CProfile snapshot。 | IDE 正式集成；不能说所有 PyCharm profiling 都用 cProfile。[后端选择][pycharm-profiler] · [报告查看][pycharm-report] |
| Spyder 6.0.7 | Profiler 插件通过 `python -m cProfile -o ...` 运行程序，再用 pstats 加载、比较结果。 | 固定版本源码，证明真实 GUI 集成。[实现][spyder-source] · [使用说明][spyder-doc] |
| PySpark 4.0.1 | `BasicProfiler` 明确基于 cProfile + Accumulator，`UDFBasicProfiler` 继承它。Session UDF profiler 支持 `spark.sql.pyspark.udf.profiler=perf`，结果可按 UDF ID 展示、清除和渲染。 | 与 PyFlink 很接近的分布式 Python worker 场景。[采集实现][spark-basic] · [使用说明][spark-doc] |
| Apache Airflow | 性能分析工具里的 `profiled()` context manager 在代码段周围启停 cProfile、输出 pstats。 | 开发/性能分析脚本采用，不能描述为 Airflow 运维 UI 内置功能。[固定提交源码][airflow-kit] |
| Ray | 官方性能优化指南另有 cProfile 调用示例，用于分析应用逻辑和 Ray API 调用。 | 官方使用指导；应与 Dashboard 的 py-spy 内置集成区分。[Ray cProfile 指南][ray-cprofile] |

IPython 文档检索时为 9.18.1，源码取 main；PyCharm 文档当时标为 2026.3 Help，因此以上为查询时现状。
Spyder、Spark 和 Airflow 采用固定 tag/commit，便于复核。

## 一个直接影响 PyFlink 选型的事实

PySpark 4.0.1 的 `spark.profile.render(id=..., type="perf")` 默认 renderer 为 `flameprof`。
实现直接把 `pstats.Stats.stats` 交给 flameprof 生成 SVG。在 IPython 中返回 HTML 展示，其他环境返回 SVG 源字符串。
因此“cProfile 只能看日志，想要火焰图必须换成 py-spy”是不成立的。
不过，这证明的是保存和渲染路径可行，并不改变 cProfile 的数据采集范围。
[PySpark 渲染实现][spark-sql] · [Spark 4.0.1 文档][spark-doc]

## py-spy 的直接使用和维护证据

| 项目或渠道 | 核实内容 | 可以得出的结论 |
| --- | --- | --- |
| Ray 2.60.0 | 固定提交的 `CpuProfilingManager` 调用 `py-spy dump` 和 `record`，Dashboard 提供 worker 进程栈与 CPU 火焰图入口。 | 是正式集成的后端，不只是推荐教程。[源码][ray-manager] · [文档][ray-profiling] |
| Ray 安装包 | Ray 2.60.0 的相关额外依赖包含 py-spy，针对 Python 版本设置最低工具版本。 | 项目承担了一部分工具分发/兼容性处理。[setup.py][ray-setup] |
| Apache Airflow | 性能分析工具的采样 context manager 调用 `py-spy record --idle`，scheduler 性能脚本也能启动 py-spy。相同工具文件还提供 cProfile。 | 独立项目的真实工程使用，两类 profiler 在该工具中并存。[工具][airflow-kit] · [scheduler 脚本][airflow-scheduler] |
| py-spy 发布记录 | GitHub API 返回最新发布 v0.4.3，发布时间 2026-11-02；此前 v0.4.2 为 2026-04-24，v0.4.1 为 2025-07-31。 | 有近期发布与兼容性维护，不能继续将 v0.4.2 称为最新版。[v0.4.3][pyspy-release] · [发布 API][pyspy-releases] |

v0.4.3 的发布说明包括 Python 3.15 支持、Windows ARM64 wheels、线程名与栈解析修复等。
这证明它持续跟进 CPython/平台变化，同时也说明外部读取 CPython 内存布局具有版本维护成本。
发布说明不是对特定 PyFlink 组合的兼容性或开销保证。[发布说明][pyspy-release]

## Python 官方推荐需要标注版本

Python 3.11 文档推荐 cProfile 给多数用户，这条引用适合描述当时的标准库选择。
当前查询到的 Python 3.15 文档已经把统计采样推荐给大多数性能分析工作，将确定性跟踪定位于精确调用次数、
极短函数等需求；cProfile 继续作为 `profiling.tracing` 的兼容别名保留。弃用的是纯 Python `profile` 模块，
不能混同为 cProfile 已弃用。[3.11 文档][python-311] · [3.15 文档][python-315]

对我们的含义是，不应拿旧版的一句默认推荐替代场景分析，也不应因为新版存在新的标准库采样器就扩大本轮范围。
先围绕实际支持的 Python 版本、目标进程状态和部署条件比较现有两个候选。

## 使用规模证据的边界

- 未发现能够公平比较两者活跃用户数或生产部署数的权威统计。本稿不作市场份额排名。
- cProfile 随 CPython 分发，没有与 py-spy 相同口径的独立包安装次数或 GitHub star。
- 项目集成和官方指南可以证明工具有实际采用；不能由此推导每个项目有多少用户真正启用 profiling。
- Ray 选用 py-spy 是很接近 Flink 的线上进程观察案例；Spark 选用 cProfile 是很接近 Flink 的 UDF 性能采集案例。
  两个参照分别支持不同使用方式，应与我们希望交付的诊断入口对应。

[python-history]: https://docs.python.org/3.11/whatsnew/2.5.html
[python-311]: https://docs.python.org/3.11/library/profile.html
[python-315]: https://docs.python.org/3/library/profiling.html
[ipython-doc]: https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-prun
[ipython-source]: https://github.com/ipython/ipython/blob/main/IPython/core/magics/execution.py
[pycharm-profiler]: https://www.jetbrains.com/help/pycharm/profiler.html
[pycharm-report]: https://www.jetbrains.com/help/pycharm/read-the-profiling-report.html
[spyder-source]: https://github.com/spyder-ide/spyder/blob/v6.0.7/spyder/plugins/profiler/widgets/main_widget.py
[spyder-doc]: https://docs.spyder-ide.org/current/panes/profiler.html
[spark-basic]: https://github.com/apache/spark/blob/v4.0.1/python/pyspark/profiler.py
[spark-sql]: https://github.com/apache/spark/blob/v4.0.1/python/pyspark/sql/profiler.py
[spark-doc]: https://spark.apache.org/docs/4.0.1/api/python/development/debugging.html#identifying-hot-loops-python-profilers
[ray-manager]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py
[ray-setup]: https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/setup.py
[ray-profiling]: https://docs.ray.io/en/latest/ray-observability/user-guides/profiling.html
[ray-cprofile]: https://docs.ray.io/en/latest/ray-observability/user-guides/debug-apps/optimize-performance.html#profiling-using-python-s-cprofile
[airflow-kit]: https://github.com/apache/airflow/blob/c2ff56eff6b1b6c0733170c35ea65f131e8ce59c/devel-common/src/tests_common/test_utils/perf/perf_kit/python.py
[airflow-scheduler]: https://github.com/apache/airflow/blob/c2ff56eff6b1b6c0733170c35ea65f131e8ce59c/dev/airflow_perf/scheduler_dag_execution_timing.py
[pyspy-release]: https://github.com/benfred/py-spy/releases/tag/v0.4.3
[pyspy-releases]: https://api.github.com/repos/benfred/py-spy/releases?per_page=3
[pyspy-native-owner]: https://github.com/benfred/py-spy/issues/332#issuecomment-752839934
[pyspy-torch-issue]: https://github.com/benfred/py-spy/issues/872

## 能力比较与 PyFlink 影响

比较对象是 CPython cProfile 和 py-spy。cProfile 的本地语义实验采用 Python 3.11.13；源码另核对 3.12.12。
py-spy 主体源码核对到 v0.4.3，不能把此前原型参考的 v0.4.2 或本机安装的 v0.4.1 当成最新版本。
本地 Flink 基线依然是 `6fea6ccc349` 原型所基于的代码，没有重构生产代码。

| 维度 | cProfile | py-spy | 对本方案的意义 |
| --- | --- | --- | --- |
| 采集机制 | 进程内跟踪函数调用与返回，统计次数及耗时 | 进程外读取解释器状态，周期性采样调用栈 | 调用计数与样本占比提供不同证据，不能互换数值。 |
| 时间含义 | 当前默认 timer 是 elapsed/perf counter，可能包含等待，可指定其他 timer | 栈样本权重，默认尝试排除 idle 线程，可显式包含 idle | 两者都不能不加解释地当成精确进程 CPU accounting。 |
| 临时定位运行中 worker | 程序必须已有合作式启用点；当前 Flink 是预配置开关和 bundle 生命周期 | 可以按 PID attach，指定时长后停止；需版本、权限匹配 | 在线诊断是 py-spy 更直接的使用方式。 |
| 函数细节 | 保留被追踪范围内的调用次数、自身时间、累计时间与 caller 关系 | 保留采样时刻的完整可见栈，短调用可能漏采，不提供精确调用次数 | 高频短函数和算法调用次数分析适合 cProfile；热点分布适合采样。 |
| Python 线程范围 | 本地 CPython 3.11 默认绑定启用的线程；3.12 实现已换用 sys.monitoring，须按版本核对 | 遍历解释器的 PyThreadState，适合观察多个 Python 线程 | 不应将所有版本的 cProfile 简化为同一线程语义。 |
| C/C++ 与 Cython | 可统计部分扩展调用入口的耗时，不展开完整原生栈 | native 模式可合并 Python 线程对应的原生栈，依赖平台和符号 | 两者都不能宣称完整覆盖纯 C++ 线程池或 GPU。 |
| 可视化 | pstats 可用 flameprof/SnakeViz 等展示；聚合 caller/callee 数据不总能唯一重建上下文 | 直接生成 SVG 或输出 speedscope 等采样数据 | 可视化不是选择 py-spy 的独有理由。 |
| 开销 | 与被追踪的事件频率有关，需计入 Flink 打印/导出成本 | 与采样率、线程规模、栈解析等有关，默认会短暂停住进程 | 两者均需代表性负载验证。 |
| 部署 | 标准库内置，无跨进程读取权限需求，渲染可以离线进行 | 额外工具、解释器布局兼容性、容器/PID namespace/ptrace 权限 | 受限托管环境中，cProfile 可能更容易落地。 |
| 内存 | 无分配字节数与分配/释放轨迹 | 无内存分配跟踪 | CPU 选型不替代独立 Memray 路线。 |
| Flink 接入成本 | 导出已有统计较小；若要运行中开关、独立窗口、全线程管理仍需额外设计 | 需要目标发现、工具执行和结果接入；本地原型已有相关实现 | 应按相同用户需求比较，避免把“导出文件”与“完整按需 profiling”混为同等范围。 |

上述机制依据 [CPython 3.11 _lsprof.c][cpy311]、[CPython 3.12 _lsprof.c][cpy312]、
[Python 3.11 profile 文档][py311-doc]、[py-spy v0.4.3 README][pyspy-readme] 和
[py-spy 线程采样实现][pyspy-threads]。图形重建限制见 [SnakeViz notes][snakeviz]。

### 当前 Flink cProfile 接入的确切范围

- `operation.open()` 先于 Profiler 构造与 enable，因而该采集范围不覆盖本 operation 的 open，例如在 open 中加载模型。
- bundle start 调用 enable，finish 完成后 disable 并 `pstats.Stats(...).sort_stats('cumulative').print_stats()`。
- 对象重用时没有清零，结果跨 bundle 累计。finish 不返回时，这条输出路径不会产生该次统计。
- 当前没有通过这一配置导出可下载的 pstats/SVG，也没有运行时临时开关入口。官方用户文档只说明到日志查看。

这些是 Flink 当前接入的限制，可以部分通过改进接入解决，并非 cProfile 本身不能做独立窗口或文件导出。
[Profiler 实现](../../pyflink/fn_execution/profiler.py) ·
[Python operation 生命周期](../../pyflink/fn_execution/beam/beam_operations_slow.py) ·
[Cython operation 生命周期](../../pyflink/fn_execution/beam/beam_operations_fast.pyx) ·
[Flink 用户文档](../../docs/user_guide/debugging.rst)

### 多模态负载必须单独验证的 py-spy 边界

v0.4.3 的 `python_spy.rs` 219–359 行只沿 PyThreadState 链遍历 Python 线程，在 321–330 行为这些线程合并 native 栈。
该路径不会为没有 PyThreadState 的纯原生工作线程生成栈。因此，能显示 Python 调用进入 PyTorch 的 native 路径，
不代表能够解释 OpenMP 等原生线程池的全部 CPU 消耗。该结论同时得到维护者早期回复的支持，现状判断以当前源码为准。
[源码][pyspy-threads] · [维护者说明][pyspy-native-issue]

v0.4.3 默认使用 LockingStrategy::Lock，读取前会暂停目标。`--nonblocking` 避免采样阶段暂停，
但可能增加错误或不完整栈，并且与 `--native` 互斥。外部采样不等于无干扰。
[默认与参数互斥][pyspy-config] · [nonblocking 说明][pyspy-readme]

2026-09-20 的 [py-spy #872][pyspy-torch-report] 是用户对 v0.4.2、192 核 Linux/PyTorch 大线程池的严重减速报告。
截至本次查询该 issue 仍 open，未有维护者回复。本报告未复现其性能倍率，也不推断 v0.4.3 有相同表现。
它仅用于确定我们的必要验收负载需要包含实际 torch 线程规模；不能拿它证明所有 py-spy 部署都不适合生产。

## 公开 CPU 开销对照与选型权重

OSDI 2023 的 Scalene 论文 Table 3 给出 cProfile 与 py-spy 的同环境对照。
以下为执行耗时相对无 profiler 的倍率，1.73× 表示耗时增加 73%，不是增加 173%。

| 工作负载 | cProfile | py-spy |
| --- | --- | --- |
| pprint | 2.63× | 1.03× |
| sympy | 2.11× | 1.02× |
| raytrace | 1.98× | 1.00× |
| 全部十项的中位数 | 1.73× | 1.02× |

实验为 Linux、8 核 Ryzen 7、CPython 3.10.9，选用 pyperformance 中十个较长基准，
每项扩展到至少 10 秒，重复十次后取四分位间均值，再汇总各基准结果。
这些是论文作者的历史实测，非当前 PyFlink/Torch 的保证值。论文未锁定 py-spy 精确版本。
[论文 §6.1、§6.4、Table 3][overhead-paper]

同期作者脚本留有 `py-spy record -o /tmp/profile.svg -- python3` 与
`python3 -m cProfile -o /dev/null`，未显式指定 rate/native/nonblocking。
它读取目标程序打印的 elapsed 时间，因而不代表采样器额外 CPU 为零。
该提交只启用部分 profiler 且循环次数与论文不同，只能补充命令和计时口径，不能当完整复现包。
[作者脚本][overhead-script] · [目标循环计时][overhead-workload]

这组结果支持“cProfile 在部分程序上使耗时达到约两倍或更高，而普通 py-spy 采样通常显著更低扰动”。
其原因有源码依据：cProfile 为调用/返回等事件执行计时和记账，成本随事件频率增长；
py-spy 按采样周期抓栈，成本主要随采样频率、线程规模和每次抓栈代价增长。
[调用事件实现][overhead-cprofile] · [采样循环][overhead-pyspy]

据此，本方案应明确把低扰动作为在线诊断的优先指标，CPU 主路径优先 py-spy。
已有 cProfile 适合保留给受控函数分析或受限部署，不能因其可视化改动较小就忽略逐调用跟踪成本。
Torch 大线程池与 native 的开销仍属具体环境验收项，不应以个别例外掩盖常规采样的低开销优势。

## 本地语义与开销小实验

环境为 macOS arm64、CPython 3.11.13。两个合成 CPU 负载分别是大量短 Python 函数调用，
以及一次长时间的 OpenSSL PBKDF2 原生调用。每项先将 baseline 校准到约 150 ms，预热一对，
再做七对交替 baseline/cProfile 测量。计时不包括报告渲染和 print_stats。
这只用于说明负载敏感性，不代表 Flink、PyTorch、YOLO 或 Linux 上的开销，也不能用于给 py-spy 排名。

| 负载 | baseline 中位数 | cProfile 中位数 | 配对耗时倍率中位数 |
| --- | --- | --- | --- |
| 短 Python 函数调用 | 142.51 ms | 410.06 ms | 2.89× |
| 单次长原生调用 | 125.36 ms | 124.24 ms | 0.99× |

原生负载的约 0.99× 属于本实验的测量波动，不能解释成 cProfile 加速了程序。
它说明 cProfile 的成本受 Python 调用事件频率影响，不能用一个固定百分比描述所有作业。
原生负载的结果只显示 `_hashlib.pbkdf2_hmac` 等入口，未展开 OpenSSL 内部调用链。

七对原始 wall-time 比例如下，便于观察波动。

| 轮次 | Python 调用负载 | 原生调用负载 |
| --- | --- | --- |
| 1 | 2.929× | 1.000× |
| 2 | 2.926× | 0.985× |
| 3 | 2.889× | 0.988× |
| 4 | 2.931× | 0.979× |
| 5 | 2.872× | 1.003× |
| 6 | 2.875× | 1.011× |
| 7 | 2.870× | 0.954× |

其他观察来自同一脚本。

- sleep 的 profile 耗时为 53.55 ms，进程 CPU 增量约 0.083 ms，说明默认计时包含等待。
- 在主线程启用 cProfile 时，新 Python 线程的目标函数未出现在结果中。此观察仅适用于本次 CPython 3.11 环境。
- 同一 Profiler 两次 enable/disable，每次调用函数一次，统计次数依次为 1、2，印证跨窗口累计而未自动清零。

本机 py-spy 为 0.4.1，macOS 采样需要额外权限且本机构建不支持 native。本轮未更改权限或安装新工具，
没有取得可用于与 cProfile 对照的 py-spy 开销数据。现有 PyFlink 原型的 macOS 接入证据也仅证明登记和失败传播。

<details>
<summary>复现实验脚本（Python 3.11，仅标准库）</summary>

保存为 `cprofile_probe.py` 后运行。脚本在所在目录写入原始 JSON 与两份 `.prof`。

```python
import cProfile
import hashlib
import io
import json
import platform
import pstats
import statistics
import sys
import threading
import time
from pathlib import Path

OUT = Path(__file__).parent

def leaf(value):
    return (value * 17 + 31) & 1023

def python_calls(iterations):
    result = 0
    for value in range(iterations):
        result += leaf(value)
    return result

def native_call(iterations):
    return hashlib.pbkdf2_hmac('sha256', b'profiling-input', b'fixed-salt', iterations, 32)

def timed(function, iterations, profiled):
    profiler = cProfile.Profile() if profiled else None
    start_cpu = time.process_time()
    start_wall = time.perf_counter()
    if profiler:
        profiler.enable()
    result = function(iterations)
    if profiler:
        profiler.disable()
    return {
        'wall_seconds': time.perf_counter() - start_wall,
        'cpu_seconds': time.process_time() - start_cpu,
    }, profiler, result

report = {
    'python': sys.version,
    'platform': platform.platform(),
    'method': 'Two synthetic CPU workloads; calibrate baseline to roughly 150 ms, one warmup pair, seven measured alternating baseline/profile pairs; rendering and print_stats excluded.',
    'workloads': {},
}
for name, function, initial, maximum in (
        ('python_call_heavy', python_calls, 200_000, 5_000_000),
        ('native_call_heavy', native_call, 50_000, 2_000_000)):
    calibration, _, _ = timed(function, initial, False)
    iterations = max(1, min(maximum, int(initial * 0.15 / calibration['wall_seconds'])))
    timed(function, iterations, False)
    timed(function, iterations, True)
    pairs = []
    last_profiler = None
    for index in range(7):
        pair = {}
        order = (False, True) if index % 2 == 0 else (True, False)
        for enabled in order:
            result, profiler, checksum = timed(function, iterations, enabled)
            pair['profiled' if enabled else 'baseline'] = result
            if profiler:
                last_profiler = profiler
        pairs.append(pair)
    last_profiler.dump_stats(str(OUT / f'{name}.prof'))
    stats = pstats.Stats(last_profiler, stream=io.StringIO())
    top = sorted(stats.stats.items(), key=lambda entry: entry[1][3], reverse=True)[:5]
    entry = {
        'iterations': iterations,
        'pairs': pairs,
        'baseline_wall_median_seconds': statistics.median(p['baseline']['wall_seconds'] for p in pairs),
        'profiled_wall_median_seconds': statistics.median(p['profiled']['wall_seconds'] for p in pairs),
        'paired_wall_ratio_median': statistics.median(p['profiled']['wall_seconds'] / p['baseline']['wall_seconds'] for p in pairs),
        'top_functions': [{'function': key[2], 'calls': value[1], 'cumulative_seconds': value[3]} for key, value in top],
    }
    report['workloads'][name] = entry

# Show that elapsed call time can mostly be waiting rather than consumed CPU.
profiler = cProfile.Profile()
start_cpu = time.process_time()
profiler.enable()
time.sleep(0.05)
profiler.disable()
stats = pstats.Stats(profiler, stream=io.StringIO())
report['wait_probe'] = {
    'process_cpu_seconds': time.process_time() - start_cpu,
    'sleep_profile_seconds': next(value[3] for key, value in stats.stats.items() if 'time.sleep' in key[2]),
}

# This observation applies to this CPython 3.11 run, not all Python versions.
def worker_thread_task():
    return python_calls(10_000)
profiler = cProfile.Profile()
profiler.enable()
thread = threading.Thread(target=worker_thread_task)
thread.start()
thread.join()
profiler.disable()
stats = pstats.Stats(profiler, stream=io.StringIO())
report['thread_probe'] = {'worker_function_in_main_thread_profile': any(key[2] == 'worker_thread_task' for key in stats.stats)}

# Mirror the current Flink enable/disable/print_stats lifetime without printing.
profiler = cProfile.Profile()
counts = []
for _ in range(2):
    profiler.enable()
    leaf(1)
    profiler.disable()
    stats = pstats.Stats(profiler, stream=io.StringIO())
    counts.append(next(value[1] for key, value in stats.stats.items() if key[2] == 'leaf'))
report['reused_profiler_call_counts'] = counts
(OUT / 'cprofile-probe-results.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({
    'python': sys.version.split()[0],
    'platform': platform.platform(),
    'workloads': {key: {field: value[field] for field in ('iterations', 'baseline_wall_median_seconds', 'profiled_wall_median_seconds', 'paired_wall_ratio_median', 'top_functions')} for key, value in report['workloads'].items()},
    'wait_probe': report['wait_probe'],
    'thread_probe': report['thread_probe'],
    'reused_profiler_call_counts': counts,
}, indent=2))
```

</details>

## 选型建议

1. 对本方案的运行中 worker 诊断目标，CPU 主路径优先 py-spy。低扰动采样与按需启动是主要理由，
   公开基准支持其常规开销明显低于 cProfile。Linux 权限、版本和代表性负载验证用于确定可发布参数。
2. cProfile 的成熟度与可视化能力成立，适合受控函数分析、已有统计导出或禁止跨进程读取的环境。
   这些用途保留，但不因导出实现较小就将逐调用跟踪作为本轮在线 CPU 诊断的首选。
3. 对 torch/YOLO，不能以“py-spy 有 native”替代线程覆盖和性能验证。首轮用同一个实际作业比较可见热点和吞吐/延迟，
   确认足以指导预处理、数据转换、用户函数等优化，再确定首版后端。
4. 本轮不同时建设两套完整控制后端，不增加自动降级链路。现有 cProfile 保留，内存诊断继续单独验证。

[cpy311]: https://github.com/python/cpython/blob/v3.11.14/Modules/_lsprof.c#L112-L120
[cpy312]: https://github.com/python/cpython/blob/v3.12.12/Modules/_lsprof.c#L710-L788
[py311-doc]: https://docs.python.org/3.11/library/profile.html
[pyspy-readme]: https://github.com/benfred/py-spy/blob/v0.4.3/README.md
[pyspy-threads]: https://github.com/benfred/py-spy/blob/v0.4.3/src/python_spy.rs#L219-L359
[pyspy-config]: https://github.com/benfred/py-spy/blob/v0.4.3/src/config.rs#L478-L484
[pyspy-native-issue]: https://github.com/benfred/py-spy/issues/332#issuecomment-752839934
[pyspy-torch-report]: https://github.com/benfred/py-spy/issues/872
[snakeviz]: https://jiffyclub.github.io/snakeviz/#notes

[overhead-paper]: https://www.usenix.org/system/files/osdi23-berger.pdf#page=10
[overhead-script]: https://github.com/plasma-umass/scalene/blob/5a8d2be356088a16885cac53a1e1eeef90ff1e47/benchmarks/new_benchmark.py
[overhead-workload]: https://github.com/plasma-umass/scalene/blob/5a8d2be356088a16885cac53a1e1eeef90ff1e47/test/expensive_benchmarks/bm_mdp.py
[overhead-cprofile]: https://github.com/python/cpython/blob/v3.11.14/Modules/_lsprof.c#L406-L455
[overhead-pyspy]: https://github.com/benfred/py-spy/blob/v0.4.3/src/sampler.rs#L64-L87
