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

# Ray Python profiling 源码研究

核对日期为 2026-11-06。Ray 基线为当天最新正式版 `ray-2.60.0`，commit `ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d`，发布日期 2026-11-04。Memray 基线为 `v1.20.0`，commit `f70db8143c7601ca55b4b219e50577a3f06e38ea`。Ray 的 observability extra 对 Memray 未固定版本，因此下述 Memray 内部语义特指此版本。此次进行了源码、测试代码和上游文档核对，未运行 Ray 集群或实际 attach 实验。 [Ray release](https://github.com/ray-project/ray/releases/tag/ray-2.60.0) · [Ray dependencies](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/setup.py#L258-L279) · [Memray release](https://github.com/bloomberg/memray/releases/tag/v1.20.0)

Ray 的接入值得复用的部分是节点侧调用成熟工具、从运行对象定位 worker、提供小范围参数和现成报告。CPU 和内存的生命周期存在关键差异。py-spy 的记录进程承担采样，Memray attach 则在目标 Python 进程里启动 Tracker，attach 命令退出后仍继续记录。 [Ray managers](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py#L128-L377) · [Memray attach contract](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/docs/attach.rst#L22-L48)

**请求链路**

| 环节 | Ray 2.60.0 的实现 |
| --- | --- |
| 页面入口 | `ProfilingLink.tsx` 提供 CPU、Stack Trace、Memory 的参数对话框，提交按钮在新窗口打开 GET URL。CPU 可选 flamegraph/raw/speedscope，内存可选 flamegraph/table。 [UI](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/client/src/common/ProfilingLink.tsx#L319-L337) · [CPU/stack](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/client/src/common/ProfilingLink.tsx#L449-L505) · [memory](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/client/src/common/ProfilingLink.tsx#L593-L688) |
| 目标定位 | Task 入口提交 task ID、attempt number、node ID。Head 用 State API 取得 RUNNING task 的 worker PID/ID。Actor 入口使用已有的 PID/node ID。 [Task resolution](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py#L310-L351) · [Actor actions](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/client/src/components/ActorTable.tsx#L708-L723) |
| 跨节点调用 | Reporter Head 找对应 node 的 Reporter Agent，再发 `CpuProfiling`、`GetTraceback` 或 `MemoryProfiling` gRPC。RPC 传 PID 和选项，返回 success、output，内存另有 warning。 [Head CPU](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py#L515-L543) · [Protocol](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/src/ray/protobuf/reporter.proto#L21-L117) |
| 节点采集 | Agent 为请求创建 CpuProfilingManager 或 MemoryProfilingManager。manager 通过 `shutil.which` 在 Agent PATH 找工具，以 argv 调 `asyncio.create_subprocess_exec`。 [Agent](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_agent.py#L596-L664) · [Manager](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py#L90-L215) |
| 返回结果 | 请求等待采集及报告生成，Agent 读取整个产物作为 RPC output，Head 直接返回 SVG/HTML/文本。CPU task 的多任务提示对 SVG 放进 HTML，raw/speedscope 放响应头。 [Responses](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py#L116-L146) · [Worker CPU](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py#L716-L739) · [Memory](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py#L1119-L1133) |

Dashboard profiling 默认关闭。Head 提供 `/api/profiling_enabled` 返回开关及配置默认值，CPU 默认 5 秒，内存 10 秒，时长上限默认 60 秒且可由环境变量调整。native、idle、subprocesses、leaks、Python allocator tracing 默认关闭。这个接口返回配置与 head 平台提示，工具是否实际可用仍由目标节点执行时检查。 [Constants](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/_private/ray_constants.py#L184-L319) · [Head defaults](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py#L172-L215)

**py-spy 接入细节**

命令形状如下，native 参数仅在 Linux 分支追加，idle 和 subprocesses 按请求追加。CPU manager 没有显式设置 rate、threads、gil 或 nonblocking，采用安装版本默认行为。 [py-spy command construction](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py#L90-L215)

```text
py-spy dump -p <pid> [--native] [--subprocesses]
py-spy record -o <format>_<pid>_cpu_profiling.<svg-or-txt>
  -p <pid> -d <duration> -f <flamegraph|raw|speedscope>
  [--native] [--idle] [--subprocesses]
```

CPU 文件名只包含 format 和 PID。同 PID、同 format 的并发请求使用相同路径。manager 内没有会话 ID、per-PID 互斥、输出限额或产物淘汰，采集方法等待 `communicate()`，没有本方法内的超时/取消回收分支。这些是当前控制流的静态观察，尚未做并发故障复现。 [CPU manager](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py#L170-L215)

Ray 检测 `sudo -n true` 能否成功，成功时给 profiler 命令加 `sudo -n`。错误信息还会建议 setuid。Flink 现有方案采用管理员指定 executable 和显式部署权限，更容易把运行条件写清楚。 [Ray sudo handling](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py#L19-L87)

Task 采集前后都查询 task 是否 RUNNING，之后查该 worker 当前还有哪些任务，存在多个时提示整进程采样范围。代码注释明确将它定义为 best-effort overlap 判断。前后检查没有比较原 PID/worker ID/启动时间，worker 入口直接接受 PID，因而仍需保留我们已有的 registration handle 与启动身份校验。 [Task checks](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py#L523-L567) · [Worker target](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_head.py#L671-L725)

**Memray 接入细节**

Ray 的内存请求按如下顺序执行。原始文件放在 Agent 日志目录的 memray 子目录，名字为 PID 加秒级时间戳，转换后生成同名 HTML。leaks 是报告阶段的选项，native 与 trace-python-allocators 是采集阶段的选项。 [Memory manager](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/profile_manager.py#L218-L377) · [Agent lifecycle](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_agent.py#L641-L664)

```text
memray attach <pid> -o <capture.bin> [--native] [--trace-python-allocators]
agent await sleep(duration + 1)
memray detach <pid>
memray <flamegraph|table> -o <report.html> -f [--leaks] <capture.bin>
```

Ray attach 未传 `--duration`，时间由 Agent 协程控制。detach 失败时把错误放入 warning，仍尝试渲染文件。sleep、detach 和 render 未包在保证 detach 的 try/finally 中。若协程在 sleep 期间取消，这条代码路径没有执行 detach 的保证。CLI 子进程结束或 HTTP 客户端断连都不能证明目标 Tracker 已停止。 [Agent lifecycle](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/reporter_agent.py#L641-L664) · [Memray background contract](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/docs/attach.rst#L22-L48)

Memray 的几个语义必须进入 Flink 设计。

1. 目标进程需要能 `import memray`。Ray 在 Agent PATH 查到 memray，只证明命令存在。涉及不同 worker virtualenv/runtime env 时，需要单独满足目标环境依赖及工具兼容性。v1.20.0 的 `auto` 优先使用可用的 `sys.remote_exec`，然后使用平台相应的 gdb/lldb。源码说明 sys.remote_exec 需要 Python 3.14+，从控制器可用性选择后，实际目标支持仍可能执行失败。 [Package requirement](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/docs/attach.rst#L22-L28) · [Attach method selection](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/attach.py#L393-L416)
2. `attach -o --duration N` 会在注入成功后返回，目标进程通过 Python `threading.Timer` 定时调用停止。这个定时器依赖目标线程调度与解释器，时间已过去只能作为预计停止时刻。Timer 设置为非 daemon，也需覆盖目标正常退出是否等待定时器的行为。 [Timer](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/attach.py#L102-L131) · [Command return](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/attach.py#L550-L572)
3. 状态存在目标进程全局的 `memray._last_tracker`。再次 attach 会先停止旧 tracker，detach 停止当前 tracker，原生 CLI 没有 session token。晚到的 detach 可能停掉后来启动的采集，自动停止后的 detach 则报告没有先前 attach。Flink 必须明确采集所有权和停止确认协议。 [Tracker state](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/attach.py#L69-L131)
4. Tracker 销毁会写 trailer 并更新 header。FileReader 把物理 EOF 和 trailer 都视为 END_OF_FILE，所以文件可解析、HTML 生成成功不足以证明 writer 已关闭。停止确认应来自采集生命周期协议。 [Writer finalization](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/_memray/tracking_api.cpp#L929-L933) · [Reader EOF handling](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/_memray/record_reader.cpp#L706-L724)
5. trace-python-allocators 控制 pymalloc 分配粒度。leaks 展示采集窗口结束时未释放的记录，业务正常持有的对象也会出现。普通报告的默认归因是高水位快照。 [Allocator flag](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/attach.py#L484-L488) · [Report selection](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/common.py#L198-L211)
6. 上游文档明确说明 attach 会向目标注入代码，有崩溃或死锁的边界风险，并建议用于开发环境。因此应将 Memray 定为独立 opt-in 能力，先验证目标环境与原生扩展，再确定发布支持范围。 [Upstream caveats](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/docs/attach.rst#L80-L106)
7. 默认 Memray HTML 从 jsdelivr 加载 Bootstrap、Plotly、D3 等资源。`--no-web` 会把对应 vendored JS/CSS 嵌入 HTML，适合通过 Flink 现有 BLOB 下载后离线查看。Ray 的 render 命令未传此参数。 [HTML 模板](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/reporters/templates/base.html#L385-L397) · [Flamegraph 模板](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/reporters/templates/flamegraph.html#L95-L110) · [CLI option](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/flamegraph.py#L40-L44)
8. Memray HTML 可嵌入源代码。v1.20.0 的 flamegraph 支持 `--confidential-files=all`，保留函数名、文件名和行号，同时省略源码行。结果展示和下载须明确其内容边界。 [HTML option](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/flamegraph.py#L47-L58)

**对现有 PyFlink 设计的具体影响**

以下为基于上述代码的设计判断。

- CPU 继续使用现有 py-spy 链路，增加一次性线程栈 dump 能覆盖“先看 worker 卡在哪里”的入口。采样和 dump 共用目标身份及子进程回收机制。
- CPU、dump 和内存共享 Flink ProfilingService 的路由、历史、并发额度和 BLOB 下载，各自保留清楚的工具生命周期。现有异步提交、查询、下载比 Ray 的长请求直接返整份内容更适合已有 Flink 接口。
- Memray 的原始采集文件和 HTML 分属 capture/render 两步。RUNNING 应覆盖两步，停止成功且渲染成功后才发布 FINISHED。临时 bin、渲染输出和日志一起遵守会话清理与容量限制。
- 对 Memray 先验证“启动、唯一会话所有权、目标侧有界停止、停止确认、原始文件关闭”的完整链路。若用原生 attach CLI，需明确管理员独占工具的约束及停止确认缺口。若增加 worker 协作式 Tracker 包装，应把 session token、停止和完成标记集中在一个内部实现，并验证进程失联时的处理。
- 目标 worker 的解释器、Memray 可导入性和版本兼容性应成为 memory 能力的条件。能力信息随 worker 身份走，避免拿 head/控制器的平台或 PATH 代替 worker 条件。
- 保留现有唯一文件名和 worker 身份校验。记录整 worker 采样范围，多算子共享 worker 时展示共享上下文。算子归因需要更精确的关联证据。
- 内存界面先固定一种 HTML flamegraph 输出，采用 `--no-web --confidential-files=all` 生成离线报告并省略源代码行。native 与 pymalloc tracing 按真实诊断需求开放。leaks 可在后续作为“窗口结束仍存活”视图增加，避免首版组合膨胀。

Memray 的 debugger 注入由同步 `subprocess.check_output` 启动 gdb/lldb，其调用本身没有 timeout。停止外层 memray CLI 时，还需要验证 debugger 后代进程的退出及目标进程恢复执行状态。此处只确认了潜在清理缺口，未复现残留 debugger。 [Debugger execution](https://github.com/bloomberg/memray/blob/f70db8143c7601ca55b4b219e50577a3f06e38ea/src/memray/commands/attach.py#L135-L202)

Ray 已有真实 actor attach/detach、缺包、PID 不存在和渲染测试，也有端到端 task 内存页面检查。部分 manager 测试通过 mocks 检查 py-spy 参数，内存集成测试跳过 Windows/macOS。测试存在本身不证明我们的 PyFlink Linux/容器/Beam 环境已验证。后续最需要覆盖的是请求取消、TM/worker 退出、两次采集竞争、定时停止与显式停止竞争，以及采集停止后才渲染。 [Ray manager tests](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/tests/test_profile_manager.py#L38-L165) · [Ray end-to-end test](https://github.com/ray-project/ray/blob/ad7245ffb7d4c289e0d14cb2fd8c02af3c09433d/python/ray/dashboard/modules/reporter/tests/test_reporter.py#L1400-L1444)
