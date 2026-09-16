# EdgeFaultLab

**面向分布式 Edge / IoT 系统的确定性故障注入与韧性测试工具。**

EdgeFaultLab 位于系统节点之间，注入的是**语义级故障**——丢掉一条
`CONTROL_COMMAND`、重复一条命令、让时间戳变旧、让一个网关死掉——然后检查系统
是否守住了它承诺的不变量。没守住就以非零退出码结束，可以直接卡住 CI。

```text
Scenario -> Inject -> Observe -> Assert -> Report
```

```bash
pip install -e .
edgefaultlab validate scenarios/drop_message.json
edgefaultlab run scenarios/drop_message.json
```

Python 3.10+，零运行时依赖，不需要 root，不碰内核网络。

---

## 它是什么

一个"懂协议"的代理，加一个会下判断的断言引擎。你描述两个节点之间的一条链路、
命中部分流量的一个故障、以及系统必须保持的不变量；EdgeFaultLab 启动系统、注入
故障、记录它看到的每一件事，最后告诉你哪些不变量活了下来。

```text
      节点 A                          节点 B
   (gateway A1)                   (controller C1)
         |                                ^
         v                                |
   +-------------------------------------------+
   |              EdgeFaultLab                 |
   |  Scenario Runner -> Fault Engine          |
   |  TCP proxy -> Event trace -> Assertions   |
   +-------------------------------------------+
```

## 为什么需要它

任何项目都能证明"一切正常时系统能跑"。真正决定 Edge / IoT 部署能不能活下来的是
另一类问题：

* 消息**丢了**会怎样？
* 消息**重复了**会怎样？
* 消息**晚到了**会怎样？
* Gateway **执行到一半死掉**会怎样？
* Server 离线期间系统还能不能工作？
* 恢复之后会不会出现**双主**或**过期 Owner**？
* 同一条 actuator 命令会不会被**执行两次**？
* 执行结果丢失后，系统会不会**重新启动执行器**？

EdgeFaultLab 的价值是把这些问题变成机器可以回答的问题：让"我们处理了重复消息"
从设计文档里的一句话变成一条测试结论。

## 与通用网络故障代理的区别

Toxiproxy、`tc`/`netem`、包级 chaos 工具在本职工作上非常强：延迟、带宽、连接
重置、泛化的丢包。EdgeFaultLab 不是它们的替代品，也不打算是。它工作的层次更高，
用的是系统自己的词汇：

| 通用网络故障代理 | EdgeFaultLab |
| --- | --- |
| 丢 *N% 的数据包* | 丢掉 *下一条 `A1` 发往 `C1` 的 `CONTROL_COMMAND`* |
| 复制*字节* | 复制*一条命令，并保持 `message_id` 与 payload 不变* |
| 给*一个 socket* 加延迟 | 延迟*所有 `CONTROL_RESULT`* |
| 报告*吞吐和错误* | 报告*断言 PASS / FAIL 与退出码* |
| 问"字节到了吗？" | 问"`cmd-001` 是不是只被执行了一次？" |

最后一行是重点。代理告诉你网络做了什么，EdgeFaultLab 告诉你系统是否还守得住
自己的不变量。

## 架构

```text
             EdgeFaultLab

      +---------------------+
      |   Scenario Runner   |
      +----------+----------+
                 |
       +---------+---------+
       |         |         |
       v         v         v
    Process    Fault     Assertion
    Manager    Engine     Engine
       |         |         |
       +----+----+----+----+
            |         |
            v         v
        Event Trace   Report
```

| 模块 | 职责 |
| --- | --- |
| `scenario.py` | 解析与校验 scenario JSON；唯一决定"scenario 能说什么"的地方 |
| `runner.py` | 一次运行：起进程、开链路、按时刻注入故障、收尾、判定、出报告 |
| `proxy.py` | TCP 上的 newline JSON；转发，并执行故障引擎给出的决定 |
| `faults.py` | 故障生命周期与逐条决策，可复现（按 seed） |
| `processes.py` | 只启动 / 终止 / 重启它自己启动过的子进程 |
| `assertions.py` | 运行结束后针对"真正送达的消息"做的 5 种检查 |
| `matcher.py` | 全部查询能力：`field == value`，外加点号路径 |
| `recorder.py` | 事件轨迹（`events.jsonl`）与运行计数 |
| `report.py` | 控制台输出、`summary.json`、`report.md`、恢复时间 |
| `cli.py` | `edgefaultlab validate` / `edgefaultlab run` |

## 故障类型

| 动作 | 行为 | 想回答的问题 |
| --- | --- | --- |
| `drop` | 丢弃命中的消息 | 发送方会重试吗？ |
| `delay` | 扣住消息，之后再发 | 谁会超时？ |
| `duplicate` | 再发一份同样的消息（`message_id` 与 payload 原样保留） | 消费方幂等吗？ |
| `reorder` | 缓冲接下来的若干条命中消息并倒序发出 | 顺序重要吗？ |
| `timestamp_offset` | 给已存在的 `timestamp` 字段加偏移 | 过期命令会被拒绝吗？ |
| `disconnect` | 断开该链路当前已建立的 TCP 连接 | 节点多快重连？ |
| `link_down` | 让一条链路在 N 秒内不可用 | 系统扛得住中断吗？ |
| `process_kill` | 终止 EdgeFaultLab 自己启动的进程 | 谁来接管？ |
| `process_restart` | 用同样的 command / cwd / env 重新启动它 | 能恢复吗？ |

```json
{"at": 5.0, "action": "drop", "link": "gateway_to_controller",
 "match": {"type": "CONTROL_COMMAND"}, "count": 1}
```

故障按消息匹配，可以带 `count`（只作用于最前面的 N 条）或 `probability`
（按比例）。概率一定在该故障自己的、由 seed 派生的随机源上掷骰子，绝不使用全局
随机状态。

## Scenario 文件

用 JSON，因为它不带来依赖，而且任何语言都能生成。以 `_` 开头的键视为注释；
进程的 `cwd` 相对于 scenario 文件解析。

```json
{
  "_note": "the first CONTROL_COMMAND never reaches the controller",
  "name": "lost control command",
  "seed": 42,
  "duration": 20,

  "links": [
    {"name": "gateway_to_controller",
     "listen": "127.0.0.1:9501",
     "upstream": "127.0.0.1:9601"}
  ],

  "processes": [
    {"name": "gateway_a1",
     "command": ["python", "gateway.py", "--id", "A1"],
     "cwd": "../target-project"}
  ],

  "faults": [
    {"at": 5.0, "action": "drop", "link": "gateway_to_controller",
     "match": {"type": "CONTROL_COMMAND"}, "count": 1}
  ],

  "assertions": [
    {"assert": "eventually", "after": 5, "within": 15,
     "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"}},
    {"assert": "unique",
     "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
     "key": "payload.command_id"}
  ]
}
```

`edgefaultlab validate` 会在开跑之前一次性检查全部问题：未知链路、未声明的进程、
拼错的 action、拼错的字段、缺少必填字段、故障时刻晚于运行时长。
EdgeFaultLab **不硬编码任何消息 schema**：它只解析 JSON 对象，并匹配你写出来的
字段。

## 断言

断言只看"真正到达链路另一侧"的消息。被丢弃的消息从未送达，因此不能满足断言；
被延迟的消息在它真正到达的时刻被计入。

| `assert` | 含义 | 字段 |
| --- | --- | --- |
| `message_count` | 命中的消息有多少条 | `equals` / `min` / `max` |
| `never` | 命中消息一条都不能出现 | `match` |
| `eventually` | 在时间窗内必须出现命中消息 | `after`、`within` |
| `unique` | 某个 key 不能出现两次 | `key`（如 `payload.command_id`） |
| `sequence` | 若干过滤条件必须按顺序成立 | `steps` |

当一个 scenario 有多条链路时，同一条消息会不止一次穿过代理。断言可以指定观察点：

```json
{"assert": "unique", "link": "producer_to_relay", "direction": "reverse",
 "match": {"type": "CONTROL_RESULT", "payload.status": "EXECUTED"},
 "key": "payload.command_id"}
```

不写选择器时，所有链路上的每一次送达都会被计入。

## 快速开始

```bash
git clone https://github.com/Sver0411/EdgeFaultLab && cd EdgeFaultLab
python3 -m venv .venv && . .venv/bin/activate
pip install -e .

edgefaultlab validate scenarios/drop_message.json   # 只校验，不运行
edgefaultlab run scenarios/drop_message.json        # 运行
edgefaultlab run scenarios/drop_message.json --output runs/test-001
edgefaultlab run scenarios/drop_message.json --seed 123
```

退出码：`0` 全部断言通过；`1` 有断言失败；`2` 运行本身无法完成（端口被占用、
进程起不来、缓冲区溢出）。

## Demo

仓库自带一个三进程小系统 `examples/demo_system/`，作为微缩的 Edge / IoT 部署，
不依赖任何其它项目：

```text
producer (B1)  ->  relay (A1)  ->  consumer (C1)

producer: 发送 CONTROL_COMMAND，没收到 ACK 就重试
relay:    转发 newline JSON，上游断了会重连
consumer: 只执行一次，拒绝重复的 command_id 和过期的命令
```

```text
$ edgefaultlab run scenarios/drop_message.json

EdgeFaultLab 0.1.0

Scenario : drop message
Seed     : 42
Duration : 20s

000.015  process started      consumer pid=9967
000.024  proxy started        producer_to_relay
001.613  message received     producer_to_relay msg-0001
001.614  fault activated      relay_to_consumer fault=drop-first-command
001.616  message dropped      relay_to_consumer msg-0001 fault=drop-first-command
003.114  message received     producer_to_relay msg-0001     (重试)
003.518  message forwarded    producer_to_relay control_result-cmd-0001

Assertions
PASS  dropped command is eventually delivered and executed
PASS  no command_id is executed twice
PASS  the system keeps working after the drop

Messages
received    : 38
forwarded   : 37
dropped     : 1

Result
PASS
```

自带的 4 个 demo scenario：

| Scenario | 故障 | 验证内容 |
| --- | --- | --- |
| `scenarios/drop_message.json` | 第一条 `CONTROL_COMMAND` 被丢 | 重试、最终执行、不重复执行 |
| `scenarios/duplicate_command.json` | 第一条 `CONTROL_COMMAND` 被复制 | 同一个 `command_id` 只执行一次 |
| `scenarios/delay_result.json` | 第一条 `CONTROL_RESULT` 延迟 2.5s | 迟到但送达；`COMMAND -> ACK -> RESULT` 顺序成立 |
| `scenarios/process_crash.json` | 6s 杀掉 relay，14s 重启 | 链路恢复，且没有任何东西执行两次 |

## Smart Agriculture 集成示例

`examples/smart_agriculture/` 里有 5 个面向真实多节点系统（Server、A1/A2 网关、
传感器、控制器）的 scenario：网关故障接管、服务器离线、命令重复、命令过期、命令
丢失。它们启动那个仓库自己的入口，不复制它的任何源码；你只需要把 `cwd` 改成自己
的 checkout 路径。端口布局的思路见
[examples/smart_agriculture/README.md](examples/smart_agriculture/README.md)。

## 输出与报告

```text
runs/<run_id>/
  events.jsonl     全部事件，一行一个 JSON，时间相对运行起点
  summary.json     计数、断言结果、PID、恢复时间——给 CI 用
  report.md        故障时间线、恢复、断言、结论——给人看
  logs/<name>.log  运行启动的每个进程的 stdout/stderr
```

记录的事件包括 `PROCESS_START/EXIT/KILL/RESTART`、`CONNECTION_OPEN/CLOSE`、
`MESSAGE_RECEIVED/FORWARDED/DROPPED/DELAYED/DUPLICATED/REORDERED/MUTATED`、
`MALFORMED_MESSAGE`、`MESSAGE_TOO_LARGE`，故障生命周期
`FAULT_SCHEDULED/ACTIVATED/COMPLETED`、每条被故障命中的送达，以及运行结束时每条
断言对应的一条 `ASSERTION_PASS` / `ASSERTION_FAIL`。消息体超过 16 KB 会被截断并
标记 `truncated: true`，一次运行不可能写满磁盘。

**恢复时间（recovery time）**：从破坏性故障（`drop`、`disconnect`、`link_down`、
`process_kill`）发生，到系统重新出现生命迹象之间的距离——也就是之后第一条满足
某个 `eventually` 断言的消息。

## 确定性

```json
{"action": "drop", "probability": 0.3, "match": {"type": "CONTROL_COMMAND"}}
```

同一个 scenario、同一个 seed、同样的消息顺序，会产生同样的故障决策。每个故障持有
自己的 `random.Random`，种子来自 `(seed, fault id)`；全局随机状态从不被触碰，
所以进程里其它任何事情都不会改变某个决策。

seed **不保证**的是：整次运行逐毫秒一致。进程调度、TCP 时序、重传，以及被测系统
自己的行为，仍然由操作系统决定。EdgeFaultLab 保证的是**它自己的决策**可复现，
而不是整场运行可以完全复刻——事件轨迹每一行都带时间戳，正是因为时序仍在变化。

## 安全边界

* **只绑定回环地址。** 链路绑定 `127.0.0.1` / `::1` / `localhost`；绑定外部网卡
  不是 v0.1 的功能，`validate` 会直接拒绝。
* **只能杀自己的子进程。** `process_kill` / `process_restart` 只能引用 scenario
  里声明过的进程名；格式里没有 `pid` 字段，写了会被校验拒绝。
* **不做系统级网络控制。** v0.1 从不调用 `tc`、`netem`、`iptables`、`pfctl`、
  防火墙或 root network namespace。
* **不静默失败。** scenario 有问题、上游连不上、端口被占用、进程起不来、缓冲区
  溢出——全部显式报错并以非零退出码结束，没有 `except: pass`。

## 局限

EdgeFaultLab v0.1 测试的是系统的**软件可见故障语义**，它不是硬件、射频或电气测试
台。它不会：

* 注入真实 RF 干扰，或模拟 LoRa PHY；
* 破坏真实 IP 数据包，也**不使用** `tc` / `netem` / 抓包；
* 模拟 CPU 掉电（brownout）、Flash 损坏或断电；
* 测试电气故障；
* 替代硬件验证或现场测试；
* 复刻真实部署的毫秒级调度。

它要求被测系统通过 TCP + newline JSON 可达，并且只能对"经过它的链路"注入故障。
代理不实现 TLS 终止或协议感知的分帧，它复制的是换行分隔的行。进程管理在 macOS 和
Linux 上经过验证（CI 跑 `ubuntu-latest`）；Windows 分支使用
`CTRL_BREAK_EVENT` / `terminate()`，属于尽力而为。

## 没有实现的

v0.1 明确不做：Web Dashboard、Grafana / Prometheus、Docker、Kubernetes、
Chaos Mesh、MQTT broker、LoRa / CAN / BLE / 串口模拟、UDP、QUIC、gRPC、
HTTP 代理、抓包与 PCAP、内核或 root 网络操作、分布式 agent、云服务、鉴权、
数据库、插件市场，以及任何与 LLM 或故障预测相关的东西。

v0.1 只做：本地、TCP、newline JSON、进程、故障、断言、报告。

## 目录与测试

```text
edgefaultlab/                10 个源文件，只用标准库
scenarios/                   4 个可运行的 demo scenario + 1 个模板
examples/demo_system/        三节点 demo 系统
examples/smart_agriculture/  5 个面向另一个仓库的集成 scenario
tests/                       39 个测试，包含真正跑 TCP 的端到端测试
```

```bash
pip install -e ".[dev]"
pytest
```

## 许可证

MIT。
