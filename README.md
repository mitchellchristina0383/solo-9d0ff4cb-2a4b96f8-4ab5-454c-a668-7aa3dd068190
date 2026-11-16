# Dependency Alert API

值班工程师面对一串服务故障告警时，需要先处理真正的**上游根因故障**，同时保留尚未恢复的
下游告警但予以抑制。本服务用 **FastAPI + SQLite（WAL）** 实现：

- 整版提交服务依赖图（`A` 依赖 `B`，则 `B` 是 `A` 的上游）；
- 带**每服务递增正整数序号**上报故障 / 恢复，支持幂等重试、冲突拒绝、迟到不回退；
- 查询时动态计算**需处理故障**与**被抑制故障**及其上游根因；
- 每条需处理告警带**阶段标识**，值班工程师提交阶段标识记录处置确认；过期阶段被拒绝，
  恢复后再次故障、或被上游故障抑制后重新成为需处理时都产生新阶段；
- 替换依赖图时按服务名保留状态，恢复后下游自动重新归类，无需重报；
- 所有写操作走序列化事务，并发上报 / 换版 / 查询 / 确认不会混合不同已提交状态。

## 运行

### Docker Compose（推荐）

```bash
docker compose up --build
# API:   http://localhost:8000
# 文档:  http://localhost:8000/docs   （Swagger UI，可直接操作）
# 健康:  http://localhost:8000/health
```

SQLite 数据文件保存在命名卷 `alert-data`（容器内 `/data/alerts.db`）。

### 本地直接运行

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
ALERT_DB_PATH=./data/alerts.db uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/graph` | 整版替换依赖图。校验失败（环路 / 未知引用 / 重名 / 重边 / 自依赖）返回 `422` 并**保留旧版** |
| `GET` | `/graph` | 读取当前已提交图（含内部修订号 `revision`） |
| `POST` | `/reports` | 上报故障 / 恢复 |
| `GET` | `/alerts` | 查询告警分类（需处理 / 被抑制 / 根因 / 路径 / 阶段标识） |
| `POST` | `/acknowledgements` | 按阶段标识记录处置确认（过期阶段 `409 stale_phase`） |
| `POST` | `/admin/reset` | 清空全部数据（测试 / 运维辅助） |
| `GET` | `/health` | 健康检查 |

### 提交依赖图

```json
POST /graph
{
  "version": "v1",
  "services": [
    {"name": "A", "dependencies": ["B", "C"]},
    {"name": "B", "dependencies": ["D"]},
    {"name": "C", "dependencies": ["D"]},
    {"name": "D", "dependencies": []}
  ]
}
```

`dependencies` 中列出的是**上游**服务；所有被引用服务必须在同一次提交中出现，
整图必须是 DAG（Kahn 算法检环）。每次成功提交 `revision` 递增。

### 上报故障 / 恢复

```json
POST /reports
{"service": "B", "seq": 7, "status": "fault"}
```

序号 `seq` 为**每个服务各自维护的严格递增正整数**，规则：

| 情况 | 结果 |
| --- | --- |
| `seq > last_seq` | 应用新状态 |
| `seq == last_seq` 且内容相同 | 幂等重试，`applied=false, duplicate=true`，状态不变 |
| `seq == last_seq` 但内容不同（fault/recover 互斥） | `409 conflict`，状态不变 |
| `seq < last_seq`（迟到上报） | `409 conflict`，**绝不回退状态** |
| 服务不在当前图中 | `404 unknown_service` |
| `seq <= 0` | `422`（请求体校验） |

序号允许有空洞（如 1 之后直接 3）。每次上报（含被拒绝的）都写入 `events` 审计表。

### 查询告警

```json
GET /alerts
{
  "graph_revision": 1,
  "graph_version": "v1",
  "services": ["A", "B", "C", "D"],
  "actionable_faults": [
    {"service": "D", "last_seq": 1, "phase": 1,
     "acknowledged": false, "acknowledged_at": null}
  ],
  "suppressed_faults": [
    {
      "service": "B",
      "last_seq": 1,
      "root_causes": ["D"],
      "paths": [["B", "D"]]
    }
  ],
  "down_services": ["B", "D"],
  "unreported_services": ["C"],
  "healthy_services": []
}
```

### 处置确认

```json
POST /acknowledgements
{"service": "D", "phase": 1}
→ {"acknowledged": true, "duplicate": false, "service": "D",
   "phase": 1, "acknowledged_at": "2026-11-16T21:20:00.000Z"}
```

工程师先 `GET /alerts` 读取某需处理告警的 `phase`，再把它提交回 `/acknowledgements`。
确认只绑定到**该阶段**，规则：

| 情况 | 结果 |
| --- | --- |
| `phase` 等于当前需处理阶段 | 记录确认（首次 `duplicate=false`） |
| 同一当前阶段重复确认 | 幂等，`duplicate=true`，保留首次确认时间 |
| `phase` 已过期（服务已恢复、已进入下一阶段） | `409 stale_phase`，不写确认 |
| 服务当前被抑制 / 健康 / 未上报 | `409 stale_phase`，不写确认 |
| 服务不在当前图中 | `404 unknown_service` |
| `phase <= 0` | `422`（请求体校验） |

### 阶段（phase）语义

- 阶段标识按服务单调递增，只增不减、绝不复用；服务每**重新进入**需处理集合
  （恢复后再次故障；仍故障但先被上游抑制、随后重新成为需处理；从图中移除又加回后再次
  需处理）都会分配**新的阶段号**，此前的确认不会延续。
- 服务持续处于需处理状态时，重复的同号上报（幂等重试）、以及**分类结果未变**的依赖图
  替换，都不推进阶段、不清除确认。
- 一条上报只有被真正应用（`applied=true`）时才可能改变分类，也才可能推进阶段；被拒绝
  或幂等的上报不推进阶段、不改写确认。
- 阶段的推进与上报 / 换版写在同一事务中；`/alerts` 从同一已提交快照读取阶段，因此上报、
  换图与确认交错时，返回的阶段、告警与确认始终对应同一已提交状态。写入失败则事务回滚，
  阶段与确认都不前进。

## 抑制规则（核心语义）

1. 只有当前仍处于故障（down）状态的服务才会出现在 `actionable_faults` 或
   `suppressed_faults` 中，两类互斥且并集恰为全部 down 服务。
2. **根因**：故障且没有任何故障上游的服务。根因永远是需处理故障。
3. 一个非根因故障被抑制，当且仅当存在一条到达某根因的依赖路径，且**路径上每个节点都
   处于故障中**。
4. 健康节点或**从未上报**的节点会截断路径：其下游即使故障也不能经由该路径被抑制；
   若所有路径都被截断，该故障仍归入需处理。
5. 分类是查询时根据当前图 + 当前状态**动态计算**的。服务恢复、依赖图换版后立即重新
   归类，不需要下游重新上报。

## 换版与状态保留

- 服务状态（up/down、最后序号）独立于图存储，按服务名为主键。换版后同名服务的状态与
  序号历史原样保留；被移除服务的状态行不删除但不会出现在查询中，将来重新加入时恢复可见。
- 新图中首次出现的服务归入 `unreported_services`。

## 并发模型

- SQLite 开启 WAL；每个写事务以 `BEGIN IMMEDIATE` 开始，写者全局串行化，遇
  `SQLITE_BUSY` 短时重试。
- 上报、换版与处置确认都在各自的写事务内先按**同一已提交快照**重新计算分类并同步阶段，
  再完成所选写入；因此阶段号与确认绝不会指向另一个状态下的告警。事务失败则整体回滚。
- 读事务持有单一已提交快照，因此换版与查询并发时，每次 `/alerts` 看到的节点集、边、
  修订号与阶段必然属于同一个已提交版本，不会出现混合状态。
- 每个请求使用独立的线程级连接，请求结束即关闭。

## 自测

`tests/selftest.py` 仅依赖标准库，对运行中的服务做端到端校验：

```bash
python tests/selftest.py                      # 默认 http://127.0.0.1:8000
BASE_URL=http://localhost:8000 python tests/selftest.py
# 容器内：docker compose exec alert-api python tests/selftest.py
```

`tests/test_phase_atomicity.py` 直接调用服务层（使用临时数据库），验证事务原子性：

```bash
python -I tests/test_phase_atomicity.py
```

`selftest.py` 覆盖：图校验与原子替换、序号幂等 / 冲突 / 迟到语义、全故障路径抑制与健康 /
未上报截断、恢复即时重分类、换版状态保留、阶段分配与处置确认（含恢复后再故障、抑制解除后
重新需处理、分类未变换版保留确认、过期阶段拒绝），以及同序号并发重试、同序号异内容竞争、
多序号乱序竞争、并发换版、上报 / 换图 / 确认交错等关键并发场景（共 120 项断言）。

`test_phase_atomicity.py` 覆盖：上报 / 换图 / 确认事务中注入写入失败后状态、阶段与确认均不前进，
过期确认不写库，旧数据库启动时阶段回填，以及幂等重试与分类未变换版不重置确认（共 33 项断言）。

## 目录结构

```
app/
  main.py            FastAPI 路由与异常映射
  database.py        SQLite 连接、schema、事务管理
  classification.py  已提交快照上的图加载与告警分类
  phase_service.py   阶段分配与处置确认
  graph_service.py   图校验、整版替换
  report_service.py  上报序号语义
  schemas.py         Pydantic 模型
  errors.py          领域错误
tests/selftest.py            端到端自测（标准库，HTTP）
tests/test_phase_atomicity.py 事务原子性自测（服务层，临时数据库）
Dockerfile, docker-compose.yml, requirements.txt
```
