# 林芯树轮年表 API

FastAPI + SQLite 的树轮年表服务：按树号、芯号报送逐年**整数**轮宽指数，按年份查询
参考值，并可在查询时**临时剔除**一棵待核树。

## 快速开始（Docker Compose）

```bash
docker compose up --build -d
open http://127.0.0.1:8000/docs        # Linux 用 xdg-open
docker compose logs -f api
docker compose down                    # 加 -v 连数据卷一起删
```

数据位于命名卷 `treering-data` 的 `/data/treering/chronology.sqlite3`（WAL 模式）。
服务以非 root 用户运行，`/health` 同时用作 Compose healthcheck。

本地不装 Docker 时可直接跑：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
TREERING_DB=./chronology.sqlite3 uvicorn app.main:app --port 8000
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/reports` | 报送/重报若干根芯的逐年指数，**整根替换**，单事务 |
| GET | `/chronology/{year}?exclude_tree=N` | 查参考值，可临时剔除树 N |
| POST | `/chronology/query` | 同上，JSON 请求体 `{"year":…, "exclude_tree":…}` |
| GET | `/cores/{tree}/{core}` | 查看单根芯当前已提交的序列和修订号 |
| GET | `/trees` | 已报送的树号 |
| GET | `/health` | 健康检查 |

报送示例（`years` 的键在 JSON 里是字符串，服务端转成整数年份）：

```bash
curl -X POST http://127.0.0.1:8000/reports -H 'Content-Type: application/json' -d '{
  "cores": [
    {"tree_no": 1, "core_no": 1, "years": {"1975": 90, "1976": 92}},
    {"tree_no": 1, "core_no": 2, "years": {"1975": 96, "1976": 94}}
  ]
}'
```

**整根替换语义**：同一 `(tree_no, core_no)` 再次报送时，旧序列整体丢弃；
新序列里没有的年份，该芯就退出那些年份的计算；新序列为空对象则该芯不再有任何年份数据（芯记录保留、修订号 +1，序列为空）。

## 计算规则

1. 当年样本树：该年至少有一根已报芯有值的树。剔除树只在该次查询中生效；
   若被剔除的树当年本来就没有数据，则不计入剔除（响应里 `excluded_tree_present=false`）。
2. 株内中位数：对该树当年**所有有值的芯**取中位数（偶数个芯时为两中值平均）。
3. 参考值：对所有样本树的株内中位数再取中位数（偶数棵时为两中值平均）。
4. 样本树数 = 剔除后实际参与计算的树数；≥ 3 才返回精确参考值
   （`status=ok`、`enough_samples=true`），否则 `status=insufficient_samples`、
   `value=null`，仍返回 `sample_trees` 供判断。
5. 全程用整数做精确运算：`value_x4` 是参考值的 4 倍整数，`value = value_x4/4`
   （粒度 0.25），避免浮点误差。

## 为什么查询和报送都是次线性的

写入时增量维护三张按年聚合的表（`tree_year`、`year_value_bit`、`year_stat`）：

* `tree_year(year, tree_no)` 存该树当年的株内中位数（二倍整数，保证偶数芯也精确）；
* `year_value_bit(year, node, cnt)` 是当年株内中位数在固定取值域上的**稀疏 Fenwick
  树（二进制索引树）**：中位值 v 存在 BIT 下标 v+1 处，节点 n 的 `cnt` 是落在区间
  `(n-lowbit(n), n]` 内的树数；计数归零的节点直接删除，因此表里只留有贡献的行；
* `year_stat(year, tree_count)` 存当年仍有样本树的数量；最后一棵树退出该年时整行删除。

### 查询（普通查询与剔除单株查询）

查询只做：

1. 按主键读 `year_stat` 一行；
2. 若带 `exclude_tree`，按主键读 `tree_year` 一行拿到该树中位数；
3. 在 Fenwick 树上用二进制提升做 1～2 次次序统计（中位数），**每次固定最多 32 次
   主键点查**，与当年样本树数、不同中位值数都无关；剔除的那棵树只在途经节点的
   `cnt` 上做 −1 修正，**不为任何树预存剔除后的参考值**。

所以一次查询读取的行数是常数级（`O(32)`，相对样本树数次线性），且查询连接由 SQLite
authorizer 限制为**只能读**上述聚合表，`measurement`/`core` 原始取芯表在查询连接上直接报
"access ... is prohibited"。节点上全部是主键/唯一索引 SEARCH（测试用
`EXPLAIN QUERY PLAN` 断言无 SCAN）。响应里的
`read_cost.rows_read / statements / slot_probes` 是本次查询实际读的行数等，可以直接观察。

### 报送（增量维护，不再重建整年分布）

一次报送只可能改变它涉及的 `(树, 年)` 的株内中位数。对每个这样的组合：

1. 只读这棵树自己当年的取芯明细（主键/索引点查，与该年有多少棵树无关），重算**这一棵
   树**的株内中位数；
2. 把该中位数的 old → new 差异作用到当年的 Fenwick 树：
   * 中位值首次出现 / 完全消失 = 一次 +1 / −1；
   * 中位值变化 = 先 −1 后 +1；
   每次更新沿 Fenwick 父节点链走，**最多 32 个节点**，稀疏节点按需插入/删除。

因此某年只有一棵树的取芯变动时，维护该年分布所扫描和改写的记录数上界为常数
（≤ 2×32 个 Fenwick 节点 + 1 个 `year_stat` 行），**不随该年样本树数线性增长**；
年内某个中位值首次出现或完全消失时同样如此。取芯明细、`tree_year`、
`year_value_bit`、`year_stat` 与 `meta.data_version` 在同一个 `BEGIN IMMEDIATE`
事务里提交，查询任何时刻只能看到一个完整的已提交状态。

实测（单年，全部树各 1 芯、中位值互不相同）：

| 样本树数 | 普通查询 rows_read / probes | 剔除查询 rows_read / probes | 单株重报改写的 Fenwick 节点数 |
| --- | --- | --- | --- |
| 100 | 66 / 64 | 35 / 32 | 61 |
| 1,000 | 66 / 64 | 35 / 32 | 58 |
| 10,000 | 66 / 64 | 35 / 32 | 59 |
| 50,000 | 66 / 64 | 35 / 32 | 58 |

读取量和单株报送的写入量都不随样本树数变化。

## 旧数据卷升级（只用已有聚合，失败可回滚）

旧版本卷里是累计直方图 `year_value_cum` 与带 `slot_count` 的 `year_stat`。服务在**第一次
写入前、同一个写事务内**自动升级：

* 只读取已提交的聚合（`tree_year` 和旧直方图），把每个不同中位值按其 `cnt` 重放进
  Fenwick 树，**不重新扫描 `measurement` 原始明细**；
* 重建当前形状的 `year_stat`，校验 Fenwick 根节点计数等于各年已提交树数，然后删除旧
  直方图并写入 `meta.schema_version=2`；
* 升级与本次报送一起 COMMIT；升级或报送任一步失败，整个事务 ROLLBACK，旧卷的表、
  数据和 `data_version` 原样保留，下次报送会重新尝试升级。

## 一致性与失败回滚

* 报送的取芯明细、株内中位数（`tree_year`）、年度分布（`year_value_bit` +
  `year_stat`）和版号 `meta.data_version` 在**同一个** `BEGIN IMMEDIATE` 事务里改完才
  COMMIT；失败（含校验失败、注入的异常、磁盘错误、旧卷升级失败）整体 ROLLBACK，旧数据和
  版号原样保留。
* 读连接使用只读 URI + 显式 `BEGIN`（deferred）读事务，一次请求只看到一个已提交快照；
  写事务进行中或提交瞬间都不会读到半更新的年表。
* WAL 模式允许读不阻塞写；`busy_timeout` + 指数退避只对 `SQLITE_BUSY` 重试。
* `meta.data_version` 每次成功报送 +1 并在响应中返回，可用来判断两次查询之间是否有写入；
  `meta.schema_version` 记录本卷的聚合结构代次（当前为 2）。

并发/回滚/快照一致性由 `tests/test_concurrency.py` 覆盖，增量维护成本与旧卷升级由
`tests/test_incremental.py` 覆盖。

## 测试与试跑

```bash
pip install -r requirements-dev.txt
pytest -q                                # 30 个用例
bash scripts/smoke.sh http://127.0.0.1:8000   # 需要已启动的服务
python scripts/demo.py http://127.0.0.1:8000  # 打印完整示例响应
```

测试覆盖：中位数（奇数/偶数、.5 精度）、逐树剔除、不足 3 树、整根替换丢年份、
被剔除树当年无数据、校验失败、注入异常回滚、并发报送后聚合与原始明细一致、
读期间快照版本一致、真实随机数据与暴力解对拍（含 2000 棵树、1–3 芯的大规模对拍）、
原始表在查询连接上不可读、只读表结构中没有“逐树剔除后答案”列、查询计划无 SCAN、
读取行数为常数级、**单株重报时分布维护的扫描/改写记录数不随样本树数增长（含中位值首次
出现与完全消失）**、**旧累计直方图卷仅用已有聚合同事务升级、升级失败保留已提交数据与版号**。

## 从 /docs 试查

1. 打开 `http://127.0.0.1:8000/docs`。
2. 展开 `POST /reports` → *Try it out*，粘贴上面的报送示例 → *Execute*（201）。
3. 展开 `GET /chronology/{year}` → year 填 `1975` → *Execute*，记下 `value`。
4. 同一接口 `exclude_tree` 填 `4` → *Execute*，对比 `sample_trees`、`value`、
   `excluded_tree_present` 和 `read_cost`（无论该年有多少树，`slot_probes` 都固定为 32）。
5. 再次 `POST /reports` 只报 `{"tree_no":3,"core_no":1,"years":{"1975":121}}`（缺 1976），
   然后查 `1976`：该芯 1976 的数据已被替换掉；这一步只增量改动树 3 涉及的年份，
   不重建任何一年的完整分布。
6. 把 `exclude_tree` 调到只剩 2 棵样本树，观察 `insufficient_samples`。
