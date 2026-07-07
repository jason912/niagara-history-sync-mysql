# Niagara History Sync v3.3

将 Niagara N4 站点历史数据（通过 oBIX 协议）复制到 MySQL。

---

## 目录

- [快速开始](#快速开始)
- [完整操作命令](#完整操作命令)
- [增量同步](#增量同步)
- [多站轮流同步](#多站轮流同步)
- [v3.3 新功能](#v33-新功能)
- [MySQL 表结构](#mysql-表结构)
- [常见问题](#常见问题)
- [环境要求](#环境要求)

---

## 快速开始

```bash
# 1. 安装依赖（仅 mysql2 一个外部包）
npm install mysql2

# 2. 交互式配置（输入站点地址、oBIX 账号密码、MySQL 信息）
node niagara-sync.js init

# 3. 探测站点，自动发现历史文件夹
node niagara-sync.js probe

# 4. 开始同步
node niagara-sync.js sync
```

---

## 完整操作命令

### `init` — 初始化配置

```bash
node niagara-sync.js init
```
交互式填写站点和数据库信息，生成 `niagara-sync.json`。

也可以一次性传入：
```bash
node niagara-sync.js init --pass Obix12345678 --db-pass 你的MySQL密码
```

### `probe` — 探测站点

```bash
node niagara-sync.js probe
```
自动连接站点，发现可用的历史文件夹并填入配置文件。

### `list` — 列出可同步的历史点

```bash
node niagara-sync.js list           # 列出全部
node niagara-sync.js list 温度      # 只列出名字包含"温度"的点
node niagara-sync.js list CO2       # 只列出名字包含"CO2"的点
```

### `sync` — 开始同步（核心命令）

```bash
# 完整同步所有历史点（默认并发 4 个）
node niagara-sync.js sync

# 只同步名字包含关键字的点
node niagara-sync.js sync --filter CO2

# 指定并发数（最大 8）
node niagara-sync.js sync --parallel 6

# 顺序模式：一次只同步一个点（慢但省资源）
node niagara-sync.js sync --serial

# 只同步最近 24 小时的数据
node niagara-sync.js sync --since 2026-07-06T11:00:00

# 限定时间范围
node niagara-sync.js sync --since 2026-07-01T00:00:00 --until 2026-07-07T00:00:00

# 预览模式：只查不算，不写入数据库
node niagara-sync.js sync --dry-run

# JSON 格式输出（适合程序调用）
node niagara-sync.js sync --json
```

### `status` — 查看同步状态

```bash
node niagara-sync.js status
```
显示 MySQL 中每个点已同步了多少条记录、时间范围、以及断点续传信息。

### `config` — 查看当前配置

```bash
node niagara-sync.js config
node niagara-sync.js config --json   # JSON 格式输出
```

---

## 增量同步

**是的，这是增量同步。** 工具通过 MySQL 中的 `_sync_state` 表实现断点续传：

1. 第一次 sync 是全量同步
2. 每次同步完，工具会记录每个点最后同步到的**时间戳**
3. 第二次及之后运行，只拉取**上次同步之后新增**的数据

所以你可以放心设个定时任务每天跑一次，每次都只同步新增的记录。

---
## 多站轮流同步

⚠️ 当前版本一次只支持**一个站点**。如果需要同步多个 Niagara 站，有两种方式：

### 方式一：手动切换配置（推荐）

```bash
# 准备多个配置文件
copy niagara-sync.json   niagara-sync.144.json   # 站点 144 的配置
copy niagara-sync.json   niagara-sync.146.json   # 站点 146 的配置
# 编辑每个文件里的 source.host 和 source.user 等

# 轮流同步
copy niagara-sync.144.json niagara-sync.json && node niagara-sync.js sync
copy niagara-sync.146.json niagara-sync.json && node niagara-sync.js sync
```

### 方式二：写个批处理脚本

新建 `sync-all-stations.bat`：
```bat
@echo off
echo === Syncing Station 144 ===
copy /Y niagara-sync.144.json niagara-sync.json
node niagara-sync.js sync

echo === Syncing Station 146 ===
copy /Y niagara-sync.146.json niagara-sync.json
node niagara-sync.js sync

echo === All done! ===
```

### 关于多站数据存放

- **不同站的数据存到不同的 MySQL 数据库**：在配置里改 `target.database`
- **或者存到同一数据库**：工具会自动为每个点创建单独的表，不同站的点名通常不同，不会冲突
- **不同站的点名可能重复**：建议不同站用不同的数据库，或手动区分

---

## v3.3 新功能

**默认并发同步。** 多个历史点同时读取和写入（默认 4 个 worker）。每个点独立进行 oBIX 查询和 MySQL 写入，互不干扰。

顺序模式（一次一个点）：`node niagara-sync.js sync --serial`

---

## MySQL 表结构

每个历史点创建一张表，表名 = 去除特殊字符后的点名称。

### 数据表

| 列名 | 类型 | 说明 |
|---|---|---|
| id | BIGINT AUTO_INCREMENT | 主键 |
| ts | DATETIME(3) | 时间戳（Asia/Shanghai，精确到毫秒） |
| value | DECIMAL(10,3) | 数值（3 位小数），非数值类型为 NULL |
| raw | TEXT | 原始字符串值（非数值类型时使用） |

### 同步状态表 `_sync_state`

记录每个点的同步断点，用于增量同步。

---

## 常见问题

### Q：支持多个 station 轮流同步吗？
当前不支持一个命令同步多个站。详见 [多站轮流同步](#多站轮流同步) 的手动方案。

### Q：是增量同步还是全量同步？
**增量同步。** 每次只拉取上次同步之后的新数据。第一次运行是全量。

### Q：Node.js 最低版本要求？
**Node.js >= 10**。代码使用 async/await、Buffer、ES6 标准特性，不依赖任何实验性 API。
（README 之前写 >= 12 是保守说法，实际 10 以上就能跑。）

### Q：需要安装什么依赖？
只需要一个外部包：`npm install mysql2`。oBIX 通信全部使用 Node.js 内置的 `http` 和 `crypto` 模块。

### Q：同步过程中断了怎么办？
重新跑 `sync` 即可。工具会从 `_sync_state` 记录的断点继续，不会重复同步已有数据。

### Q：同步速度慢怎么办？
- 增加并发数：`--parallel 8`
- 减少单次查询上限：在 `niagara-sync.json` 中调小 `sync.limit`（默认 100000）
- 缩小时间范围：用 `--since` 限制

---

## 环境要求

| 依赖 | 版本 | 说明 |
|---|---|---|
| Node.js | >= 10 | 仅使用内置模块（http、crypto、fs、path） |
| mysql2 | 最新版 | `npm install mysql2` — 唯一的外部包 |

无其他依赖。不需要数据库驱动、编译工具或运行时库。
