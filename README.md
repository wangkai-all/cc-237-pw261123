# 逐点交付接收系统（pointwise-delivery）

把**目录通知、文件观察、接收存储和下游领取**串成一条可审计链路的本地交付
接收页。纯 Python 标准库实现，无第三方依赖，所有运行数据均为生成文件。

## 要解决的问题

发送方的行为有三个棘手点，朴素的“等固定时间 / 看文件名和大小就判齐”都会出错：

1. **目录先到、内容晚到**：清单（目录通知）发布时，数据文件可能一个都没到；
2. **同名文件会被替换**：替换过程中文件可能只写了一半，且旧版本可能已被下游领取；
3. **目录会更正**：更正可能迟到，旧批次的更正绝不能覆盖正在发布的新批次。

## 核心设计与保证

### 判齐：不靠时间、不靠文件名大小

观察器每个扫描周期检查投递区，一个文件必须依次通过三关才算 `complete`：

| 关卡 | 不通过时的状态 | 说明 |
|---|---|---|
| 清单可解析且模式合法 | 清单 `writing` / `corrupt` | 清单经临时文件原子改名，读到的 JSON 要么完整要么还在写 |
| 大小与清单一致，且连续 2 个周期 `size+mtime_ns` 不变 | 文件 `writing` | 慢写的 `.xxx.writing` 临时文件也会被观察，页面能看到“写到一半：40000/200000 字节” |
| 实际 SHA-256 与清单声明一致 | 文件 `corrupt` | **大小相同但内容损坏在这里被拦住**，与 `writing` 是两种不同状态 |

因此“目录更正”（清单侧的 `stale`/`received`）和“文件写到一半”（文件侧
`writing`）在页面上是完全不同的状态。

### 来源可改写性决定入库方式

| 条件 | 方式 | 空间成本 | 完整性保证 |
|---|---|---|---|
| 清单 `mutable`，或 immutable 但来源仍带写位 | **独立复制** | = 文件大小 | 字节复制进对象存储，来源之后被改写/删除与接收方无关；入库前后两次 SHA-256 校验 |
| 清单 `immutable` 且来源真实只读且同文件系统 | **可靠引用（硬链接）** | ≈ 0（不新增数据块） | 只读位 + 内容寻址 + 哈希校验；发送方替换同名文件走“新 inode 再 rename”，旧 inode 被硬链接钉住不受影响 |
| 想硬链接但跨文件系统 | 自动降级独立复制 | = 文件大小 | 记录降级原因（`fallback_note`），仍做双重哈希 |

对象按哈希内容寻址（`store/objects/ab/cdef…`），天然去重；存储物理占用按
inode 去重统计，页面同时展示逻辑字节与物理字节。

### 同名替换与下游领取

- 观察器持续监督已判齐文件的来源 inode；检测到 inode 变化（同名替换）就
  新建一个版本行重新走完整判齐流程，旧版本标记 `superseded`。
- **新版本 complete 之前，发布指针不动**，期间领取仍拿到旧的完整版本。
- 发布是单事务内对 `publishes` 指针的原子替换；领取在同一把服务锁内读
  指针并打开对象。并发领取与替换相交时，每次领取只可能拿到旧完整版或新
  完整版，响应头 `X-Pointwise-Sha256` 与实际字节必然一致（测试中 30 路
  并发领取 + 指针切换，无一次不一致）。

### 目录更正与 epoch 围栏

- 每个批次维护单调递增的 `epoch`，只有当前 epoch 的清单能生效。
- 更旧 epoch 的清单（含迟到重投的旧通知）一律标记 `stale`，**不能覆盖
  当前目录，也没有可发布版本**，因此更正旧批次不可能冲掉正在发布的新批次。
- 撤回目录会删除该批次全部发布指针（下游领取返回 404）并标记 `withdrawn`，
  历史记录保留；后续更正目录到达后批次自动重新开放。

### 接收器停止 / 恢复

停止只把状态写进 SQLite（`meta.state=stopped`），观察器跳过扫描，已发布
内容和全部历史保留；恢复后从投递区现状补扫，不丢事件。

## 目录结构

```
pointwise/
  hashes.py     SHA-256 工具
  manifest.py   清单结构、模式校验、确定性内容生成
  sender.py     本地交付模拟器 CLI（init/write/corrupt/seal/correct/replace/...）
  watcher.py    观察器：稳定观察 + 哈希裁决 + copy/hardlink 决策 + epoch 围栏
  storage.py    内容寻址对象存储（复制 / 硬链接 / 跨设备降级 / 去重统计）
  models.py     SQLite 状态库（全部状态机、发布指针事务、事件流）
  service.py    领域服务：发布、撤回、停止恢复、下游领取
  server.py     ThreadingHTTPServer：静态网页 + JSON API + 领取流
web/            真实接收页（index.html / styles.css / app.js，1.5s 轮询）
scripts/        run.sh / reset.sh / demo.sh
tests/test_e2e.py  端到端测试（标准库，42 项断言）
runtime/        运行生成：dropzone/（投递区）、store/（对象）、receiver.db
```

## 使用

```bash
# 1. 启动接收服务（默认 http://127.0.0.1:8000/）
bash scripts/run.sh

# 2. 另开终端，用模拟器投递（--dropzone 指向运行目录的投递区）
DZ=runtime/dropzone
python3 -m pointwise sender --dropzone "$DZ" init --batch b \
    --mode mutable --files a.txt:4096 b.txt:1024
python3 -m pointwise sender --dropzone "$DZ" write --batch b --slow   # 慢写可见 writing
python3 -m pointwise sender --dropzone "$DZ" corrupt --batch b --name a.txt
python3 -m pointwise sender --dropzone "$DZ" correct --batch b --bump a.txt --add c.txt:512
python3 -m pointwise sender --dropzone "$DZ" replace --batch b --name a.txt

# 或一键跑完全部场景
bash scripts/demo.sh

# 3. 测试
python3 -I tests/test_e2e.py
```

## HTTP 接口

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/api/overview` | 全量视图：批次/清单/文件版本/发布指针/空间统计/事件流 |
| POST | `/api/files/<id>/publish` | 逐个发布（仅当前 epoch 的 complete 版本，否则 409） |
| POST | `/api/batches/<id>/withdraw` | 撤回目录（body 可带 `reason`） |
| POST | `/api/receiver/stop` · `/resume` | 停止 / 恢复接收器 |
| GET | `/api/files/<id>/claim` | 下游领取，响应头带 `X-Pointwise-Sha256/Epoch/Storage-Mode/Evidence-B64` |

页面上每个已发布文件都有“发布依据”按钮，展示哈希、epoch、存储方式、
入库额外空间、替换标记与发布时间；事件流记录每次清单到达、判齐、损坏、
stale 拦截、发布、替换、领取、撤回。
