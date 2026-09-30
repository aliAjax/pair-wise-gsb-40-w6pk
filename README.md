# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、线索、离线批次和时间线。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。

## 主要接口

写操作使用 JSON，并需要 `X-User` 与 `X-Role` 请求头。角色包括 `coordinator`、`operator`、`field`、`analyst`、`viewer`。

- `GET /health`、`GET /api/state`
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/assets`：登记资源
- `POST /api/areas`：创建搜索区域
- `POST /api/assignments`：按能力、海况和航程分配资源
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源并释放任务
- `POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/offline/batch`：幂等合并离线记录
- `POST /api/incidents/merge`：确认重复报警并回，区域、线索和资源分配并入主事件
- `POST /api/incidents/merge/revert`：撤销合并，把仍属于该报警的内容放回
- `GET /api/incidents/{id}/timeline`

## 重复报警合并流程

1. 创建同船相近报警时，新报警自动标记为 `duplicate`，并记录 `duplicate_of`；待确认期间仍可挂搜索区域、线索和资源。
2. `POST /api/incidents/merge`（`coordinator` 或 `operator`）携带 `duplicate_incident_id`、`client_batch_id` 和判断说明 `note`，可显式传 `main_incident_id`（默认取 `duplicate_of`）。确认后：
   - 重复报警下的搜索区域、线索整体转入主事件，`origin_incident_id` 保留原归属，原编号和时间线均可追溯；
   - 区域上的资源分配随区域一起并入，不释放资源；线索的人工核验结论保留，系统初判按主事件位置重算；
   - 重复报警置为 `merged`，两名值班员用不同批次同时确认时，靠条件更新保证只有一方成功（另一方 409）。
3. 合并按 `client_batch_id` 分批且可恢复：同批次重试只处理未并入项，已并入内容按明细跳过、不重复；完全失败时报警回到 `duplicate`，部分成功为 `partial`，全部完成为 `merged`。
4. `POST /api/incidents/merge/revert` 携带 `client_batch_id` 和 `reason`：只把当前仍挂在主事件下且属于该批次的区域和线索放回；已转到其他事件的内容标记 `detached` 且不改动。主事件已 `closed/cancelled` 时先转 `reported`（待处理）再放回；主事件自身也已被并走时，报警恢复为独立的 `reported`。撤销后需用新批次重新确认。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、离线幂等和权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
