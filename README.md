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
- `POST /api/incidents/merge-duplicate`：按批次确认重复报警，区域和线索并入主事件
- `POST /api/incidents/revert-merge`：按批次撤销合并，放回仍属于主事件的内容
- `POST /api/offline/batch`：幂等合并离线记录
- `GET /api/incidents/{id}/timeline`

### 重复报警合并流程

待确认的重复报警（`duplicate`）上可以继续创建搜索区域、记录线索和分配资源；
确认合并后才冻结（`merged`），原事件记录与编号保留，便于追溯。

- **确认合并** `POST /api/incidents/merge-duplicate`，参数
  `client_batch_id`（调用方生成的批次幂等键）、`duplicate_id`、`primary_id`。
  重复报警名下的全部搜索区域和线索整体改挂到主事件，资源分配随区域保留，
  并写入 `merge_batches` 批次与时间线。
- **并发**：状态翻转使用版本 CAS 并在 `BEGIN IMMEDIATE` 事务内执行，
  两名值班员同时确认时只有一方成功，另一方收到 409。
- **重试**：合并失败后用同一 `client_batch_id` 重放，返回首次结果
  （`idempotent: true`），已经并入的区域和线索不会重复移动；批次对象
  不一致或批次已撤销时返回 409。
- **撤销** `POST /api/incidents/revert-merge`，参数 `client_batch_id`、
  `reason`。只把批次记录内、此刻仍挂在主事件名下的区域和线索放回原报警；
  已转走或主事件自己新增的内容不受影响。区域已离开主事件时，相关线索
  放回并解除跨事件的区域引用。主事件若已关闭/取消，先转 `reported`
  （待处理）再撤销。撤销后批次不可重放，需要重新发起确认。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、离线幂等和权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
