# 数据字典

| 字段 | 类型 | 可空 | 定义 |
|---|---|---:|---|
| `run_id` | string | 否 | 输入摘要派生的稳定运行编号 |
| `source_line` | integer | 否 | 原文件物理行号 |
| `reason_code` | string | 是 | 隔离或冲突的稳定原因码 |
| `raw_value` | string | 是 | 未归一的原始证据 |
| `normalized_value` | string | 是 | 按项目规则得到的值 |
| `rule_version` | string | 否 | 计算规则版本 |

字符串编号逐字保留。金额与计量值用 `Decimal`。时间值带偏移并以 UTC 参与比较。区间统一采用左闭右开。

## M7 备份恢复字段

| 字段 | 类型 | 可空 | 定义 |
|---|---|---:|---|
| `receipt_id` | string | 否 | 一次实际入库的回执编号；未给出时由原始内容摘要派生 |
| `log_id` | string | 否 | 日志内 `request_id`；为空时由 `receipt_id` 与行号派生 |
| `seq` | integer | 否 | 实际入库写入顺序，同时作为恢复游标 |
| `state` | string | 否 | 日志状态：`CURRENT` 已入桶、`PENDING` 待采纳、`DEFERRED` 恢复期暂存 |
| `backup_id` | string | 否 | 备份时点编号，由成员 `seq` 与桶摘要派生，重复创建幂等 |
| `backup_seq` | integer | 否 | 备份包含的最大 `seq` |
| `restore_id` | string | 否 | 恢复编号 |
| `base_version` | integer | 否 | 发起恢复时的库版本 |
| `checkpoint` | integer | 否 | 已摊入暂存区的最大 `seq`；中断后从此续跑 |
| `processed` / `total` | integer | 否 | 已摊入成员数与备份成员总数 |
| `status` | string | 否 | `RECOVERING` / `EFFECTIVE` / `FAILED_SWITCH` / `STALE_VERSION` / `ABORTED` |
| `version` | integer | 否 | 每次成功切换加一；用于并发栅栏 |

回执表独立于日志与小时桶：整库恢复不会删除回执行；恢复只替换当前桶和日志状态。
