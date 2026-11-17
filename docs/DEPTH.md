# 深度登记 多时区应用日志小时桶重算

        - 规模：中型
        - 核心规则：事件先归一到 UTC，DST 重叠时保留 fold；小时桶右端开，水位只能前进且迟到事件仍进入修订。
        - 使用者与工作流：导入随仓样例，逐模块复核，最后导出单一证据清单。

        ## 功能模块

        | 模块 | 使用者动作 | 入口 | 核心文件 | 测试 |
        |---|---|---|---|---|
        | M1 日志采信 | 运行样例并复核证据 | `/api/logs/import` / `/logs` | `app/modules/m1_logs/core.py` `app/modules/m1_logs/service.py` | `tests/test_m1_logs.py` |
| M2 时间解析 | 运行样例并复核证据 | `/api/time/normalize` / `/time` | `app/modules/m2_time/core.py` `app/modules/m2_time/service.py` | `tests/test_m2_time.py` |
| M3 事件去重 | 运行样例并复核证据 | `/api/events/dedupe` / `/events` | `app/modules/m3_events/core.py` `app/modules/m3_events/service.py` | `tests/test_m3_events.py` |
| M4 小时分桶 | 运行样例并复核证据 | `/api/buckets/rebuild` / `/buckets` | `app/modules/m4_buckets/core.py` `app/modules/m4_buckets/service.py` | `tests/test_m4_buckets.py` |
| M5 迟到修订 | 运行样例并复核证据 | `/api/watermark/advance` / `/watermark` | `app/modules/m5_watermark/core.py` `app/modules/m5_watermark/service.py` | `tests/test_m5_watermark.py` |
| M6 批次对账 | 运行样例并复核证据 | `/api/export/buckets.csv` / `/runs` | `app/modules/m6_runs/core.py` `app/modules/m6_runs/service.py` | `tests/test_m6_runs.py` |
| M7 备份恢复 | 从实际入库日志建备份，分段摊入暂存区，切换当前小时桶并回读三段范围 | `/api/backups` `/api/restores` `/restore` | `app/modules/m7_restore/core.py` `app/modules/m7_restore/service.py` `app/store.py` | `tests/test_m7_restore.py` |

        ## 陷阱登记

        | TRAP | 现象 | 朴素实现 | 本项目处理 | 处理位置 | 测试 |
        |---|---|---|---|---|---|
        | TRAP-03 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m1_logs/core.py` | `tests/test_m1_logs.py::test_trap_1` |
| TRAP-05 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m2_time/core.py` | `tests/test_m2_time.py::test_trap_1` |
| TRAP-07 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m3_events/core.py` | `tests/test_m3_events.py::test_trap_1` |
| TRAP-09 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m4_buckets/core.py` | `tests/test_m4_buckets.py::test_trap_1` |
| TRAP-11 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m5_watermark/core.py` | `tests/test_m5_watermark.py::test_trap_1` |
| TRAP-12 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m6_runs/core.py` | `tests/test_m6_runs.py::test_trap_1` |
| TRAP-13 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m1_logs/core.py` | `tests/test_m1_logs.py::test_trap_2` |
| TRAP-14 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m2_time/core.py` | `tests/test_m2_time.py::test_trap_2` |
| TRAP-16 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m3_events/core.py` | `tests/test_m3_events.py::test_trap_2` |
| TRAP-23 | 真实边界输入 | 朴素处理会错分 | 保留证据并稳定裁决 | `app/modules/m4_buckets/core.py` | `tests/test_m4_buckets.py::test_trap_2` |
| TRAP-24 | 整库还原会丢回执 | 直接还原整库 | 回执独立持久化，恢复只替换当前小时桶 | `app/store.py` | `tests/test_m7_restore.py::test_original_receipt_readable_after_restore` |
| TRAP-25 | 全部重放越过恢复时点 | 备份后日志一并重放 | 较新日志隔离为待采纳，不自动入桶 | `app/modules/m7_restore/service.py` | `tests/test_m7_restore.py::test_newer_logs_are_isolated_as_pending_after_restore` |
| TRAP-26 | 恢复期间接收日志污染当前桶 | 直接写当前桶 | 记为 DEFERRED，切换后才转为待采纳 | `app/modules/m7_restore/service.py` | `tests/test_m7_restore.py::test_ingest_during_recovery_not_in_current_bucket` |
| TRAP-27 | 并发恢复互相覆盖 | 后到者直接覆盖 | 版本栅栏 `expected_version`，过期返回 409 | `app/store.py` | `tests/test_m7_restore.py::test_concurrent_restores_only_one_switch_wins` |
| TRAP-28 | 中断后整段重跑重复计数 | 从头重放 | checkpoint 续跑，幂等暂存 | `app/store.py` | `tests/test_m7_restore.py::test_interrupted_restore_resumes_from_checkpoint` |
| TRAP-29 | 切换失败留下半成品 | 先清空再写入 | 单事务切换，失败回滚保留旧版 | `app/store.py` | `tests/test_m7_restore.py::test_switch_failure_keeps_old_version_and_can_retry` |
| TRAP-30 | 旧回执重试重复计数 | 每次重放都入库 | 回执幂等，重试返回原结果 | `app/store.py` | `tests/test_m7_restore.py::test_replaying_old_receipt_does_not_double_count` |

        ## 对账规则

        - 输入记录数 = 有效记录数 + 隔离记录数 + 空记录数。
        - 核心数值或事件数在转换、判定和导出之间保持守恒。
        - 同一输入摘要重跑复用原运行编号，不增加结果行。
        - 备份日志数 = 当前桶计数合计；切换前暂存区合计必须与备份合计一致。
        - 当前桶 + 待采纳 = 实际入库有效日志；恢复期间另计暂存日志。

        ## 已知不覆盖

        - 生产系统连接、身份认证和外部密钥不在演示范围内。
