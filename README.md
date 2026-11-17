# 多时区应用日志小时桶重算

        面向一线复核人员的单服务数据工作台。样例数据随仓库提供，结论保留行号、原因码和原值。

        ## 业务模块

        - M1 日志采信：`/api/logs/import`
- M2 时间解析：`/api/time/normalize`
- M3 事件去重：`/api/events/dedupe`
- M4 小时分桶：`/api/buckets/rebuild`
- M5 迟到修订：`/api/watermark/advance`
- M6 批次对账：`/api/export/buckets.csv`
- M7 备份恢复：`/api/backups` `/api/restores` `/restore`

        ## 核心规则

        事件先归一到 UTC，DST 重叠时保留 fold；小时桶右端开，水位只能前进且迟到事件仍进入修订。

        ## 启动

        ```bash
        docker compose up --build
        ```

        浏览器访问 `http://localhost:18212`。运行测试：

        ```bash
        docker compose exec -T app pytest -q
        ```

        ## 样例流程

        依次运行六个模块；最后在批次页复核输入、有效、隔离与输出合计。同一输入摘要重复执行时复用原 `run_id`。

        ## 备份恢复（M7）

        实际入库日志写入独立台账，回执单独持久化，备份只取实际入库且属于当前桶的日志
        （`POST /api/backups`，可按 `at` 时点选取）。恢复分三段：

        - `POST /api/restores` 创建恢复并把备份摊入暂存区（可带 `chunks` 分批）；
          中断后用 `POST /api/restores/{id}/resume` 从 checkpoint 续跑。
        - `POST /api/restores/{id}/switch` 才替换当前小时桶；切换失败或版本过期时
          事务回滚，旧版保留。`expected_version` 是并发栅栏，过期返回 409。
        - 恢复进行中继续接收日志，标为暂存（`DEFERRED`），不进当前桶；切换后较新
          日志为待采纳（`PENDING`），用 `POST /api/pending/adopt` 并入。
        - `GET /api/restores/{id}/readback` 回读恢复中、已生效与待采纳三段范围。

        原回执始终可回读（`GET /api/intake/receipts/{receipt_id}`）；同一回执重试
        返回原结果且不重复计数，内容不同返回 409 `RECEIPT_CONFLICT`。

        ## 已知不覆盖

        不处理加密文件与需要外部密钥的对象；不连接真实生产存储。
        不扩展时区校正（沿用 M2 规则）与访问授权。
