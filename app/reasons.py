REASONS = {
    "BAD_COLUMNS": "字段数量不符", "EMPTY_VALUE": "必要值为空",
    "DUPLICATE_CONFLICT": "同一业务键取值冲突", "TRUNCATED": "尾记录不完整",
    "INVALID_VALUE": "值不符合定义", "STALE_VERSION": "提交版本已过期",
    "TIMEZONE_REQUIRED": "缺少时区", "DST_GAP": "本地时间落在夏令时空隙",
    "RECEIPT_CONFLICT": "回执编号内容冲突", "RESTORE_INCOMPLETE": "恢复尚未摊完",
    "SWITCH_FAILED": "切换失败已保留旧版", "RESTORE_IN_PROGRESS": "恢复进行中",
    "RESTORE_ABORTED": "恢复已放弃", "NOT_FOUND": "目标不存在",
}

def reason(code: str) -> str:
    return REASONS.get(code, "未登记原因")
