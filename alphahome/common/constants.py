# alphahome/common/constants.py

class UpdateTypes:
    """
    定义任务更新类型常量。
    """
    SMART = "smart"
    MANUAL = "manual"
    FULL = "full"

    # 显示文本常量
    SMART_DISPLAY = "智能增量"
    MANUAL_DISPLAY = "手动增量"
    FULL_DISPLAY = "全量更新"


class ApiParams:
    """
    定义API参数常量。
    """
    LIST_STATUS_LISTED = "L"    # 上市状态
    LIST_STATUS_DELISTED = "D"  # 退市状态
    LIST_STATUS_PAUSED = "P"    # 暂停上市