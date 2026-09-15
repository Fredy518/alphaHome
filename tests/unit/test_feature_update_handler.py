from alphahome.gui.handlers import feature_update_handler as handler


class FakeLabel:
    def __init__(self):
        self.text = ""

    def config(self, **kwargs):
        self.text = kwargs.get("text", self.text)


def test_cancelled_feature_operation_is_not_rendered_as_failed(monkeypatch):
    label = FakeLabel()
    refresh_requests = []
    monkeypatch.setattr(
        handler.controller,
        "request_feature_list",
        lambda: refresh_requests.append(True),
    )

    handler.handle_feature_operation_complete(
        {"feature_status_label": label},
        {
            "operation": "刷新",
            "status": "cancelled",
            "success_count": 1,
            "fail_count": 0,
            "cancelled_count": 28,
            "refresh_list": False,
        },
    )

    assert label.text == "刷新已停止: 成功 1, 未执行 28"
    assert "失败" not in label.text
    assert refresh_requests == []


def test_busy_feature_operation_is_rendered_as_not_started(monkeypatch):
    label = FakeLabel()
    monkeypatch.setattr(handler.controller, "request_feature_list", lambda: None)

    handler.handle_feature_operation_complete(
        {"feature_status_label": label},
        {
            "operation": "刷新",
            "status": "busy",
            "success_count": 0,
            "fail_count": 0,
            "error_message": "一键日常更新正在运行",
            "refresh_list": False,
        },
    )

    assert label.text == "刷新未启动: 一键日常更新正在运行"
    assert "失败" not in label.text
