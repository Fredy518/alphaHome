#!/usr/bin/env python
# -*- coding: utf-8 -*-

from alphahome.gui.handlers import task_execution_handler


class _Value:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


def test_get_execution_params_does_not_merge_pit_page_selection(monkeypatch):
    collection_task = {"task_name": "stock_basic", "task_type": "collection"}
    monkeypatch.setattr(
        task_execution_handler.data_collection,
        "get_selected_collection_tasks",
        lambda: [collection_task],
    )
    monkeypatch.setattr(task_execution_handler, "add_log_entry", lambda *args, **kwargs: None)

    params = task_execution_handler.get_execution_params(
        {
            "exec_mode": _Value("智能增量"),
            "use_insert_mode": _Value(False),
        }
    )

    assert params is not None
    assert params["tasks_to_run"] == [collection_task]
