from alphahome.common.task_system.task_factory import UnifiedTaskFactory
from alphahome.fetchers.tasks import discover_tasks
from alphahome.fetchers.tasks.macro.tushare_macro_yieldcurve import (
    TushareMacroYieldCurveTask,
)


def test_tushare_macro_yieldcurve_is_archived_and_not_registered():
    UnifiedTaskFactory._task_registry.pop("tushare_macro_yieldcurve", None)
    discover_tasks(force_reload=True)

    assert TushareMacroYieldCurveTask.archived is True
    assert "yc_cb" in TushareMacroYieldCurveTask.archived_reason
    assert TushareMacroYieldCurveTask.hide_from_gui is True
    assert "tushare_macro_yieldcurve" not in UnifiedTaskFactory._task_registry
