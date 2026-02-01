from alphahome.features.storage.refresh import MaterializedViewRefresh


def test_materialized_view_refresh_has_is_materialized_view_helper():
    # Guardrail: prevent AttributeError in runtime refresh path
    assert hasattr(MaterializedViewRefresh, "_is_materialized_view")

