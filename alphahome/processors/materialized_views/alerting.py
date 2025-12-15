"""\
物化视图告警系统

实现物化视图的监控告警机制，包括：
- 刷新失败告警
- 数据质量告警
- 告警日志记录
- 告警历史查询

告警系统记录所有问题，供用户手动复查和处理。
"""

from typing import Any, Dict, List, Optional
import logging
from enum import Enum

from alphahome.common.db_manager import DBManager


class AlertSeverity(Enum):
    """告警严重级别"""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertType(Enum):
    """告警类型"""

    REFRESH_FAILED = "refresh_failed"
    REFRESH_TIMEOUT = "refresh_timeout"
    DATA_QUALITY_WARNING = "data_quality_warning"
    DATA_QUALITY_ERROR = "data_quality_error"
    NULL_VALUES_DETECTED = "null_values_detected"
    OUTLIERS_DETECTED = "outliers_detected"
    ROW_COUNT_ANOMALY = "row_count_anomaly"
    DUPLICATE_KEYS = "duplicate_keys"
    TYPE_MISMATCH = "type_mismatch"


class MaterializedViewAlerting:
    """物化视图告警系统。"""

    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)

    async def alert_refresh_failed(
        self,
        view_name: str,
        error_message: str,
        refresh_strategy: str = "full",
        duration_seconds: float = 0.0,
        additional_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            if "timeout" in error_message.lower():
                severity = AlertSeverity.ERROR.value
                alert_type = AlertType.REFRESH_TIMEOUT.value
            else:
                severity = AlertSeverity.ERROR.value
                alert_type = AlertType.REFRESH_FAILED.value

            details: Dict[str, Any] = {
                "error_message": error_message,
                "refresh_strategy": refresh_strategy,
                "duration_seconds": duration_seconds,
            }
            if additional_details:
                details.update(additional_details)

            await self._record_alert(
                view_name=view_name,
                alert_type=alert_type,
                severity=severity,
                message=f"Refresh failed for {view_name}: {error_message}",
                details=details,
            )

            self.logger.error(
                f"刷新失败告警: {view_name} - {error_message}",
                extra={
                    "view_name": view_name,
                    "alert_type": alert_type,
                    "severity": severity,
                },
            )

        except Exception as e:
            self.logger.error(f"记录刷新失败告警失败: {e}", exc_info=True)
            raise

    async def alert_data_quality_issue(
        self,
        view_name: str,
        check_name: str,
        check_status: str,
        check_message: str,
        check_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            if check_status == "pass":
                return

            alert_type, severity = self._map_check_to_alert(check_name, check_status)

            details: Dict[str, Any] = {
                "check_name": check_name,
                "check_status": check_status,
                "check_message": check_message,
            }
            if check_details:
                details.update(check_details)

            await self._record_alert(
                view_name=view_name,
                alert_type=alert_type,
                severity=severity,
                message=f"Data quality issue in {view_name}: {check_message}",
                details=details,
            )

            log_level = logging.WARNING if check_status == "warning" else logging.ERROR
            self.logger.log(
                log_level,
                f"数据质量告警: {view_name} - {check_name} - {check_message}",
                extra={
                    "view_name": view_name,
                    "alert_type": alert_type,
                    "severity": severity,
                    "check_name": check_name,
                },
            )

        except Exception as e:
            self.logger.error(f"记录数据质量告警失败: {e}", exc_info=True)
            raise

    async def get_alert_history(
        self,
        view_name: Optional[str] = None,
        alert_type: Optional[str] = None,
        severity: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        try:
            where_clauses = []
            params: List[Any] = []

            if view_name:
                where_clauses.append("view_name = $" + str(len(params) + 1))
                params.append(view_name)

            if alert_type:
                where_clauses.append("alert_type = $" + str(len(params) + 1))
                params.append(alert_type)

            if severity:
                where_clauses.append("severity = $" + str(len(params) + 1))
                params.append(severity)

            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

            query = f"""
            SELECT
                id,
                view_name,
                alert_type,
                severity,
                message,
                details,
                created_at
            FROM materialized_views.materialized_views_alerts
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2};
            """

            params.extend([limit, offset])
            rows = await self.db_manager.fetch(query, *params)

            alerts: List[Dict[str, Any]] = []
            for row in rows:
                import json

                details = json.loads(row["details"]) if row.get("details") else {}
                alerts.append(
                    {
                        "id": row["id"],
                        "view_name": row["view_name"],
                        "alert_type": row["alert_type"],
                        "severity": row["severity"],
                        "message": row["message"],
                        "details": details,
                        "created_at": row["created_at"],
                    }
                )

            self.logger.info(f"获取告警历史: {len(alerts)} 条记录")
            return alerts

        except Exception as e:
            self.logger.error(f"获取告警历史失败: {e}", exc_info=True)
            raise

    async def get_alert_summary(self, view_name: Optional[str] = None, days: int = 7) -> Dict[str, Any]:
        try:
            where_clause = ""
            params: List[Any] = []

            if view_name:
                where_clause = "WHERE view_name = $1 AND created_at >= NOW() - INTERVAL '1 day' * $2"
                params = [view_name, days]
            else:
                where_clause = "WHERE created_at >= NOW() - INTERVAL '1 day' * $1"
                params = [days]

            query = f"""
            SELECT
                severity,
                COUNT(*) as count
            FROM materialized_views.materialized_views_alerts
            {where_clause}
            GROUP BY severity
            ORDER BY severity;
            """

            rows = await self.db_manager.fetch(query, *params)

            summary: Dict[str, Any] = {
                "total": 0,
                "by_severity": {},
                "critical": 0,
                "error": 0,
                "warning": 0,
                "info": 0,
            }

            for row in rows:
                sev = row["severity"]
                count = int(row["count"])
                summary["total"] += count
                summary["by_severity"][sev] = count
                if sev in summary:
                    summary[sev] = count

            return summary

        except Exception as e:
            self.logger.error(f"获取告警统计摘要失败: {e}", exc_info=True)
            raise

    async def get_unacknowledged_alerts(
        self,
        view_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """获取未确认的告警列表。"""

        try:
            where_clauses = ["acknowledged = FALSE"]
            params: List[Any] = []

            if view_name:
                where_clauses.append("view_name = $" + str(len(params) + 1))
                params.append(view_name)

            where_clause = " AND ".join(where_clauses)
            query = f"""
            SELECT
                id,
                view_name,
                alert_type,
                severity,
                message,
                details,
                acknowledged,
                acknowledged_by,
                acknowledged_at,
                notes,
                created_at
            FROM materialized_views.materialized_views_alerts
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2};
            """

            params.extend([limit, offset])
            rows = await self.db_manager.fetch(query, *params)

            import json

            alerts: List[Dict[str, Any]] = []
            for row in rows:
                details = json.loads(row["details"]) if row.get("details") else {}
                alerts.append(
                    {
                        "id": row["id"],
                        "view_name": row["view_name"],
                        "alert_type": row["alert_type"],
                        "severity": row["severity"],
                        "message": row["message"],
                        "details": details,
                        "acknowledged": row.get("acknowledged"),
                        "acknowledged_by": row.get("acknowledged_by"),
                        "acknowledged_at": row.get("acknowledged_at"),
                        "notes": row.get("notes"),
                        "created_at": row["created_at"],
                    }
                )

            return alerts

        except Exception as e:
            self.logger.error(f"获取未确认告警失败: {e}", exc_info=True)
            raise

    async def acknowledge_alert(self, alert_id: int, acknowledged_by: str, notes: str = "") -> None:
        """确认告警（将 acknowledged 标记为 True）。"""

        try:
            update_sql = """
            UPDATE materialized_views.materialized_views_alerts
            SET
                acknowledged = TRUE,
                acknowledged_by = $1,
                acknowledged_at = NOW(),
                notes = $2
            WHERE id = $3;
            """
            await self.db_manager.execute(update_sql, acknowledged_by, notes, alert_id)
        except Exception as e:
            self.logger.error(f"确认告警失败: {e}", exc_info=True)
            raise

    def _map_check_to_alert(self, check_name: str, check_status: str) -> tuple[str, str]:
        if check_name == "null_check":
            alert_type = AlertType.NULL_VALUES_DETECTED.value
        elif check_name == "outlier_check":
            alert_type = AlertType.OUTLIERS_DETECTED.value
        elif check_name == "row_count_change":
            alert_type = AlertType.ROW_COUNT_ANOMALY.value
        elif check_name == "duplicate_check":
            alert_type = AlertType.DUPLICATE_KEYS.value
        elif check_name == "type_check":
            alert_type = AlertType.TYPE_MISMATCH.value
        else:
            alert_type = AlertType.DATA_QUALITY_WARNING.value

        severity = AlertSeverity.WARNING.value if check_status == "warning" else AlertSeverity.ERROR.value
        return alert_type, severity

    async def _record_alert(
        self,
        *,
        view_name: str,
        alert_type: str,
        severity: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        import json

        details_json = json.dumps(details) if details else "{}"
        insert_sql = """
        INSERT INTO materialized_views.materialized_views_alerts (
            view_name,
            alert_type,
            severity,
            message,
            details,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, NOW());
        """
        await self.db_manager.execute(insert_sql, view_name, alert_type, severity, message, details_json)
