"""Create local login roles for the schema-scoped fundpos capability roles.

Secrets are generated locally and written only to ~/.alphahome/config.json.
The script never prints a DSN or password.  It uses the existing AlphaHome
administrator connection solely for this one-time role bootstrap.
"""

from __future__ import annotations

import json
import secrets
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

LOGIN_ROLES = {
    "fundpos_app_writer": "fundpos_writer",
    "fundpos_app_migrator": "fundpos_migrator",
}


def _login_dsn(admin_dsn: str, role: str, password: str) -> str:
    values = conninfo_to_dict(admin_dsn)
    values.update(user=role, password=password)
    return make_conninfo(**values)


def main() -> int:
    path = Path("~/.alphahome/config.json").expanduser()
    config = json.loads(path.read_text(encoding="utf-8-sig"))
    admin_dsn = config.get("database", {}).get("url")
    if not admin_dsn:
        raise SystemExit("AlphaHome administrator connection is unavailable")
    passwords = {role: secrets.token_urlsafe(32) for role in LOGIN_ROLES}
    with psycopg.connect(admin_dsn) as connection, connection.transaction():
        with connection.cursor() as cursor:
            for role, capability in LOGIN_ROLES.items():
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if cursor.fetchone():
                    cursor.execute(
                        sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
                            sql.Identifier(role), sql.Literal(passwords[role])
                        )
                    )
                else:
                    cursor.execute(
                        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                            sql.Identifier(role), sql.Literal(passwords[role])
                        )
                    )
                cursor.execute(
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(capability), sql.Identifier(role)
                    )
                )
    config["fundpos_database"] = {
        "url": _login_dsn(admin_dsn, "fundpos_app_writer", passwords["fundpos_app_writer"])
    }
    config["fundpos_migration_database"] = {
        "url": _login_dsn(
            admin_dsn,
            "fundpos_app_migrator",
            passwords["fundpos_app_migrator"],
        )
    }
    path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "configured",
                "writer_role": "fundpos_app_writer",
                "migration_role": "fundpos_app_migrator",
                "secrets_printed": False,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
