import asyncpg
import pytest

from scripts.database.plan_runtime_roles import ROLES, role_bootstrap_sql


pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


async def test_prepared_roles_allow_own_writes_and_deny_compatibility_writes(isolated_database_url):
    connection = await asyncpg.connect(isolated_database_url)
    schemas = ("rawdata", "pit", "factors", "features", "pgs_factors", "role_test_source")
    created_schemas, created_roles = [], False
    try:
        assert not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1))", list(ROLES))
        for schema in schemas:
            if not await connection.fetchval("SELECT to_regnamespace($1)", schema):
                await connection.execute(f"CREATE SCHEMA {schema}")
                created_schemas.append(schema)
        for schema in ("pit", "factors", "features", "role_test_source"):
            await connection.execute(f"CREATE TABLE {schema}.role_test_fact(id integer PRIMARY KEY, value integer)")
        await connection.execute("CREATE VIEW pgs_factors.role_test_compat AS SELECT * FROM factors.role_test_fact")
        await connection.execute("CREATE MATERIALIZED VIEW features.role_test_mv AS SELECT 1 AS value; CREATE UNIQUE INDEX role_test_mv_key ON features.role_test_mv(value)")
        await connection.execute(role_bootstrap_sql(["role_test_source"]))
        created_roles = True
        for role, schema in (("ah_fetch_writer", "role_test_source"), ("ah_pit_writer", "pit"),
                             ("ah_factor_writer", "factors"), ("ah_feature_writer", "features")):
            await connection.execute(f"SET ROLE {role}")
            await connection.execute(f"INSERT INTO {schema}.role_test_fact VALUES(1,1)")
            assert await connection.fetchval("SELECT COUNT(*) FROM pgs_factors.role_test_compat") in (0, 1)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute("INSERT INTO pgs_factors.role_test_compat VALUES(9,9)")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(f"ALTER TABLE {schema}.role_test_fact ADD COLUMN forbidden integer")
            if role != "ah_factor_writer":
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute("INSERT INTO factors.role_test_fact VALUES(2,2)")
            if role == "ah_feature_writer":
                await connection.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY features.role_test_mv")
            await connection.execute("RESET ROLE")
        await connection.execute("SET ROLE ah_data_reader")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute("DELETE FROM factors.role_test_fact")
        await connection.execute("RESET ROLE")
    finally:
        await connection.execute("RESET ROLE")
        if created_roles:
            await connection.execute("DROP OWNED BY " + ",".join(ROLES))
            for role in reversed(ROLES):
                await connection.execute(f"DROP ROLE {role}")
        await connection.execute("DROP MATERIALIZED VIEW IF EXISTS features.role_test_mv; DROP VIEW IF EXISTS pgs_factors.role_test_compat")
        for schema in ("pit", "factors", "features", "role_test_source"):
            await connection.execute(f"DROP TABLE IF EXISTS {schema}.role_test_fact")
        for schema in reversed(created_schemas):
            await connection.execute(f"DROP SCHEMA {schema}")
        await connection.close()
