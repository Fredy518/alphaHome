"""Print a reviewable PG17 role bootstrap. This command never connects or executes SQL."""

import argparse
import re


ROLES = ("ah_data_reader", "ah_fetch_writer", "ah_pit_writer", "ah_factor_writer", "ah_feature_writer", "ah_schema_owner")


def quoted(name):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        raise ValueError("Schema names must be lower_snake_case identifiers")
    return '"' + name + '"'


def role_bootstrap_sql(fetch_schemas=()):
    sources = tuple(sorted(set(fetch_schemas)))
    if set(sources) & {"public", "rawdata", "pit", "factors", "features", "pgs_factors", "fundpos"}:
        raise ValueError("Fetch writers may only own explicitly named source schemas")
    schemas = (*sources, "rawdata", "pit", "factors", "features", "pgs_factors")
    for schema in schemas:
        quoted(schema)
    sql = ["-- REVIEW ONLY: PG17; fresh role names; existing schemas required; no login/password or ownership change.",
           "BEGIN;", "SET LOCAL lock_timeout='5s';"]
    for role in ROLES:
        # CREATE deliberately rejects pre-existing roles with unknown privilege inheritance.
        sql.append(f"CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;")
    sql.append("GRANT ah_data_reader TO ah_fetch_writer,ah_pit_writer,ah_factor_writer,ah_feature_writer;")
    sql.append("DO $$ BEGIN EXECUTE format('GRANT CONNECT,TEMP ON DATABASE %I TO ah_data_reader', current_database()); END $$;")
    for schema in schemas:
        namespace = quoted(schema)
        sql.extend([
            f"GRANT USAGE ON SCHEMA {namespace} TO ah_data_reader;",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO ah_data_reader;",
            f"GRANT USAGE,CREATE ON SCHEMA {namespace} TO ah_schema_owner;",
            f"ALTER DEFAULT PRIVILEGES FOR ROLE ah_schema_owner IN SCHEMA {namespace} GRANT SELECT ON TABLES TO ah_data_reader;",
        ])
    for schema, role in [(value, "ah_fetch_writer") for value in sources] + [
        ("pit", "ah_pit_writer"), ("factors", "ah_factor_writer"), ("features", "ah_feature_writer")
    ]:
        namespace = quoted(schema)
        sql.extend([
            f"GRANT INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA {namespace} TO {role};",
            f"GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA {namespace} TO {role};",
            f"ALTER DEFAULT PRIVILEGES FOR ROLE ah_schema_owner IN SCHEMA {namespace} GRANT INSERT,UPDATE,DELETE ON TABLES TO {role};",
            f"ALTER DEFAULT PRIVILEGES FOR ROLE ah_schema_owner IN SCHEMA {namespace} GRANT USAGE,SELECT ON SEQUENCES TO {role};",
        ])
    sql.extend([
        "GRANT MAINTAIN ON ALL TABLES IN SCHEMA features TO ah_feature_writer;",
        "ALTER DEFAULT PRIVILEGES FOR ROLE ah_schema_owner IN SCHEMA features GRANT MAINTAIN ON TABLES TO ah_feature_writer;",
        "REVOKE CREATE ON SCHEMA pgs_factors FROM PUBLIC,ah_data_reader,ah_fetch_writer,ah_pit_writer,ah_factor_writer,ah_feature_writer;",
        "REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN ON ALL TABLES IN SCHEMA pgs_factors FROM PUBLIC,ah_data_reader,ah_fetch_writer,ah_pit_writer,ah_factor_writer,ah_feature_writer;",
        "COMMIT;",
    ])
    return "\n".join(sql) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fetch-schema", action="append", default=[])
    args = parser.parse_args()
    print(role_bootstrap_sql(args.fetch_schema), end="")


if __name__ == "__main__":
    main()
