"""Render small, reviewable migrations. This module never opens a database."""

import argparse
import re


def _identifier(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise ValueError('Invalid migration identifier')
    return f'"{value}"'


def recovery_schema_sql():
    from alphahome.features.storage.recovery import CREATE_CHECKPOINT_SQL
    from alphahome.pit.run_ledger import CREATE_RUN_LEDGER_SQL

    return ("BEGIN;\nSET LOCAL lock_timeout = '5s';\n"
            "CREATE SCHEMA IF NOT EXISTS pit;\nCREATE SCHEMA IF NOT EXISTS features;\n"
            + CREATE_RUN_LEDGER_SQL + CREATE_CHECKPOINT_SQL + "\nCOMMIT;\n")


def rawdata_mapping_sql(view, source_schema, source_table, columns=None):
    target = 'rawdata.' + _identifier(view)
    source = _identifier(source_schema) + '.' + _identifier(source_table)
    projection = ', '.join(_identifier(column) for column in columns) if columns else '*'
    # OR REPLACE retains dependencies and fails on incompatible existing column
    # types/order. Never DROP CASCADE to force a routine mapping migration.
    return ("BEGIN;\nSET LOCAL lock_timeout = '5s';\nCREATE SCHEMA IF NOT EXISTS rawdata;\n"
            f'CREATE OR REPLACE VIEW {target} AS SELECT {projection} FROM {source};\nCOMMIT;\n')


def main(argv=None):
    parser = argparse.ArgumentParser(description='Print maintenance SQL only; no database connection or execution')
    commands = parser.add_subparsers(dest='operation', required=True)
    commands.add_parser('recovery-ledgers')
    mapping = commands.add_parser('rawdata-mapping')
    mapping.add_argument('--view', required=True)
    mapping.add_argument('--source-schema', required=True)
    mapping.add_argument('--source-table', required=True)
    mapping.add_argument('--column', action='append', help='Explicit projection in existing view column order')
    args = parser.parse_args(argv)
    sql = recovery_schema_sql() if args.operation == 'recovery-ledgers' else rawdata_mapping_sql(
        args.view, args.source_schema, args.source_table, args.column,
    )
    print(sql, end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
