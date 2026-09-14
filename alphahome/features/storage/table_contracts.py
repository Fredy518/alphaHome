"""Pure migration generation; normal refresh never installs these constraints."""

from hashlib import sha256

from .atomic import identifier


def unique_key_migration_sql(recipe) -> str:
    target = f"{identifier(recipe.schema)}.{identifier(recipe.view_name)}"
    keys = tuple(recipe.primary_keys)
    if not keys:
        raise ValueError("Recipe has no business-key contract")
    columns = ", ".join(identifier(key) for key in keys)
    nulls = " OR ".join(f"{identifier(key)} IS NULL" for key in keys)
    digest = sha256((recipe.full_name + ':' + ','.join(keys)).encode()).hexdigest()[:12]
    index_name = identifier(f"uq_feature_{digest}")
    statements = [
        "BEGIN;",
        "SET LOCAL lock_timeout = '5s';",
        f"SELECT pg_advisory_xact_lock(hashtext('alphahome.features'), hashtext('{recipe.full_name}'));",
        f"LOCK TABLE {target} IN SHARE ROW EXCLUSIVE MODE;",
        f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM {target} WHERE {nulls}) "
        "THEN RAISE EXCEPTION 'migration refused: null business keys'; END IF; "
        f"IF EXISTS (SELECT 1 FROM {target} GROUP BY {columns} HAVING COUNT(*)>1) "
        "THEN RAISE EXCEPTION 'migration refused: duplicate business keys'; END IF; END $$;",
    ]
    statements.extend(f"ALTER TABLE {target} ALTER COLUMN {identifier(key)} SET NOT NULL;" for key in keys)
    statements.extend([f"CREATE UNIQUE INDEX {index_name} ON {target} ({columns});", "COMMIT;"])
    return "\n".join(statements) + "\n"
