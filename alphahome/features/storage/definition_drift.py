"""Detect financial MV code AND installed-definition drift using a creation seal.

The seal is written only when explicitly creating a new MV. Inspection/refresh
never repairs or replaces definitions. PostgreSQL's deparsed SQL is hashed after
whitespace normalization, and the recipe hash is separately recorded, so an
unchanged embedded marker cannot hide a manual DDL edit.
"""

from hashlib import sha256
import re

PREFIX = "alphahome_financial_definition_v2"


def recipe_signature(recipe):
    sql = recipe.get_create_sql()
    match = re.search(r"'([a-f0-9]{64})'::text\s+AS\s+_pit_definition_hash", sql, re.I)
    return match.group(1) if match else None


def definition_hash(definition):
    return sha256(" ".join(definition.split()).encode()).hexdigest()


def definition_seal(recipe, definition):
    signature = recipe_signature(recipe)
    if not signature or not definition:
        raise ValueError(
            "Financial recipe signature and installed definition are required"
        )
    return f"{PREFIX}:{signature}:{definition_hash(definition)}"


def definition_drift(recipe, definition, comment):
    if recipe_signature(recipe) is None:
        return None
    expected = definition_seal(recipe, definition) if definition else None
    if comment != expected:
        return f"migration_required: {recipe.full_name} financial definition drift or missing seal"
    return None


def seal_comment_sql(recipe, definition):
    # Only internal, validated relation names are allowed in COMMENT DDL.
    if not re.fullmatch(
        r"[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*", recipe.full_name
    ):
        raise ValueError("Unsafe materialized-view name")
    seal = definition_seal(recipe, definition)
    return f"COMMENT ON MATERIALIZED VIEW {recipe.full_name} IS '{seal}'"
