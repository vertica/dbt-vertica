from dataclasses import dataclass, field
from typing import FrozenSet, List, Optional

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationConfig, RelationType
from dbt.adapters.relation_configs import (
    RelationConfigChangeAction,
    RelationResults,
)
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.vertica.relation_configs.policies import (
    MAX_CHARACTERS_IN_IDENTIFIER,
    )


@dataclass(frozen=True, eq=False, repr=False)
class VerticaRelation(BaseRelation):
    quote_character: str = '"'
    require_alias: bool = (
        False  # used to govern whether to add an alias when render_limited is called
    )
    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
            }
        )
    )
    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
            }
        )
    )

    def __post_init__(self):
        # Check for length of Postgres table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > self.relation_max_name_length()
        ):
            raise DbtRuntimeError(
                f"Relation name '{self.identifier}' "
                f"is longer than {self.relation_max_name_length()} characters"
            )

    def relation_max_name_length(self):
        return MAX_CHARACTERS_IN_IDENTIFIER

    