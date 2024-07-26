from dbt.contracts.results import CatalogArtifact
from dbt.tests.util import run_dbt
import pytest

from dbt.tests.adapter.catalog import files


class TestCatalogRelationTypes:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "table.sql": files.MY_TABLE,
            "view.sql": files.MY_VIEW,
            #"my_materialized_view.sql": files.MY_MATERIALIZED_VIEW,
        }

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["seed"])
        #run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    @pytest.mark.parametrize(
        "node_name,relation_type",
        [
           # ("seed.test.seed", "table"),
            ("model.test.table", "table"),
           # ("model.test.view", "view"),
           # ("model.test.my_materialized_view", "materialized view"),
        ],
    )
    def test_relation_types_populate_correctly(
        self, docs: CatalogArtifact, node_name: str, relation_type: str
    ):
        
        assert node_name in docs.nodes
        node = docs.nodes[node_name]
        assert node.metadata.type == relation_type