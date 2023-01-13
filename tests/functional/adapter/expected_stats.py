# Copyright (c) [2018-2023]  Micro Focus or one of its affiliates.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



from dbt.tests.util import AnyString, AnyFloat


def vertica_stats(is_table, partition=None, cluster=None):
    stats = {}

    if is_table:
        stats.update(
            {
                "num_bytes": {
                    "id": "num_bytes",
                    "label": AnyString(),
                    "value": AnyFloat(),
                    "description": AnyString(),
                    "include": True,
                },
                "num_rows": {
                    "id": "num_rows",
                    "label": AnyString(),
                    "value": AnyFloat(),
                    "description": AnyString(),
                    "include": True,
                },
            }
        )

    if partition is not None:
        stats.update(
            {
                "partitioning_type": {
                    "id": "partitioning_type",
                    "label": AnyString(),
                    "value": partition,
                    "description": AnyString(),
                    "include": True,
                }
            }
        )

    if cluster is not None:
        stats.update(
            {
                "clustering_fields": {
                    "id": "clustering_fields",
                    "label": AnyString(),
                    "value": cluster,
                    "description": AnyString(),
                    "include": True,
                }
            }
        )

    has_stats = {
        "id": "has_stats",
        "label": "Has Stats?",
        "value": bool(stats),
        "description": "Indicates whether there are statistics for this table",
        "include": False,
    }
    stats["has_stats"] = has_stats

    return stats