# Copyright (c) [2018-2025]  Micro Focus or one of its affiliates.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dbt.tests.adapter.functions.test_udfs import UDFsBasic


class TestVerticaSqlUdfs(UDFsBasic):
    """Vertica SQL (scalar) UDFs created via dbt function models.

    Vertica SQL functions do not accept a volatility specifier, so only the
    base scenario (no volatility config) is exercised; the Deterministic /
    Stable / NonDeterministic variants from the base suite do not apply.
    """

    pass
