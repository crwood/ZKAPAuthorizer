# Copyright 2019 PrivateStorage.io, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests for our custom Hypothesis strategies.
"""

from __future__ import (
    absolute_import,
)

from testtools import (
    TestCase,
)

from fixtures import (
    TempDir,
)

from hypothesis import (
    given,
    note,
)
from hypothesis.strategies import (
    data,
)

from allmydata.client import (
    config_from_string,
)

from .strategies import (
    tahoe_config_texts,
)

class TahoeConfigsTests(TestCase):
    """
    Tests for ``tahoe_configs``.
    """
    @given(data())
    def test_parses(self, data):
        """
        Configurations built by the strategy can be parsed.
        """
        tempdir = self.useFixture(TempDir())
        config_text = data.draw(tahoe_config_texts({}))
        note(config_text)
        config_from_string(
            tempdir.join(b"tahoe.ini"),
            b"tub.port",
            config_text.encode("utf-8"),
        )
