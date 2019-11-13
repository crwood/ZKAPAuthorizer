# coding: utf-8
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
Tests for ``_zkapauthorizer.model``.
"""

from __future__ import (
    absolute_import,
)

from os import (
    mkdir,
)
from errno import (
    EACCES,
)

from testtools import (
    TestCase,
)
from testtools.matchers import (
    AfterPreprocessing,
    MatchesStructure,
    MatchesAll,
    Equals,
    Raises,
    IsInstance,
    raises,
)

from fixtures import (
    TempDir,
)

from hypothesis import (
    given,
)

from hypothesis.strategies import (
    data,
    lists,
    datetimes,
    integers,
)

from twisted.python.filepath import (
    FilePath,
)

from ..model import (
    SchemaError,
    StoreOpenError,
    VoucherStore,
    Voucher,
    open_and_initialize,
    memory_connect,
)

from .strategies import (
    tahoe_configs,
    vouchers,
    random_tokens,
    unblinded_tokens,
)


class VoucherStoreTests(TestCase):
    """
    Tests for ``VoucherStore``.
    """
    def test_create_mismatched_schema(self):
        """
        ``open_and_initialize`` raises ``SchemaError`` if asked for a database
        with a schema version other than it can create.
        """
        tempdir = self.useFixture(TempDir())
        dbpath = tempdir.join(b"db.sqlite3")
        self.assertThat(
            lambda: open_and_initialize(
                FilePath(dbpath),
                required_schema_version=100,
            ),
            raises(SchemaError),
        )


    @given(tahoe_configs(), datetimes(), vouchers())
    def test_get_missing(self, get_config, now, voucher):
        """
        ``VoucherStore.get`` raises ``KeyError`` when called with a
        voucher not previously added to the store.
        """
        tempdir = self.useFixture(TempDir())
        config = get_config(tempdir.join(b"node"), b"tub.port")
        store = VoucherStore.from_node_config(
            config,
            lambda: now,
            memory_connect,
        )
        self.assertThat(
            lambda: store.get(voucher),
            raises(KeyError),
        )

    @given(tahoe_configs(), vouchers(), lists(random_tokens(), unique=True), datetimes())
    def test_add(self, get_config, voucher, tokens, now):
        """
        ``VoucherStore.get`` returns a ``Voucher`` representing a voucher
        previously added to the store with ``VoucherStore.add``.
        """
        tempdir = self.useFixture(TempDir())
        config = get_config(tempdir.join(b"node"), b"tub.port")
        store = VoucherStore.from_node_config(
            config,
            lambda: now,
            memory_connect,
        )
        store.add(voucher, tokens)
        self.assertThat(
            store.get(voucher),
            MatchesStructure(
                number=Equals(voucher),
                redeemed=Equals(False),
                created=Equals(now),
            ),
        )

    @given(tahoe_configs(), vouchers(), datetimes(), lists(random_tokens(), unique=True))
    def test_add_idempotent(self, get_config, voucher, now, tokens):
        """
        More than one call to ``VoucherStore.add`` with the same argument results
        in the same state as a single call.
        """
        tempdir = self.useFixture(TempDir())
        config = get_config(tempdir.join(b"node"), b"tub.port")
        store = VoucherStore.from_node_config(
            config,
            lambda: now,
            memory_connect,
        )
        store.add(voucher, tokens)
        store.add(voucher, [])
        self.assertThat(
            store.get(voucher),
            MatchesStructure(
                number=Equals(voucher),
                created=Equals(now),
                redeemed=Equals(False),
                token_count=Equals(None),
            ),
        )


    @given(tahoe_configs(), datetimes(), lists(vouchers(), unique=True))
    def test_list(self, get_config, now, vouchers):
        """
        ``VoucherStore.list`` returns a ``list`` containing a ``Voucher`` object
        for each voucher previously added.
        """
        tempdir = self.useFixture(TempDir())
        nodedir = tempdir.join(b"node")
        config = get_config(nodedir, b"tub.port")
        store = VoucherStore.from_node_config(
            config,
            lambda: now,
            memory_connect,
        )

        for voucher in vouchers:
            store.add(voucher, [])

        self.assertThat(
            store.list(),
            Equals(list(
                Voucher(number, created=now)
                for number
                in vouchers
            )),
        )

    @given(tahoe_configs(), datetimes())
    def test_uncreateable_store_directory(self, get_config, now):
        """
        If the underlying directory in the node configuration cannot be created
        then ``VoucherStore.from_node_config`` raises ``StoreOpenError``.
        """
        tempdir = self.useFixture(TempDir())
        nodedir = tempdir.join(b"node")

        # Create the node directory without permission to create the
        # underlying directory.
        mkdir(nodedir, 0o500)

        config = get_config(nodedir, b"tub.port")

        self.assertThat(
            lambda: VoucherStore.from_node_config(
                config,
                lambda: now,
                memory_connect,
            ),
            Raises(
                AfterPreprocessing(
                    lambda (type, exc, tb): exc,
                    MatchesAll(
                        IsInstance(StoreOpenError),
                        MatchesStructure(
                            reason=MatchesAll(
                                IsInstance(OSError),
                                MatchesStructure(
                                    errno=Equals(EACCES),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )


    @given(tahoe_configs(), datetimes())
    def test_unopenable_store(self, get_config, now):
        """
        If the underlying database file cannot be opened then
        ``VoucherStore.from_node_config`` raises ``StoreOpenError``.
        """
        tempdir = self.useFixture(TempDir())
        nodedir = tempdir.join(b"node")

        config = get_config(nodedir, b"tub.port")

        # Create the underlying database file.
        store = VoucherStore.from_node_config(config, lambda: now)

        # Prevent further access to it.
        store.database_path.chmod(0o000)

        self.assertThat(
            lambda: VoucherStore.from_node_config(
                config,
                lambda: now,
            ),
            raises(StoreOpenError),
        )


class VoucherTests(TestCase):
    """
    Tests for ``Voucher``.
    """
    @given(vouchers())
    def test_json_roundtrip(self, voucher):
        """
        ``Voucher.to_json . Voucher.from_json → id``
        """
        ref = Voucher(voucher)
        self.assertThat(
            Voucher.from_json(ref.to_json()),
            Equals(ref),
        )


class UnblindedTokenStoreTests(TestCase):
    """
    Tests for ``UnblindedToken``-related functionality of ``VoucherStore``.
    """
    @given(tahoe_configs(), datetimes(), vouchers(), lists(unblinded_tokens(), unique=True))
    def test_unblinded_tokens_round_trip(self, get_config, now, voucher_value, tokens):
        """
        Unblinded tokens that are added to the store can later be retrieved.
        """
        tempdir = self.useFixture(TempDir())
        config = get_config(tempdir.join(b"node"), b"tub.port")
        store = VoucherStore.from_node_config(
            config,
            lambda: now,
            memory_connect,
        )
        store.insert_unblinded_tokens_for_voucher(voucher_value, tokens)
        retrieved_tokens = store.extract_unblinded_tokens(len(tokens))
        self.expectThat(tokens, AfterPreprocessing(sorted, Equals(retrieved_tokens)))

        # After extraction, the unblinded tokens are no longer available.
        more_unblinded_tokens = store.extract_unblinded_tokens(1)
        self.expectThat([], Equals(more_unblinded_tokens))

    @given(
        tahoe_configs(),
        datetimes(),
        vouchers(),
        integers(min_value=1, max_value=100),
        data(),
    )
    def test_mark_vouchers_redeemed(self, get_config, now, voucher_value, num_tokens, data):
        """
        The voucher for unblinded tokens that are added to the store is marked as
        redeemed.
        """
        random = data.draw(
            lists(
                random_tokens(),
                min_size=num_tokens,
                max_size=num_tokens,
                unique=True,
            ),
        )
        unblinded = data.draw(
            lists(
                unblinded_tokens(),
                min_size=num_tokens,
                max_size=num_tokens,
                unique=True,
            ),
        )

        tempdir = self.useFixture(TempDir())
        config = get_config(tempdir.join(b"node"), b"tub.port")
        store = VoucherStore.from_node_config(
            config,
            lambda: now,
            memory_connect,
        )
        store.add(voucher_value, random)
        store.insert_unblinded_tokens_for_voucher(voucher_value, unblinded)
        loaded_voucher = store.get(voucher_value)
        self.assertThat(
            loaded_voucher,
            MatchesStructure(
                redeemed=Equals(True),
                token_count=Equals(num_tokens),
            ),
        )
