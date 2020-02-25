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
from datetime import (
    timedelta,
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
    tuples,
    datetimes,
    timedeltas,
    integers,
)

from twisted.python.filepath import (
    FilePath,
)

from ..storage_common import (
    BYTES_PER_PASS,
)

from ..model import (
    SchemaError,
    StoreOpenError,
    VoucherStore,
    Voucher,
    Pending,
    DoubleSpend,
    Redeemed,
    LeaseMaintenanceActivity,
    open_and_initialize,
    memory_connect,
)

from .strategies import (
    tahoe_configs,
    vouchers,
    voucher_objects,
    random_tokens,
    unblinded_tokens,
    posix_safe_datetimes,
)
from .fixtures import (
    TemporaryVoucherStore,
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
        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
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
        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
        store.add(voucher, tokens)
        self.assertThat(
            store.get(voucher),
            MatchesStructure(
                number=Equals(voucher),
                state=Equals(Pending()),
                created=Equals(now),
            ),
        )

    @given(tahoe_configs(), vouchers(), datetimes(), lists(random_tokens(), unique=True))
    def test_add_idempotent(self, get_config, voucher, now, tokens):
        """
        More than one call to ``VoucherStore.add`` with the same argument results
        in the same state as a single call.
        """
        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
        store.add(voucher, tokens)
        store.add(voucher, [])
        self.assertThat(
            store.get(voucher),
            MatchesStructure(
                number=Equals(voucher),
                created=Equals(now),
                state=Equals(Pending()),
            ),
        )


    @given(tahoe_configs(), datetimes(), lists(vouchers(), unique=True))
    def test_list(self, get_config, now, vouchers):
        """
        ``VoucherStore.list`` returns a ``list`` containing a ``Voucher`` object
        for each voucher previously added.
        """
        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
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


class LeaseMaintenanceTests(TestCase):
    """
    Tests for the lease-maintenance related parts of ``VoucherStore``.
    """
    @given(
        tahoe_configs(),
        posix_safe_datetimes(),
        lists(
            tuples(
                # How much time passes before this activity starts
                timedeltas(min_value=timedelta(1), max_value=timedelta(days=1)),
                # Some activity.  This list of two tuples gives us a trivial
                # way to compute the total passes required (just sum the pass
                # counts in it).  This is nice because it avoids having the
                # test re-implement size quantization which would just be
                # repeated code duplicating the implementation.  The second
                # value lets us fuzz the actual size values a little bit in a
                # way which shouldn't affect the passes required.
                lists(
                    tuples(
                        # The activity itself, in pass count
                        integers(min_value=1, max_value=2 ** 16 - 1),
                        # Amount by which to trim back the share sizes
                        integers(min_value=0, max_value=BYTES_PER_PASS - 1),
                    ),
                ),
                # How much time passes before this activity finishes
                timedeltas(min_value=timedelta(1), max_value=timedelta(days=1)),
            ),
        ),
    )
    def test_lease_maintenance_activity(self, get_config, now, activity):
        """
        ``VoucherStore.get_latest_lease_maintenance_activity`` returns a
        ``LeaseMaintenanceTests`` with fields reflecting the most recently
        finished lease maintenance activity.
        """
        store = self.useFixture(
            TemporaryVoucherStore(get_config, lambda: now),
        ).store

        expected = None
        for (start_delay, sizes, finish_delay) in activity:
            now += start_delay
            started = now
            x = store.start_lease_maintenance()
            passes_required = 0
            for (num_passes, trim_size) in sizes:
                passes_required += num_passes
                x.observe([
                    num_passes * BYTES_PER_PASS - trim_size,
                ])
            now += finish_delay
            x.finish()
            finished = now

            # Let the last iteration of the loop define the expected value.
            expected = LeaseMaintenanceActivity(
                started,
                passes_required,
                finished,
            )

        self.assertThat(
            store.get_latest_lease_maintenance_activity(),
            Equals(expected),
        )


class VoucherTests(TestCase):
    """
    Tests for ``Voucher``.
    """
    @given(voucher_objects())
    def test_json_roundtrip(self, reference):
        """
        ``Voucher.to_json . Voucher.from_json → id``
        """
        self.assertThat(
            Voucher.from_json(reference.to_json()),
            Equals(reference),
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
        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
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

        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
        store.add(voucher_value, random)
        store.insert_unblinded_tokens_for_voucher(voucher_value, unblinded)
        loaded_voucher = store.get(voucher_value)
        self.assertThat(
            loaded_voucher,
            MatchesStructure(
                state=Equals(Redeemed(
                    finished=now,
                    token_count=num_tokens,
                )),
            ),
        )

    @given(
        tahoe_configs(),
        datetimes(),
        vouchers(),
        lists(random_tokens(), unique=True),
    )
    def test_mark_vouchers_double_spent(self, get_config, now, voucher_value, random_tokens):
        """
        A voucher which is reported as double-spent is marked in the database as
        such.
        """
        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
        store.add(voucher_value, random_tokens)
        store.mark_voucher_double_spent(voucher_value)
        voucher = store.get(voucher_value)
        self.assertThat(
            voucher,
            MatchesStructure(
                state=Equals(DoubleSpend(
                    finished=now,
                )),
            ),
        )

    @given(
        tahoe_configs(),
        datetimes(),
        vouchers(),
        integers(min_value=1, max_value=100),
        data(),
    )
    def test_mark_spent_vouchers_double_spent(self, get_config, now, voucher_value, num_tokens, data):
        """
        A voucher which has already been spent cannot be marked as double-spent.
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
        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
        store.add(voucher_value, random)
        store.insert_unblinded_tokens_for_voucher(voucher_value, unblinded)
        try:
            result = store.mark_voucher_double_spent(voucher_value)
        except ValueError:
            pass
        except Exception as e:
            self.fail("mark_voucher_double_spent raised the wrong exception: {}".format(e))
        else:
            self.fail("mark_voucher_double_spent didn't raise, returned: {}".format(result))

    @given(
        tahoe_configs(),
        datetimes(),
        vouchers(),
    )
    def test_mark_invalid_vouchers_double_spent(self, get_config, now, voucher_value):
        """
        A voucher which is not known cannot be marked as double-spent.
        """
        store = self.useFixture(TemporaryVoucherStore(get_config, lambda: now)).store
        try:
            result = store.mark_voucher_double_spent(voucher_value)
        except ValueError:
            pass
        except Exception as e:
            self.fail("mark_voucher_double_spent raised the wrong exception: {}".format(e))
        else:
            self.fail("mark_voucher_double_spent didn't raise, returned: {}".format(result))


    # TODO: Other error states and transient states


def store_for_test(testcase, get_config, get_now):
    """
    Create a ``VoucherStore`` in a temporary directory associated with the
    given test case.

    :param TestCase testcase: The test case for which to build the store.
    :param get_config: A function like the one built by ``tahoe_configs``.
    :param get_now: A no-argument callable that returns a datetime giving a
        time to consider as "now".

    :return VoucherStore: A newly created temporary store.
    """
    tempdir = testcase.useFixture(TempDir())
    config = get_config(tempdir.join(b"node"), b"tub.port")
    store = VoucherStore.from_node_config(
        config,
        get_now,
        memory_connect,
    )
    return store
