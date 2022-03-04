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
Tests for ``_zkapauthorizer.tests.matchers``.
"""

from json import dumps
from math import isfinite, nextafter
from sqlite3 import connect

from hypothesis import assume, example, given
from hypothesis.strategies import (
    booleans,
    fixed_dictionaries,
    floats,
    integers,
    just,
    lists,
    sampled_from,
    tuples,
)
from testtools import TestCase
from testtools.matchers import Always, Annotate, Equals, Is, Not
from zope.interface import Interface, implementer

from ._sql_matchers import structured_dump
from .matchers import (
    Provides,
    equals_database,
    matches_float_within_distance,
    matches_json,
    returns,
)
from .sql import Column, Insert, StorageAffinity, Table, create_table
from .strategies import inserts, sql_schemas


class IX(Interface):
    pass


class IY(Interface):
    pass


@implementer(IX, IY)
class X(object):
    pass


@implementer(IY)
class Y(object):
    pass


class ProvidesTests(TestCase):
    """
    Tests for ``Provides``.
    """

    def test_match(self):
        """
        ``Provides.match`` returns ``None`` when the given object provides all of
        the configured interfaces.
        """
        self.assertThat(
            Provides([IX, IY]).match(X()),
            Is(None),
        )

    def test_mismatch(self):
        """
        ``Provides.match`` does not return ``None`` when the given object provides
        none of the configured interfaces.
        """
        self.assertThat(
            Provides([IX, IY]).match(Y()),
            Not(Is(None)),
        )


class ReturnsTests(TestCase):
    """
    Tests for ``returns``.
    """

    def test_match(self):
        """
        ``returns(m)`` returns a matcher that matches when the given object
        returns a value matched by ``m``.
        """
        result = object()
        self.assertThat(
            returns(Is(result)).match(lambda: result),
            Is(None),
        )

    def test_mismatch(self):
        """
        ``returns(m)`` returns a matcher that does not match when the given object
        returns a value not matched by ``m``.
        """
        result = object()
        other = object()
        self.assertThat(
            returns(Is(result)).match(lambda: other),
            Not(Is(None)),
        )


class MatchesJSONTests(TestCase):
    """
    Tests for ``matches_json``.
    """

    def test_non_string(self):
        """
        If the value given isn't a string then ``matches_json`` does not match.
        """
        self.assertThat(
            matches_json(Always()).match(object()),
            Not(Is(None)),
        )

    def test_unparseable(self):
        """
        If the value can't be parsed as JSON then ``matches_json`` does not match.
        """
        self.assertThat(
            matches_json(Always()).match("not json"),
            Not(Is(None)),
        )

    def test_does_not_match(self):
        """
        If the parsed value isn't matched by the given matcher then
        ``matches_json`` does not match.
        """
        expected = {"hello": "world"}
        self.assertThat(
            matches_json(Not(Equals(expected))).match(dumps(expected)),
            Not(Is(None)),
        )

    def test_matches(self):
        """
        If the parsed value is matched by the given matcher then ``matches_json``
        matches.
        """
        expected = {"hello": "world"}
        self.assertThat(
            matches_json(Equals(expected)).match(dumps(expected)),
            Is(None),
        )


def _get_float_at_distance(reference: float, distance: int, negative: bool) -> float:
    """
    Get a floating point value that is a certain ULP distance from the
    reference value.

    :param reference: The reference value that will be at the indicated
        distance from the result.

    :param distance: The ULP distance from the reference to the result.

    :param negative: If true then the result will be towards negative infinity
        from the reference, otherwise it will be towards positive infinity.

    :return: The new value
    """
    towards = float("inf")
    if negative:
        towards *= -1
    actual = reference
    for n in range(distance):
        actual = nextafter(actual, towards)
    return actual


class MatchFloatWithinDistanceTests(TestCase):
    """
    Tests for ``matches_float_within_distance``.
    """

    def test_nan_rejected(self):
        """
        A reference or actual value of NaN never matches because the distance is
        undefined.
        """
        nan = float("nan")
        self.expectThat(
            matches_float_within_distance(nan, 0).match(0.0),
            Not(Is(None)),
        )
        self.expectThat(
            matches_float_within_distance(0.0, 0).match(nan),
            Not(Is(None)),
        )

    @given(floats(allow_nan=False), integers(min_value=0, max_value=100), booleans())
    def test_within_distance(self, reference, distance, negative):
        """
        If the distance from the reference to the goal is within the distance
        constraint then the match is successful.
        """
        actual = _get_float_at_distance(reference, distance, negative)
        self.assertThat(
            matches_float_within_distance(reference, distance).match(actual),
            Is(None),
        )

    @given(floats(allow_nan=False), integers(min_value=0, max_value=100), booleans())
    def test_not_within_distance(self, reference, distance, negative):
        """
        If the distance from the reference to the goal is greater than the
        distance constraint then the match fails.
        """
        not_quite = _get_float_at_distance(reference, distance, negative)

        # If we already hit infinity then we don't have enough room to go the
        # distance represented by this example.
        assume(isfinite(not_quite))

        # It's fine for *this* value to be infinity since it's the last one we
        # need to compute.
        actual = _get_float_at_distance(not_quite, 1, negative)

        # If we can't take the last step and get to a new value then we
        assume(actual != not_quite)

        self.assertThat(
            matches_float_within_distance(reference, distance).match(actual),
            Not(Is(None)),
        )


def _float_example(f):
    """
    Help create Hypothesis examples for certain floating point cases.
    """
    t = Table([("0", Column(StorageAffinity.REAL))])
    return example(
        (
            {"0": t},
            {"0": [Insert("0", t, (f,))]},
        )
    )


class EqualsDatabase(TestCase):
    """
    Tests for the ``equals_database`` matcher.
    """

    def setup_example(self):
        self.a = connect(":memory:")
        self.b = connect(":memory:")

    @given(sql_schemas())
    def test_same_schema(self, tables):
        """
        Two databases with the same schema match.
        """
        for db in [self.a, self.b]:
            for name, table in tables.items():
                db.execute(create_table(name, table))

        self.assertThat(
            equals_database(self.a).match(self.b),
            Is(None),
        )

    @given(sql_schemas(), sql_schemas())
    def test_different_schema(self, schema_a, schema_b):
        """
        Two databases with different schemas do not match.
        """
        assume(schema_a != schema_b)
        for db, schema in [(self.a, schema_a), (self.b, schema_b)]:
            for name, table in schema.items():
                db.execute(create_table(name, table))

        self.assertThat(
            self.a,
            Annotate(
                f"\ndb a: {list(structured_dump(self.a))}"
                f"\ndb b: {list(structured_dump(self.b))}",
                Not(equals_database(self.b)),
            ),
        )

    @given(
        sql_schemas(dict_kwargs={"min_size": 1}).flatmap(
            lambda schema: tuples(
                # Pass along the schema so the test can create it.
                just(schema),
                # Build some arbitrary amount of data that fits into the
                # schema so there are rows of data involved.
                fixed_dictionaries(
                    {
                        name: lists(inserts(name, table))
                        for (name, table) in schema.items()
                    }
                ),
                # Create one more row of data which will be used to make the
                # databases differ.
                sampled_from(sorted(schema.items())).flatmap(
                    lambda item: inserts(*item),
                ),
            ),
        )
    )
    def test_different_rows(self, schema_and_common_and_different):
        """
        Two databases with the same schema but different rows in their tables do
        not match.
        """
        schema, common_inserts, different_insert = schema_and_common_and_different
        for name, table in schema.items():
            sql = create_table(name, table)
            for db in [self.a, self.b]:
                db.execute(sql)

        for name, statements in common_inserts.items():
            for stmt in statements:
                for db in [self.a, self.b]:
                    db.execute(stmt.statement(), stmt.arguments())

        self.a.execute(different_insert.statement(), different_insert.arguments())

        self.assertThat(
            self.a,
            Not(equals_database(self.b)),
        )

    @given(
        sql_schemas(dict_kwargs={"min_size": 1}).flatmap(
            lambda schema: tuples(
                # Pass along the schema so the test can create it.
                just(schema),
                # Build some arbitrary amount of data that fits into the
                # schema so there are rows of data involved.
                fixed_dictionaries(
                    {
                        name: lists(inserts(name, table))
                        for (name, table) in schema.items()
                    }
                ),
            ),
        )
    )
    # Add some known problematic cases.  Hypothesis found these originally but
    # let's help it keep an eye on them in the future, too.
    @_float_example(1.12589990684262408748e15)
    @_float_example(1.12589990684262430953e15)
    def test_same_rows(self, schema_and_common):
        """
        Two databases with the same schema and the same rows in their tables
        match.
        """
        schema, common_inserts = schema_and_common
        for name, table in schema.items():
            sql = create_table(name, table)
            for db in [self.a, self.b]:
                db.execute(sql)

        for name, statements in common_inserts.items():
            for stmt in statements:
                for db in [self.a, self.b]:
                    db.execute(stmt.statement(), stmt.arguments())

        self.assertThat(
            equals_database(self.b).match(self.a),
            Is(None),
        )
