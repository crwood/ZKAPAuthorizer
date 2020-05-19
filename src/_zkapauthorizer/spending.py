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
A module for logic controlling the manner in which ZKAPs are spent.
"""

from zope.interface import (
    Interface,
    Attribute,
    implementer,
)

import attr

from .eliot import (
    GET_PASSES,
)

class IPassGroup(Interface):
    """
    A group of passed meant to be spent together.
    """
    passes = Attribute(":ivar list[Pass] passes: The passes themselves.")

    def split(select_indices):
        """
        Create two new ``PassGroup`` instances.  The first contains all passes in
        this group at the given indices.  The second contains all the others.

        :param list[int] select_indices: The indices of the passes to include
            in the first resulting group.

        :return (IPassGroup, IPassGroup): The two new groups.
        """

    def expand(by_amount):
        """
        Create a new ``PassGroup`` which contains all of this groups passes and
        some more.

        :param int by_amount: The number of additional passes the resulting
            group should contain.

        :return IPassGroup: The new group.
        """

    def mark_spent():
        """
        The passes have been spent successfully.  Ensure none of them appear in
        any ``PassGroup`` created in the future.

        :return: ``None``
        """

    def mark_invalid(reason):
        """
        The passes could not be spent.  Ensure none of them appear in any
        ``PassGroup`` created in the future.

        :param unicode reason: A short description of the reason the passes
            could not be spent.

        :return: ``None``
        """

    def reset():
        """
        The passes have not been spent.  Return them to for use in a future
        ``PassGroup``.

        :return: ``None``
        """


@implementer(IPassGroup)
@attr.s
class PassGroup(object):
    _factory = attr.ib()
    passes = attr.ib()

    def split(self, select_indices):
        selected = []
        unselected = []
        for idx, p in enumerate(self.passes):
            if idx in select_indices:
                selected.append(p)
            else:
                unselected.append(p)
        return (
            attr.evolve(self, passes=selected),
            attr.evolve(self, passes=unselected),
        )

    def expand(self, by_amount):
        return attr.evolve(
            self,
            passes=self.passes + self._factory.get(by_amount).passes,
        )

    def mark_spent(self):
        self._factory._mark_spent(self.passes)

    def mark_invalid(self, reason):
        self._factory._mark_invalid(reason, self.passes)

    def reset(self):
        self._factory._reset(self.passes)


@attr.s
class SpendingController(object):
    """
    A ``SpendingController`` gives out ZKAPs and arranges for re-spend
    attempts when necessary.
    """
    extract_unblinded_tokens = attr.ib()
    tokens_to_passes = attr.ib()

    def get(self, message, num_passes):
        unblinded_tokens = self.extract_unblinded_tokens(num_passes)
        passes = self.tokens_to_passes(message, unblinded_tokens)
        GET_PASSES.log(
            message=message,
            count=num_passes,
        )
        return PassGroup(_MessageBoundFactory(message, self), passes)


@attr.s
class _MessageBoundFactory(object):
    message = attr.ib()
    factory = attr.ib()

    def get(self, num_passes):
        return self.factory.get(self.message, num_passes)
