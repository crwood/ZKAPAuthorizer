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
``allmydata.storage``-related helpers shared across the test suite.
"""

from os import (
    SEEK_CUR,
)
from struct import (
    pack,
)

from itertools import (
    count,
    islice,
)

import attr

from twisted.python.filepath import (
    FilePath,
)

from .strategies import (
    # Not really a strategy...
    bytes_for_share,
)

from ..spending import (
    PassGroup,
)


# Hard-coded in Tahoe-LAFS
LEASE_INTERVAL = 60 * 60 * 24 * 31

def cleanup_storage_server(storage_server):
    """
    Delete all of the shares held by the given storage server.

    :param allmydata.storage.server.StorageServer storage_server: The storage
        server with some on-disk shares to delete.
    """
    starts = [
        FilePath(storage_server.sharedir),
        FilePath(storage_server.corruption_advisory_dir),
    ]
    for start in starts:
        for p in start.walk():
            if p is not start:
                p.remove()


def write_toy_shares(
        storage_server,
        storage_index,
        renew_secret,
        cancel_secret,
        sharenums,
        size,
        canary,
):
    """
    Write some immutable shares to the given storage server.

    :param allmydata.storage.server.StorageServer storage_server:
    :param bytes storage_index:
    :param bytes renew_secret:
    :param bytes cancel_secret:
    :param set[int] sharenums:
    :param int size:
    :param IRemoteReference canary:
    """
    _, allocated = storage_server.remote_allocate_buckets(
        storage_index,
        renew_secret,
        cancel_secret,
        sharenums,
        size,
        canary=canary,
    )
    for (sharenum, writer) in allocated.items():
        writer.remote_write(0, bytes_for_share(sharenum, size))
        writer.remote_close()


def whitebox_write_sparse_share(sharepath, version, size, leases, now):
    """
    Write a zero-filled sparse (if the filesystem supports it) immutable share
    to the given path.

    This assumes knowledge of the Tahoe-LAFS share file format.

    :param FilePath sharepath: The path to which to write the share file.
    :param int version: The share version to write to the file.
    :param int size: The share data size to write.
    :param list leases: Renewal secrets for leases to write to the share file.
    :param float now: The current time as a POSIX timestamp.
    """
    # Maybe-saturated size (what at least one Tahoe-LAFS comment claims is
    # appropriate for large files)
    internal_size = min(size, 2 ** 32 - 1)
    apparent_size = size

    header_format = ">LLL"
    lease_format = ">L32s32sL"
    with sharepath.open("wb") as share:
        share.write(
            pack(
                header_format,
                version,
                internal_size,
                len(leases),
            ),
        )
        # Try to make it sparse by skipping all the data.
        share.seek(apparent_size - 1, SEEK_CUR),
        share.write(b"\0")
        share.write(
            b"".join(
                pack(
                    lease_format,
                    # no owner
                    0,
                    renew,
                    # no cancel secret
                    b"",
                    # expiration timestamp
                    int(now + LEASE_INTERVAL),
                )
                for renew
                in leases
            ),
        )


def integer_passes():
    counter = count(0)
    def get_passes(message, num_passes):
        return list(islice(counter, num_passes))
    return get_passes


def pass_factory(get_passes=None):
    if get_passes is None:
        get_passes = integer_passes()
    return _PassFactory(get_passes=get_passes)


@attr.s
class _PassFactory(object):
    """
    A stateful pass issuer.

    :ivar (unicode -> int -> [bytes]) _get_passes: A function for getting
        passes.

    :ivar set[int] in_use: All of the passes given out without a confirmed
        terminal state.

    :ivar set[int] invalid: All of the passes given out and returned using
        ``IPassGroup.invalid``.

    :ivar set[int] spent: All of the passes given out and returned via
        ``IPassGroup.mark_spent``.

    :ivar set[int] issued: All of the passes ever given out.

    :ivar set[int] terminal: All of the passes ever given marked in a terminal
        state (invalid or spent).

    :ivar set[int] returned: A list of passes which were given out but then
        returned via ``IPassGroup.reset``.

    """
    _get_passes = attr.ib()

    returned = attr.ib(default=attr.Factory(list), init=False)
    in_use = attr.ib(default=attr.Factory(set), init=False)
    invalid = attr.ib(default=attr.Factory(dict), init=False)
    spent = attr.ib(default=attr.Factory(set), init=False)
    issued = attr.ib(default=attr.Factory(set), init=False)

    def get(self, num_passes):
        passes = []
        if self._returned:
            passes.extend(self._returned[:num_passes])
            del self._returned[:num_passes]
            num_passes -= len(passes)
        passes.extend(self._get_passes(num_passes))
        self.issued.update(passes)
        self.in_use.update(passes)
        return PassGroup(self, passes)

    def _mark_spent(self, passes):
        for p in passes:
            if p not in self.in_use:
                raise ValueError("Pass {} cannot be spent, it is not in use.".format(p))
        self.spent.update(passes)
        self.in_use.difference_update(passes)

    def _mark_invalid(self, reason, passes):
        for p in passes:
            if p not in self.in_use:
                raise ValueError("Pass {} cannot be invalid, it is not in use.".format(p))
        self.invalid.update(dict.fromkeys(passes, reason))
        self.in_use.difference_update(passes)

    def _reset(self, passes):
        for p in passes:
            if p not in self.in_use:
                raise ValueError("Pass {} cannot be reset, it is not in use.".format(p))
        self._returned.extend(passes)
        self.in_use.difference_update(passes)
