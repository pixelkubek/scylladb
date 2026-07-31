# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace

import asyncio
import pytest
import logging

logger = logging.getLogger(__name__)

NUM_TABLES = 3
NUM_SSTABLES_PER_TABLE = 3
ROWS_PER_SSTABLE = 20

async def test_automatic_scrub(manager: ManagerClient):
    server = await manager.server_add(config={"compaction_scrub_period": "10"})

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = "
                                  "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        tables = [f"tbl{i}" for i in range(NUM_TABLES)]

        for table in tables:
            await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, c int)")
            await manager.api.disable_autocompaction(server.ip_addr, ks, table)

        # produce multiple sstables per table by inserting a batch of rows
        # and flushing after each batch
        for i in range(NUM_SSTABLES_PER_TABLE):
            for table in tables:
                base = i * ROWS_PER_SSTABLE
                await asyncio.gather(*[
                    cql.run_async(f"INSERT INTO {ks}.{table} (pk, c) VALUES ({k}, {k})")
                    for k in range(base, base + ROWS_PER_SSTABLE)
                ])
                await manager.api.keyspace_flush(server.ip_addr, ks, table)

        log_file = await manager.server_open_log(server.server_id)
        mark = await log_file.mark()

        await log_file.wait_for("Performing automatic scrub for sstables", from_mark=mark, timeout=20)
