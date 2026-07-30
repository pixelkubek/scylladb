# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.manager_client import ManagerClient

import pytest
import logging

logger = logging.getLogger(__name__)

async def test_automatic_scrub(manager: ManagerClient):
    server = await manager.server_add(config={"compaction_scrub_period": "10"})

    log_file = await manager.server_open_log(server.server_id)
    mark = await log_file.mark()

    await log_file.wait_for("Performing automatic scrub for sstables", from_mark=mark, timeout=20)
    