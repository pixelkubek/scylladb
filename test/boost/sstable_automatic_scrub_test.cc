/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "seastarx.hh"

#include <seastar/testing/test_case.hh>

#include "test/boost/sstable_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_utils.hh"

static future<> do_test_automatic_scrub(cql_test_env& env) {
    co_return;
}

static future<> test_automatic_scrub() {
    return do_with_cql_env(do_test_automatic_scrub);
}

SEASTAR_TEST_CASE(test_automatic_scrub_works) {
    return test_automatic_scrub();
}

// Test scrub timestamp update
// 
// Test automatic scrub -> trigger & compare_sstables