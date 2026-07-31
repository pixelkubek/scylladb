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
#include "test/lib/random_schema.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"

enum class random_schema { no, yes };
template <random_schema create_random_schema>
class scrub_test_framework {
public:
    using test_func = std::function<void(table_for_tests&, compaction::compaction_group_view&, std::vector<sstables::shared_sstable>)>;

private:
    std::unique_ptr<sstable_compressor_factory> scf = make_sstable_compressor_factory_for_tests_in_thread();
    sharded<test_env> _env;
    uint32_t _seed;
    std::unique_ptr<tests::random_schema_specification> _random_schema_spec;
    tests::random_schema _random_schema;
public:
    scrub_test_framework(tests::random_schema_specification::compress_sstable compress)
        : _seed(tests::random::get_int<uint32_t>())
        , _random_schema_spec(tests::make_random_schema_specification(
                "scrub_test_framework",
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 4),
                std::uniform_int_distribution<size_t>(2, 8),
                std::uniform_int_distribution<size_t>(2, 8),
                compress))
        , _random_schema(_seed, *_random_schema_spec)
    {
        _env.start(test_env_config(), std::ref(*scf)).get();
        testlog.info("random_schema: {}", _random_schema.cql());
    }

    ~scrub_test_framework() {
        _env.stop().get();
    }

    test_env& env() { return _env.local(); }
    uint32_t seed() const { return _seed; }
    tests::random_schema& random_schema() { return _random_schema; }
    schema_ptr schema() const { return _random_schema.schema(); }

    void run(schema_ptr schema, std::deque<mutation_fragment_v2> frags, test_func func) {
        auto& env = this->env();

        const auto partition_count = std::count_if(frags.begin(), frags.end(), std::mem_fn(&mutation_fragment_v2::is_partition_start));

        auto permit = env.make_reader_permit();
        auto mr = make_mutation_reader_from_fragments(schema, permit, clone(*schema, permit, frags));

        auto close_mr = deferred_close(mr);

        // The test violates key order on purpose.
        // That's illegal with the index writer of version `ms`.
        // So we can't use this test, as it is currently written, with `ms`.
        auto version = sstable_version_types::me;
        auto sst = env.make_sstable(schema, version);
        sstable_writer_config cfg = env.manager().configure_writer();
        cfg.validation_level = mutation_fragment_stream_validation_level::partition_region; // this test violates key order on purpose

        auto wr = sst->get_writer(*schema, partition_count, cfg, encoding_stats{});
        mr.consume_in_thread(std::move(wr));

        sst->load(schema->get_sharder()).get();

        auto table = env.make_table_for_tests(schema);
        auto close_cf = deferred_stop(table);
        table->start();

        table->add_sstable_and_update_cache(sst).get();

        verify_fragments({sst}, env.make_reader_permit(), frags);

        bool found_sstable = false;
        foreach_compaction_group_view_with_thread(table, [&] (compaction::compaction_group_view& ts) {
            auto sstables = in_strategy_sstables(ts).get();
            if (sstables.empty()) {
                return;
            }
            BOOST_REQUIRE(sstables.size() == 1);
            BOOST_REQUIRE(sstables.front() == sst);
            found_sstable = true;

            func(table, ts, sstables);
        }).get();
        BOOST_REQUIRE(found_sstable);
    }

    void run(schema_ptr schema, utils::chunked_vector<mutation> muts, test_func func) {
        run(std::move(schema), explode(env().make_reader_permit(), std::move(muts)), std::move(func));
    }
};

static future<> do_test_automatic_scrub(cql_test_env& env) {
    // env.
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