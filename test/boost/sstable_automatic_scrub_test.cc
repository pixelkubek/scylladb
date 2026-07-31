/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/testing/thread_test_case.hh>
#include "readers/from_mutations.hh"
#include "seastarx.hh"

#include <seastar/testing/test_case.hh>

#include "sstables/sstable_writer.hh"
#include "test/boost/sstable_test.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/random_utils.hh"

namespace {

static future<std::vector<sstables::shared_sstable>> get_all_sstables(compaction::compaction_group_view& t) {
    auto main_set = co_await t.main_sstable_set();
    auto maintenance_set = co_await t.maintenance_sstable_set();
    auto s = *main_set->all() | std::ranges::to<std::vector>();
    auto maintenance_sstables = maintenance_set->all();
    s.insert(s.end(), maintenance_sstables->begin(), maintenance_sstables->end());
    co_return s;
}

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

    shared_sstable make_sstable() {
        auto muts = tests::generate_random_mutations(
                random_schema(),
                tests::uncompactible_timestamp_generator(seed()),
                tests::no_expiry_expiry_generator(),
                std::uniform_int_distribution<size_t>(10, 10)).get();
        
        // auto sst = make_sstable(schema(), explode(env().make_reader_permit(), std::move(muts)));

        auto sst = make_sstable_containing(env().make_sstable(schema()), std::move(muts)).get();

        
        return sst;
    }

    table_for_tests make_table() {
        auto table = env().make_table_for_tests(schema());
        table->start();

        for (size_t i = 0; i < 2; i++) {
            auto sst = make_sstable();
            table->add_sstable_and_update_cache(sst, offstrategy::yes).get();
        }

        return table;
    }

    void run(test_func func) {
        auto table = make_table();
        auto close_cf = deferred_stop(table);

        auto& cgv = table.as_compaction_group_view();
        func(table, cgv, get_all_sstables(cgv).get());
        
        // bool found_sstable = false;        
        // foreach_compaction_group_view_with_thread(table, [&] (compaction::compaction_group_view& ts) {
        //     auto sstables = get_all_sstables(ts).get();
        //     if (sstables.empty()) {
        //         return;
        //     }
        //     found_sstable = true;

        //     func(table, ts, sstables);
        // }).get();
        // BOOST_REQUIRE(found_sstable);
        // env().test_compaction_manager().get_compaction_manager().stop().get();
    }
};

// future<> cm_ended_some_work(const compaction::compaction_manager& cm) {
//     while (cm.get_stats().pending_tasks != 0 || cm.get_stats().completed_tasks == 0) {
//         co_await sleep(std::chrono::milliseconds(100));
//     }
// }

static void corrupt_sstable(sstables::shared_sstable sst, component_type type = component_type::Data) {
    auto f = sstables::test(sst).open_file(type, {}, {}).get();
    auto close_f = deferred_close(f);
    const auto wbuf_align = f.memory_dma_alignment();
    const auto wbuf_len = f.size().get();
    auto wbuf = seastar::temporary_buffer<char>::aligned(wbuf_align, wbuf_len);
    std::fill(wbuf.get_write(), wbuf.get_write() + wbuf_len, 0xba);
    auto os = output_stream<char>(sstables::test(sst).get_storage().make_component_sink(*sst, type, open_flags::wo, {}).get());
    auto close_os = deferred_close(os);
    os.write(std::move(wbuf)).get();
}


void auto_scrub_validate_corrupted_content() {
    scrub_test_framework test(tests::random_schema_specification::compress_sstable::yes);

    auto& test_env = test.env();

    test.run([&test_env] (table_for_tests& table, compaction::compaction_group_view& ts, std::vector<sstables::shared_sstable> sstables) {
        auto& cm = test_env.test_compaction_manager();

        BOOST_REQUIRE_GE(sstables.size(), 1);
        for (sstables::shared_sstable& sst : sstables) {
            corrupt_sstable(sst);
        }

        // corrupt_sstable(sst);

        cm.trigger_auto_scrub_timer();

        sleep(std::chrono::seconds(5)).wait();

        // cm_ended_some_work(cm.get_compaction_manager()).wait();

        BOOST_REQUIRE_EQUAL(table->get_sstables()->size(), sstables.size());
        BOOST_REQUIRE(table->get_sstables()->begin()->get()->is_quarantined());
    });
}
}

SEASTAR_THREAD_TEST_CASE(sstable_auto_scrub_test_corrupted_content) {
    auto_scrub_validate_corrupted_content();
}
