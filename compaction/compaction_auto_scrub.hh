#pragma once

#include "db/system_keyspace.hh"
#include "utils/pluggable.hh"
#include "sstables/sstables.hh"

namespace compaction {

class automatic_scrub_manager {
    utils::pluggable<db::system_keyspace> _sys_ks;
    bool _enabled;
    sstables::sstable& _sst;

public:
    explicit automatic_scrub_manager(db::system_keyspace& sys_ks, sstables::sstable& sst);
    
    future<> enable();

    future<> update_timestamp(db_clock::time_point tp);

    future<std::optional<db_clock::time_point>> get_timestamp();

    future<> disable();

    ~automatic_scrub_manager() noexcept;
};

} // namespace compaction