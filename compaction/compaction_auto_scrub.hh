#pragma once

#include "db/system_keyspace.hh"
#include "utils/pluggable.hh"

namespace sstables {
    class sstable;
}

namespace compaction {

class automatic_scrub_manager {
    utils::pluggable<db::system_keyspace> _sys_ks;
    bool _enabled;
    sstables::sstable& _sst;

public:
    explicit automatic_scrub_manager(sstables::sstable& sst);

    void plug(db::system_keyspace&);
    future<> unplug();
    bool plugged() const noexcept;
    
    future<> enable();

    future<> update_timestamp(db_clock::time_point tp);

    future<std::optional<db_clock::time_point>> get_timestamp();

    bool enabled() const noexcept;

    future<> disable();

    ~automatic_scrub_manager() noexcept;
};

} // namespace compaction