#include "compaction_auto_scrub.hh"
#include "db_clock.hh"
#include "seastar/core/future.hh"
#include "utils/assert.hh"
#include <optional>
#include "sstables/sstables.hh"

namespace compaction {
automatic_scrub_manager::automatic_scrub_manager(sstables::sstable& sst)
    : _sys_ks("automatic_scrub_manager::system_keyspace")
    , _enabled(false)
    , _sst(sst)
{
}

void automatic_scrub_manager::plug(db::system_keyspace& sys_ks) {
    _sys_ks.plug(sys_ks.shared_from_this());
}

future<> automatic_scrub_manager::unplug() {
    return _sys_ks.unplug();
}

bool automatic_scrub_manager::plugged() const noexcept {
    return _sys_ks.plugged();
}

future<> automatic_scrub_manager::update_timestamp(db_clock::time_point ts) {
    auto permit = _sys_ks.get_permit();
    auto tid = _sst.get_schema()->id();

    if (permit) {
        if (_enabled) {
            co_await permit->auto_scrub_update_entry(tid, ts);
            co_return;
        }

        co_await permit->auto_scrub_create_entry(tid, ts);
        _enabled = true;
    }

    co_return;
}

future<std::optional<db_clock::time_point>> automatic_scrub_manager::get_timestamp() {
    auto permit = _sys_ks.get_permit();
    auto tid = _sst.get_schema()->id();

    if (permit) {
        auto stored = co_await permit->auto_scrub_get_entry(tid);
        co_return *stored;
    }

    co_return std::nullopt;
}

bool automatic_scrub_manager::enabled() const noexcept {
    return _enabled;
}


future<> automatic_scrub_manager::disable() {
    auto permit = _sys_ks.get_permit();
    auto tid = _sst.get_schema()->id();

    if (permit) {
        co_await permit->auto_scrub_delete_entry(tid);
        _enabled = false;
    }

    co_await _sys_ks.unplug();
}

automatic_scrub_manager::~automatic_scrub_manager() noexcept {
    SCYLLA_ASSERT(!_enabled);
}

} // namespace compaction