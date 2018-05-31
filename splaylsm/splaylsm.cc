// Splaying LSM tree
// Luca Schroeder, Thomas Lively, Carlos Mendizabal

#include "../splaylsm/splaylsm.h"
#include "../splaylsm/splay_filter.h"
#include "../include/rocksdb/filter_policy.h"

void setOptions(Options& options,
                int memtable_size,
                int cache_size,
                int multiplier,
                bool is_splay) {
    options.create_if_missing = true;

    // filter & merge options
    options.merge_operator.reset(new SplayMerge);
    if (is_splay) {
        options.compaction_filter = new SplayFilter;
    }

    // compaction style and level options
    options.compaction_style = kCompactionStyleLevel;

    // memtable options
    options.write_buffer_size = memtable_size;
    options.max_write_buffer_number = 1;                  // # of memtables
    options.min_write_buffer_number_to_merge = 1;

    // level 0 options: set the size of Level 0 to @multiplier times memtable size
    options.level0_file_num_compaction_trigger = multiplier;
    options.level0_slowdown_writes_trigger = multiplier;
    options.level0_stop_writes_trigger = multiplier;
    options.target_file_size_base = memtable_size;

    // other level options: L1 is same size as L0 per RocksDB recommendation
    options.max_bytes_for_level_base = multiplier * memtable_size;
    options.max_bytes_for_level_multiplier = multiplier;

    // options.num_levels is 7 by default

    // compaction trigger options
    options.max_background_compactions = 1;
    options.max_background_flushes = 1;

    // block cache options (it's empty)
    BlockBasedTableOptions table_options;
    if (cache_size) {
    table_options.block_cache = rocksdb::NewLRUCache(cache_size, 0, true);
    } else {
    table_options.no_block_cache = true;
    }
    // no bloom filters
    table_options.filter_policy.reset((FilterPolicy *) nullptr);

    ColumnFamilyOptions column_family_options;
    column_family_options.table_factory.reset(
        NewBlockBasedTableFactory(table_options)
    );

    // os cache options (do not use OS cache for IO)
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
    // options.writable_file_max_buffer_size = 4096;

    // block size is 4096 by default, configurable using BlockBasedTableOptions
}

LSMTree::LSMTree(std::string &name, bool is_splay) :
    is_splay_(is_splay),
    cache_size_(is_splay ? 0 : 131072), // 128 KB
    memtable_size_(is_splay ? (2 * 131072) : 131072) {
    Options options;
    setOptions(options, memtable_size_, cache_size_, multiplier, is_splay);
    Status s = DB::Open(options, name, &db);
    if (!s.ok()) {
        fprintf(stderr, "Can't open database: %s\n", s.ToString().c_str());
        std::abort();
    }
}

LSMTree::~LSMTree() {
    delete db;
}

Status LSMTree::Insert(const Slice& key, const Slice& value) {
    if (is_splay_) {
        // insert new value with tag prefix
        static_assert(sizeof(SplayTag) == 1, "SplayTag does not fit in a byte");
        char buf[sizeof(SplayTag) + value.size()];
        SplayTag* tag = reinterpret_cast<SplayTag*>(&buf);
        tag->splayed = 0;
        tag->merged = 0;
        std::memcpy(buf + sizeof(SplayTag), value.data(), value.size());
        return db->Put(WriteOptions(), key, Slice(buf, sizeof(buf)));
    } else {
        // flags not necessary
        return db->Put(WriteOptions(), key, value);
    }
}

Status LSMTree::Get(const Slice& key, std::string *stringVal) {
    Status s = db->Get(ReadOptions(), key, stringVal);
    if (s.ok() && is_splay_) {
        assert(stringVal->size() >= sizeof(SplayTag));
        SplayTag* tag = reinterpret_cast<SplayTag*>(&stringVal->front());
        if (tag->merged) {
            SplayTag new_tag = {.merged = 0, .splayed = 1};
            stringVal->replace(0,
                               sizeof(tag),
                               reinterpret_cast<char*>(&new_tag),
                               sizeof(new_tag));
            Status t = db->Put(WriteOptions(), key, Slice(*stringVal));
            assert(t.ok());
        }
        // TODO: drop prefix for returned string
    }
    return s;
}
