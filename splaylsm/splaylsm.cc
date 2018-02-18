// Splaying LSM tree
// Luca Schroeder, Thomas Lively, Carlos Mendizabal

#include "../splaylsm/splaylsm.h"
#include "../splaylsm/splay_filter.h"

void setOptions(
    int memtable_size,
    int cache_size,
    int multiplier,
    Options *options
) {
    options->create_if_missing = true;

    // filter & merge options
    options->merge_operator.reset(new SplayMerge);
    options->compaction_filter = new SplayFilter;

    // compaction style and level options
    options->compaction_style = kCompactionStyleLevel;

    // memtable options
    options->write_buffer_size = memtable_size;
    options->max_write_buffer_number = 1;                  // # of memtables
    options->min_write_buffer_number_to_merge = 1;

    // level 0 options: set the size of Level 0 to @multiplier times memtable size
    options->level0_file_num_compaction_trigger = multiplier;
    options->level0_slowdown_writes_trigger = multiplier;
    options->level0_stop_writes_trigger = multiplier;
    options->target_file_size_base = memtable_size;

    // other level options: L1 is same size as L0 per RocksDB recommendation
    options->max_bytes_for_level_base = multiplier * memtable_size;
    options->max_bytes_for_level_multiplier = multiplier;

    // options->num_levels is 7 by default

    // compaction trigger options
    options->max_background_compactions = 1;
    options->max_background_flushes = 1;

    // block cache options (it's empty)
    BlockBasedTableOptions table_options;
    if (cache_size) {
    table_options.block_cache = rocksdb::NewLRUCache(cache_size, 0, true);
    } else {
    table_options.no_block_cache = true;
    }
    // bloom filter per file
    table_options.filter_policy.reset(
        NewBloomFilterPolicy(10, false)
    );
    ColumnFamilyOptions column_family_options;
    column_family_options.table_factory.reset(
        NewBlockBasedTableFactory(table_options)
    );

    // os cache options (do not use OS cache for IO)
    options->use_direct_reads = true;
    options->use_direct_io_for_flush_and_compaction = true;
    // options->writable_file_max_buffer_size = 4096;

    // block size is 4096 by default, configurable using BlockBasedTableOptions
}

LSMTree::LSMTree(std::string &name, bool isSplay) {
    is_splay_ = isSplay;
    Options options;
    if (isSplay) {
        // no cache & bigger memtable
        // (TODO: make this nicer, no need to have these as instance vars)
        cache_size = 0;
        memtable_size *= 2;
    }
    setOptions(memtable_size, cache_size, multiplier, &options);
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
    Status s = db->Put(WriteOptions(), key, value);
    assert(s.ok());

    return s;
}

Status LSMTree::Get(const Slice& key, std::string *stringVal) {
    Status s = db->Get(ReadOptions(), key, stringVal);

    if (s.ok() && is_splay_) {
        Slice value(splayed_val);
        s = db->Put(WriteOptions(), key, value);
        assert(s.ok());
    }

    return s;
}
