// Splaying LSM tree
// Luca Schroeder, Thomas Lively, Carlos Mendizabal

#include "../splaylsm/splaylsm.h"

Options getOptions(
    int memtable_size,
    int cache_size,
    int multiplier
) {
    Options options;
    options.create_if_missing = true;

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

    // bloom filter options as defaults

    // block cache options (it's empty)
    BlockBasedTableOptions table_options;
    if (cache_size) {
    table_options.block_cache = rocksdb::NewLRUCache(cache_size, 0, true);
    } else {
    table_options.no_block_cache = true;
    }
    ColumnFamilyOptions column_family_options;
    column_family_options.table_factory.reset(
        NewBlockBasedTableFactory(table_options));

    // os cache options (do not use OS cache for IO)
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = true;
    // options.writable_file_max_buffer_size = 4096;

    // block size is 4096 by default, configurable using BlockBasedTableOptions

    return options;
}

LSMTree::LSMTree(std::string &name) {
    options = getOptions(memtable_size, cache_size, multiplier);
    Status s = DB::Open(options, name, &db);
    assert(s.ok());
}

LSMTree::Insert(const Slice& key, const Slice& value) {
    s = db->Put(WriteOptions(), key, value);
    assert(s.ok());

    return s;
}

LSMTree::Get(const Slice& key, PinnableSlice* value) {
    s = db->Get(ReadOptions(), key, &value);
    assert(s.ok());

    return s;
}

