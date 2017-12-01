// Splaying LSM tree
// Luca Schroeder, Thomas Lively, Carlos Mendizabal

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"

using namespace rocksdb;

class LSMTree {
    DB* db;
    Options options;
    Status s;

    int memtable_size = 131072; // 128 KB
    int cache_size = 131072; // 128 KB
    const int multiplier = 2;

    bool is_splay_;

    public:
        LSMTree (std::string &name, bool isSplay);
        ~LSMTree ();
        Status Insert(const Slice& key, const Slice& value);
        Status Get(const Slice& key, std::string *value);
};
