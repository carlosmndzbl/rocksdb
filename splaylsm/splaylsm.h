// Splaying LSM tree
// Luca Schroeder, Thomas Lively, Carlos Mendizabal

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"

using namespace rocksdb;

class LSMTree {
    DB* db;
    Options options;
    Status s;

    const int memtable_size = 262144; // 256 KB
    const int cache_size = 262144; // 256 KB
    const int multiplier = 10;

    bool is_splay_;

    public:
        LSMTree (std::string &name, bool isSplay);
        ~LSMTree ();
        Status Insert(const Slice& key, const Slice& value);
        Status Get(const Slice& key, std::string *value);
};
