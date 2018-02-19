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
    static const int multiplier = 2;

    const int cache_size_;
    const int memtable_size_;
    const bool is_splay_;

    public:
        LSMTree (std::string &name, bool isSplay);
        ~LSMTree ();
        Status Insert(const Slice& key, const Slice& value);
        Status Get(const Slice& key, std::string *value);
};

// used as a prefix for values in a splaying LSMTree
struct SplayTag {
    uint8_t splayed : 1;
    uint8_t merged : 1;
};
