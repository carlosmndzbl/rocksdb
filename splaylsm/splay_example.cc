// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>

#include "../splaylsm/splaylsm.h"

std::string kDBPath = "/tmp/splay_simple_example";

int main() {
    Status s;

    LSMTree* db = new LSMTree(kDBPath);
    assert(db != NULL);

    // Put key-value
    s = db->Insert("key1", "value");
    assert(s.ok());
    std::string value;
    // get value
    s = db->Get("key1", &value);
    assert(s.ok());
    assert(value == "value");

    delete db;

    return 0;
}
