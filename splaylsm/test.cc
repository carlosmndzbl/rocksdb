// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <cstdio>
#include <iostream>
#include <algorithm>
#include <string>
#include <vector>
#include <random>

#include <stdio.h>
#include <time.h>

#include "../splaylsm/splaylsm.cc"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

namespace splay_test_exp {
std::string kDBPath = "/tmp/splay_test";

/*
 * Tests workload where @query_pct % of queries go to @range_pct % of keys
 */
void experiment1(
    int num_keys,
    int num_ops,
    int query_pct,
    int range_pct,
    int prob_write,
    int num_repeats,
    bool splay_enabled
    ) {
    Status s;

    // keep track of total time for average
    float total_time = 0.0;

    if (splay_enabled) {
        printf("Splaying is enabled\n");        
    } else {
        printf("Splaying is disabled\n");
    }

    for (int j = 0; j < num_repeats; j++) {
        // create DB
        LSMTree* db = new LSMTree(kDBPath, splay_enabled);
        assert(db != NULL);

        // insert initial data
        std::vector<int> initial_elements(num_keys);
        std::iota(initial_elements.begin(), initial_elements.end(), 1);
        std::shuffle(
        initial_elements.begin(),
        initial_elements.end(),
        std::mt19937{std::random_device{}()}
        );
        for (int i = 0; i < num_keys; i++) {
            int intKey = initial_elements[i];
            int intValue = intKey; // same for now

            Slice key((char*)&intKey, sizeof(int));
            Slice value((char*)&intValue, sizeof(int));

            s = db->Insert(key, value);
            if (!s.ok()) {
                fprintf(stderr, "Error on Insert(): %s\n", s.ToString().c_str());
                std::abort();
            }
        }

        // get bounds for the target range
        int range_max = (num_keys / 100) * range_pct;

        // uniform distributions for operations
        std::random_device rand_dev;
        std::mt19937 gen{rand_dev()};
        std::uniform_int_distribution<int> unif_in_range(1, range_max);
        std::uniform_int_distribution<int> unif_out_of_range(range_max + 1, num_keys);

        // distribution for probabilty of selecting range
        std::uniform_int_distribution<int> unif_percent(1, 100);

        // begin counting clock ticks while running
        clock_t t;
        t = clock();

        // inserts/gets
        for (int i = 0; i < num_ops; i++) {
            int intKey = query_pct >= unif_percent(gen)
                ? unif_in_range(gen)
                : unif_out_of_range(gen);
            int intValue = intKey;

            Slice key((char*)&intKey, sizeof(int));
            Slice value((char*)&intValue, sizeof(int));

            if (prob_write >= unif_percent(gen)) {
                s = db->Insert(key, value);
                if (!s.ok()) {
                fprintf(stderr, "Error on Put(): %s\n", s.ToString().c_str());
                std::abort();
                }
            } else {
                std::string val_res;
                s = db->Get(key, &val_res);
            }
        }

        // total clock ticks
        t = clock() - t;
        total_time += (float)t;
        printf("Runtime: %f seconds\n",((float)t)/CLOCKS_PER_SEC);

        // close db
        delete db;
        system("rm -rf /tmp/splay_test");
    }

    printf("Average runtime: %f seconds\n", (total_time/num_repeats)/CLOCKS_PER_SEC);
    printf("----------------------------------------------------------\n");

    return;
}

}

int main() {
    using namespace splay_test_exp;

    // standard workload variables
    int num_keys = 1000000;             // 1 million
    int num_ops = 1000000;              // 1 million
    int test_repeats = 5

    for (int write_pct = 0; write_pct <= 30; write_pct += 5) {
        for (int range_pct = 2; range_pct <= 30; range_pct += 2) {
            // query_pct % of queries go to range_pct % of keys
            int query_pct = 100 - range_pct;
            printf(
                "Workload: %d %% of queries go to %d %% of keys. %d %% operations are writes.\n",
                query_pct,
                range_pct,
                write_pct
            );

            experiment1(
                num_keys,
                num_ops,
                query_pct,
                range_pct,
                write_pct,
                test_repeats,
                true
            );

            experiment1(
                num_keys,
                num_ops,
                query_pct,
                range_pct,
                write_pct,
                test_repeats,
                false
            );
        }
    }

    return 0;
}
