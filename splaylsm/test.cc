// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <cstdio>
#include <algorithm>
#include <string>
#include <vector>
#include <random>
#include <iostream>
#include <time.h>
#include <chrono>

#include "../splaylsm/splaylsm.cc"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

#define SEED ((std::random_device()()))
// #define SEED 0

namespace splay_test_exp {
    static std::string kDBPath = "/tmp/splay_test";

    struct Op {
        bool is_write;
        int val;
        Op(bool is_write, int val) : is_write(is_write), val(val) {}
    };

    void pareto_ops(std::vector<Op>& vec, int num_ops, int num_keys,
                    double pareto, double p_write) {
        assert(pareto >= 0 && pareto <= 1.0);
        assert(p_write >= 0 && pareto <= 1.0);
        int boundary = std::round(num_keys * pareto);
        assert(boundary > 0 && boundary < num_keys);
        std::mt19937 gen(SEED);
        std::uniform_int_distribution<int> range_a(0, boundary - 1);
        std::uniform_int_distribution<int> range_b(boundary, num_keys - 1);
        std::uniform_real_distribution<double> unif(0.0, 1.0);

        for (int i = 0; i < num_ops; ++i) {
            bool is_write = unif(gen) < p_write;
            int val = unif(gen) < pareto ? range_b(gen) : range_a(gen);
            assert(val >= 0 && val < num_keys);
            vec.emplace_back(Op(is_write, val));
        }
    }

    void range_ops(std::vector<Op>& vec, int num_ops,
                   int start, int end, double p_write) {
        assert(p_write >= 0 && p_write <= 1.0);
        assert(end > start);
        std::mt19937 gen(SEED);
        std::uniform_int_distribution<int> range(start, end - 1);
        std::uniform_real_distribution<double> unif(0.0, 1.0);
        for (int i = 0; i < num_ops; ++i) {
            bool is_write = unif(gen) < p_write;
            int val = range(gen);
            vec.emplace_back(Op(is_write, val));
        }
    }

    void do_ops(LSMTree& db,
                std::vector<Op>::const_iterator start,
                std::vector<Op>::const_iterator end) {
        for (auto it = start; it != end; ++it) {
            Slice key((char*)&it->val, sizeof(it->val));
            if (it->is_write) {
                if (!db.Insert(key, key).ok()) {
                    std::cerr << "Error on insert" << std::endl;
                    std::abort();
                }
            } else {
                std::string res;
                Status s = db.Get(key, &res);
                if (!s.ok()) {
                    std::cerr << "Error on get " << s.ToString()
                              << it->val << std::endl;
                    std::abort();
                }
            }
        }
    }


    void setup(LSMTree& db, size_t num_keys) {
        // insert initial data
        std::vector<int> initial_elements(num_keys);
        std::iota(initial_elements.begin(), initial_elements.end(), 0);
        std::shuffle(
            initial_elements.begin(),
            initial_elements.end(),
            std::mt19937(SEED)
        );
        for (size_t i = 0; i < num_keys; i++) {
            static char val_buf[128 - sizeof(int)];
            Slice key((char*)&initial_elements[i], sizeof(int));
            Slice val(val_buf, sizeof(val_buf));
            if (!db.Insert(key, val).ok()) {
                std::cerr << "Error on Insert in setup" << std::endl;
                std::abort();
            }
        }
    }


    /*
     * Tests workload where 1-@pareto of queries go to @pareto of keys
     */
    void standard_experiment_trial(
            int num_keys,
            int warmup_ops,
            int num_ops,
            double pareto,
            double p_write,
            int num_repeats,
            bool splay_enabled) {

        // keep track of total time for average
        float total_time = 0.0;

        for (int j = 0; j < num_repeats; j++) {
            // create DB
            LSMTree db = LSMTree(kDBPath, splay_enabled);
            setup(db, num_keys);

            std::vector<Op> ops;
            pareto_ops(ops, warmup_ops + num_ops, num_keys, pareto, p_write);

            // warmup
            do_ops(db, ops.cbegin(), ops.cbegin() + warmup_ops);

            clock_t t;
            t = clock();

            // inserts/gets
            do_ops(db, ops.cbegin() + warmup_ops, ops.cend());

            // total clock ticks
            t = clock() - t;
            total_time += (float)t;

            // see standard_experiment() for format
            std::cout << "single,"
                      << (splay_enabled ? "splay-enabled," : "splay-disabled,")
                      << p_write << ","
                      << pareto << ","
                      << (float)t/CLOCKS_PER_SEC << std::endl;

            // close db
            system("rm -rf /tmp/splay_test");
        }

        // also print average
        std::cout << "avg,"
                  << (splay_enabled ? "splay-enabled," : "splay-disabled,")
                  << p_write << ","
                  << pareto << ","
                  << total_time/num_repeats/CLOCKS_PER_SEC << std::endl;
    }

    void varying_workload_experiment() {
        static int num_trials = 3;
        static int num_keys = 1000000; // 1 mil
        static int time_interval = 10000;
        // ops, min_key, max_key
        static int params[5][3] = {
            {400000, 0,      100000},
            {200000, 50000,  150000},
            {200000, 100000, 200000},
            {200000, 200000, 300000},
            {200000, 300000, 400000}
        };

        auto run_trial = [&](bool splay) {
            LSMTree db(kDBPath, splay);
            setup(db, num_keys);
            std::vector<Op> ops;
            for (int* workload : params) {
                range_ops(ops, workload[0], workload[1], workload[2], 0);
            }
            for (auto it = ops.cbegin();
                 it + time_interval <= ops.cend();
                 it += time_interval) {
                auto t1 = std::chrono::high_resolution_clock::now();
                do_ops(db, it, it + time_interval);
                auto t2 = std::chrono::high_resolution_clock::now();
                std::cout << (splay ? "splay" : "nosplay") << ","
                          << (it - ops.cbegin()) << ","
                          << std::chrono::duration<double>(t2 - t1).count()
                          << std::endl;
            }
            system(("rm -rf " + kDBPath).c_str());
        };

        run_trial(true);
        run_trial(false);
    }

    void standard_experiment() {
        // standard workload variables
        int num_keys = 100000;             // 100k
        int warmup_ops = 100000;
        int num_ops = 100000;              // 100k
        int test_repeats = 30;

        // format: single/avg,splay-enabled/splay-disabled,p_write,pareto,seconds

        for (double p_write = 0; p_write <= .3; p_write += .5) {
            for (double pareto = .02; pareto <= .4; pareto += .02) {
                // query_pct % of queries go to range_pct % of keys

                standard_experiment_trial(
                    num_keys,
                    warmup_ops,
                    num_ops,
                    pareto,
                    p_write,
                    test_repeats,
                    true
                );

                standard_experiment_trial(
                    num_keys,
                    warmup_ops,
                    num_ops,
                    pareto,
                    p_write,
                    test_repeats,
                    false
                );
            }
        }
    }
}

int main() {
    using namespace splay_test_exp;

    // varying_workload_experiment();
    standard_experiment();
}
