#include "db.h"
#include "trx.h"

#include <gtest/gtest.h>

#include <pthread.h>

#include <string>
#include <random>
#include <algorithm>

extern BufferManager buffer_manager;

std::string get_random_string(int length) {
    std::string ret;

    std::mt19937 engine(time(NULL));
    std::uniform_int_distribution<char> dis('a', 'z');
    auto gen = std::bind(dis, engine);

    for(int i = 0; i < length; ++i) {
        ret += gen();
    }

    return ret;
}

void make_random_tree(int64_t tid, int tree_sz, std::vector<std::string>& values, std::vector<int64_t>& keys) {
    for(int i = 0; i < tree_sz; ++i) {
        keys.push_back(i);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);

    std::uniform_int_distribution<int> dis(50, 112);
    auto gen = std::bind(dis, g);

    for(int i = 0; i < tree_sz; ++i) {
        // random value size
        uint16_t val_size = gen();
        values.push_back(get_random_string(val_size));
        db_insert(tid, keys[i], values[i].c_str(), val_size);
    }
}

TEST(SingleThreadTxnTest, SLockOnlyTest) {
    const char* pathname = "single_thread_txn_test.db";
    if(!std::remove(pathname))
        std::cout << "Remove existing file " << pathname << std::endl;

    init_db(64);
    int64_t table_id = open_table("single_thread_txn_test.db");

    std::vector<std::string> values;
    std::vector<int64_t> keys;

    const int tree_size = 1000;
    make_random_tree(table_id, tree_size, values, keys);
    std::cout << "Random tree has been generated." << std::endl;

    /* Transaction Begin */
    int trx_1 = trx_begin();
    EXPECT_GT(trx_1, 0);

    std::cout << "Transaction 1 has been started." << std::endl;

    for(int i = 0; i < 100; ++i) {
        char buf[150]; uint16_t val_size;
        int64_t key = keys[i];
        int flag = db_find(table_id, keys[i], buf, &val_size, trx_1);

        EXPECT_EQ(flag, 0)
            << "db_find failed | index : " << i << " | key : " << key << '\n';

        EXPECT_EQ(
            std::string(buf, val_size),
            values[i]
        ) 
        << "Not an expected value. | index : " << i << " | key : " << key << '\n'
        << "Expected : " << values[key] << '\n'
        << "Actual : " << std::string(buf, val_size) << '\n';
    }

    std::cout << "Try to commit Transaction 1" << std::endl;
    EXPECT_EQ(trx_commit(trx_1), trx_1)
        << "trx_commit failed | trx_id : " << trx_1 << '\n';
    /* Transaction End */

    std::cout << "Transaction 1 has been committed." << std::endl;

    shutdown_db();

    std::cout << "DB has been shutdown." << std::endl;
}