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
    const char* pathname = "single_thread_txn_test_s.db";
    if(!std::remove(pathname))
        std::cout << "Remove existing file " << pathname << std::endl;

    init_db(64);
    int64_t table_id = open_table(pathname);

    std::vector<std::string> values;
    std::vector<int64_t> keys;

    const int tree_size = 10000;
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
            << "db_find \\w trx failed | index : " << i << " | key : " << key << '\n';

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

TEST(SingleThreadTxnTest, XLockOnlyTest) {
    const char* pathname = "single_thread_txn_test_x.db";
    if(!std::remove(pathname))
        std::cout << "Remove existing file " << pathname << std::endl;

    init_db(64);
    int64_t table_id = open_table(pathname);

    std::vector<std::string> values;
    std::vector<int64_t> keys;

    const int tree_size = 10000;
    make_random_tree(table_id, tree_size, values, keys);
    std::cout << "Random tree has been generated." << std::endl;

    /* Transaction Begin */
    int trx_1 = trx_begin();
    EXPECT_GT(trx_1, 0);

    std::cout << "Transaction 1 has been started." << std::endl;

    for(int i = 0; i < 100; ++i) {
        char buf[150]; uint16_t old_val_size;

        std::string new_value = get_random_string(values[i].size());
        int64_t key = keys[i];
        int flag = db_update(table_id, keys[i], (char*)new_value.c_str(), new_value.size(), &old_val_size, trx_1);
        values[i] = new_value;

        EXPECT_EQ(flag, 0)
            << "db_update \\w trx failed | index : " << i << " | key : " << key << '\n';
    }

    std::cout << "Try to commit Transaction 1" << std::endl;
    EXPECT_EQ(trx_commit(trx_1), trx_1)
        << "trx_commit failed | trx_id : " << trx_1 << '\n';
    /* Transaction End */

    std::cout << "Transaction 1 has been committed." << std::endl;

    std::cout << "Validation process started." << std::endl;
    for(int i = 0; i < 100; ++i) {
        char buf[150]; uint16_t val_size;
        int64_t key = keys[i];
        int flag = db_find(table_id, keys[i], buf, &val_size);

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
    std::cout << "Validation completed." << std::endl;

    shutdown_db();

    std::cout << "DB has been shutdown." << std::endl;
}

TEST(SingleThreadTxnTest, SXLockTest) {
    const char* pathname = "single_thread_txn_test_sx.db";
    if(!std::remove(pathname))
        std::cout << "Remove existing file " << pathname << std::endl;

    init_db(64);
    int64_t table_id = open_table(pathname);

    std::vector<std::string> values;
    std::vector<int64_t> keys;

    const int tree_size = 10000;
    make_random_tree(table_id, tree_size, values, keys);
    std::cout << "Random tree has been generated." << std::endl;

    /* Transaction Begin */
    int trx_1 = trx_begin();
    EXPECT_GT(trx_1, 0);

    std::cout << "Transaction 1 has been started." << std::endl;

    for(int i = 0; i < 100; ++i) {
        /* find part */
        char fbuf[150]; uint16_t val_size;
        int64_t fkey = keys[i];
        int fflag = db_find(table_id, keys[i], fbuf, &val_size, trx_1);

        EXPECT_EQ(fflag, 0)
            << "db_find \\w trx failed | index : " << i << " | key : " << fkey << '\n';

        EXPECT_EQ(
            std::string(fbuf, val_size),
            values[i]
        )
        << "Not an expected value. | index : " << i << " | key : " << fkey << '\n'
        << "Expected : " << values[fkey] << '\n'
        << "Actual : " << std::string(fbuf, val_size) << '\n';

        /* update part */
        char buf[150]; uint16_t old_val_size;

        std::string new_value = get_random_string(values[i].size());
        int64_t key = keys[i];
        int flag = db_update(table_id, keys[i], (char*)new_value.c_str(), new_value.size(), &old_val_size, trx_1);
        values[i] = new_value;

        EXPECT_EQ(flag, 0)
            << "db_update \\w trx failed | index : " << i << " | key : " << key << '\n';
    
        /* find part */
        char rfbuf[150]; uint16_t rfval_size;
        int64_t rfkey = keys[i];
        int rfflag = db_find(table_id, keys[i], rfbuf, &rfval_size, trx_1);

        EXPECT_EQ(rfflag, 0)
            << "db_find \\w trx failed | index : " << i << " | key : " << fkey << '\n';

        EXPECT_EQ(
            std::string(rfbuf, rfval_size),
            values[i]
        )
        << "Not an expected value. | index : " << i << " | key : " << rfkey << '\n'
        << "Expected : " << values[rfkey] << '\n'
        << "Actual : " << std::string(rfbuf, rfval_size) << '\n';
    }

    std::cout << "Try to commit Transaction 1" << std::endl;
    EXPECT_EQ(trx_commit(trx_1), trx_1)
        << "trx_commit failed | trx_id : " << trx_1 << '\n';
    /* Transaction End */

    std::cout << "Transaction 1 has been committed." << std::endl;

    std::cout << "Validation process started." << std::endl;
    for(int i = 0; i < 100; ++i) {
        char buf[150]; uint16_t val_size;
        int64_t key = keys[i];
        int flag = db_find(table_id, keys[i], buf, &val_size);

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
    std::cout << "Validation completed." << std::endl;

    shutdown_db();

    std::cout << "DB has been shutdown." << std::endl;
}

/****************************************************************************************/
/*                                  Multi Thread Test                                   */  
/****************************************************************************************/

#define THREAD_N 20
#define RD_N 500
#define path_multi_thread_rd "multi_thread_rd.db"

pthread_t threads[THREAD_N];

std::vector<std::string> multi_rd_values;
std::vector<int64_t> multi_rd_keys;

void* thread_rnd_read(void* arg) {
    int64_t table_id = *((int*)arg);

    /* Transaction Begin */
    int trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    std::cout << "Transaction " << trx_id << " has been started." << std::endl;

    for(int i = 0; i < RD_N; ++i) {
        char buf[150]; uint16_t val_size;
        int64_t key = multi_rd_keys[i];
        int flag = db_find(table_id, key, buf, &val_size, trx_id);

        EXPECT_EQ(flag, 0)
            << "db_find \\w trx failed | index : " << i << " | key : " << key << '\n';

        EXPECT_EQ(
            std::string(buf, val_size),
            multi_rd_values[i]
        )
        << "Not an expected value. | index : " << i << " | key : " << key << '\n'
        << "Expected : " << multi_rd_values[i] << '\n'
        << "Actual : " << std::string(buf, val_size) << '\n';
    }

    std::cout << "Try to commit Transaction " << trx_id << std::endl;
    EXPECT_EQ(trx_commit(trx_id), trx_id)
        << "trx_commit failed | trx_id : " << trx_id << '\n';
    /* Transaction End */

    return NULL;
}

TEST(MultiThreadTxnTest, SLockOnlyTest) {
    if(!std::remove(path_multi_thread_rd)) 
        std::cout << "File " << path_multi_thread_rd << " has been removed." << std::endl;
    int64_t table_id = open_table(path_multi_thread_rd);

    init_db(64);
    make_random_tree(table_id, 20000, multi_rd_values, multi_rd_keys);
    std::cout << "Random tree has been created." << std::endl;
    shutdown_db();

    int64_t* tid = new int64_t;
    *tid = open_table(path_multi_thread_rd);

    init_db(64);
    for(int i = 0; i < THREAD_N; ++i) {
        pthread_create(&threads[i], 0, thread_rnd_read, tid);
    }
    
    for(int i = 0; i < THREAD_N; ++i) {
        pthread_join(threads[i], NULL);
    }

    delete tid;

    shutdown_db();
}