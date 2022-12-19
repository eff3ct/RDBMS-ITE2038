#include <gtest/gtest.h>

#include <string>
#include <random>

#include "db.h"
#include "log.h"
#include "trx.h"

const int max_buf = 4;


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

TEST(SimpleRecoveryTest, MakeTable) {
    std::remove("DATA1");
    std::remove("data1.log");
    std::remove("data1_log.txt");

    EXPECT_EQ(init_db(max_buf, 0, 0, "data1.log", "data1_log.txt"), 0);
    int table_id = open_table("DATA1");
    EXPECT_GT(table_id, 0);

    const int N = 10;
    
    for (int i = 1; i <= N; i++) {
        std::string str = get_random_string(100);
        int res = db_insert(table_id, i, str.c_str(), str.length());
        EXPECT_EQ(res, 0);
    }

    shutdown_db();

    std::cout << "Table has been created." << std::endl;
}

TEST(SimpleRecoveryTest, SimpleCrash) {
    EXPECT_EQ(init_db(max_buf, 0, 0, "data1.log", "data1_log.txt"), 0);
    int table_id = open_table("DATA1");
    EXPECT_GT(table_id, 0);

    const int N = 10;

    int ftrx_id = trx_begin();
    for(int i = 1; i <= N; ++i) {
        char buf[200];
        uint16_t sz = 0;
        int res = db_find(table_id, i, buf, &sz, ftrx_id);
        EXPECT_EQ(res, 0);
    }
    trx_commit(ftrx_id);

    int trx_id = trx_begin();
    
    for(int i = 1; i <= N; i++) {
        std::string str = get_random_string(100);
        uint16_t sz = 0;
        int res = db_update(table_id, i, (char*)str.c_str(), str.length(), &sz, trx_id);
    }

    std::cout << "Crash has occured." << std::endl;
}

TEST(SimpleRecoveryTest, RecoveryTest) {
    EXPECT_EQ(init_db(max_buf, 0, 0, "data1.log", "data1_recovery_log.txt"), 0);
    int table_id = open_table("DATA1");
    shutdown_db();
}