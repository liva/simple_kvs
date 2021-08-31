#include "kvs/simple_kvs.h"
#include "./test.h"
#include <assert.h>
#include <memory>

using namespace HayaguiKvs;

static void read_without_write()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key("111", 3);
    SliceContainer container;
    assert(kvs->Get(ReadOptions(), key, container).IsError());
}

static void write_and_read_once()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key1("111", 3);
    ConstSlice key2("111", 3);
    ConstSlice value1("abcde", 5);
    assert(kvs->Put(WriteOptions(), key1, value1).IsOk());
    SliceContainer container;
    assert(kvs->Get(ReadOptions(), key2, container).IsOk());
    assert(container.DoesMatch(value1));
}

static void write_twice_then_read()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key("123", 3);
    ConstSlice value1("abcde", 5);
    ConstSlice value2("defg", 4);
    assert(kvs->Put(WriteOptions(), key, value1).IsOk());
    assert(kvs->Put(WriteOptions(), key, value2).IsOk());
    SliceContainer container;
    assert(kvs->Get(ReadOptions(), key, container).IsOk());
    assert(container.DoesMatch(value2));
}

static void delete_and_read()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key1("111", 3);
    ConstSlice key2("111", 3);
    ConstSlice key3("111", 3);
    ConstSlice value1("abcde", 5);
    assert(kvs->Put(WriteOptions(), key1, value1).IsOk());
    assert(kvs->Delete(WriteOptions(), key2).IsOk());
    SliceContainer container;
    assert(kvs->Get(ReadOptions(), key3, container).IsError());
}

static void write_sorted_order() {
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key1("10000", 1);
    ConstSlice key2("100010", 3);
    ConstSlice key3("13", 4);
    ConstSlice value1("abcde", 5);
    ConstSlice value2("fghi", 4);
    ConstSlice value3("xy", 2);
    assert(kvs->Put(WriteOptions(), key1, value1).IsOk());
    assert(kvs->Put(WriteOptions(), key2, value2).IsOk());
    assert(kvs->Put(WriteOptions(), key3, value3).IsOk());
    SliceContainer container;
    assert(kvs->Get(ReadOptions(), key1, container).IsOk());
    assert(container.DoesMatch(value1));
    assert(kvs->Get(ReadOptions(), key2, container).IsOk());
    assert(container.DoesMatch(value2));
    assert(kvs->Get(ReadOptions(), key3, container).IsOk());
    assert(container.DoesMatch(value3));
}

static void write_unsorted_order() {
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key1("3", 1);
    ConstSlice key2("132", 3);
    ConstSlice key3("1214", 4);
    ConstSlice value1("abcde", 5);
    ConstSlice value2("fghi", 4);
    ConstSlice value3("xy", 2);
    assert(kvs->Put(WriteOptions(), key1, value1).IsOk());
    assert(kvs->Put(WriteOptions(), key2, value2).IsOk());
    assert(kvs->Put(WriteOptions(), key3, value3).IsOk());
    SliceContainer container;
    assert(kvs->Get(ReadOptions(), key1, container).IsOk());
    assert(container.DoesMatch(value1));
    assert(kvs->Get(ReadOptions(), key2, container).IsOk());
    assert(container.DoesMatch(value2));
    assert(kvs->Get(ReadOptions(), key3, container).IsOk());
    assert(container.DoesMatch(value3));
}

void simple_io_main()
{
    read_without_write();
    write_and_read_once();
    write_twice_then_read();
    delete_and_read();
    write_sorted_order();
    write_unsorted_order();
}
