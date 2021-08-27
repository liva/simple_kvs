#include "../hierarchical_kvs.h"
#include "../simple_kvs.h"
#include "./test.h"
#include <assert.h>
#include <memory>
#include <utility>

using namespace HayaguiKvs;

static inline void persist_with_underlying_kvs()
{
    START_TEST;
    std::shared_ptr<Kvs> subkvs = std::shared_ptr<Kvs>(new SimpleKvs);
    ConstSlice key1("111", 3);
    ConstSlice value1("abcde", 5);
    ConstSlice value2("defg", 4);
    ConstSlice key3("1001", 4);
    ConstSlice value3("xy", 2);
    ConstSlice key4("100010", 3);
    {
        std::unique_ptr<Kvs> base_kvs = std::unique_ptr<Kvs>(new SimpleKvs);
        std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new HierarchicalKvs(std::move(base_kvs), subkvs));
        assert(kvs->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs->Put(WriteOptions(), key1, value2).IsOk());
        assert(kvs->Put(WriteOptions(), key3, value3).IsOk());
        assert(kvs->Put(WriteOptions(), key4, value1).IsOk());
        assert(kvs->Delete(WriteOptions(), key4).IsOk());
    }
    {
        std::unique_ptr<Kvs> base_kvs = std::unique_ptr<Kvs>(new SimpleKvs);
        std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new HierarchicalKvs(std::move(base_kvs), subkvs));
        SliceContainer container;
        assert(kvs->Get(ReadOptions(), key1, container).IsOk());
        assert(container.DoesMatch(value2));
        assert(kvs->Get(ReadOptions(), key3, container).IsOk());
        assert(container.DoesMatch(value3));
        assert(kvs->Get(ReadOptions(), key4, container).IsError());
    }
}

static inline void recover_from_block_storage()
{
    START_TEST;
    
}

void persistence_main()
{
    persist_with_underlying_kvs();
    recover_from_block_storage();
}