#include "./test.h"
#include "./kvs_misc.h"
#include <assert.h>
#include <memory>

using namespace HayaguiKvs;

class ReadWithoutWriteTester
{
public:
    ReadWithoutWriteTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key("111", 3);
        SliceContainer container;
        assert(kvs_container_->Get(ReadOptions(), key, container).IsError());
    }

private:
    KvsContainerInterface &kvs_container_;
};

class WriteAndReadOnceTester
{
public:
    WriteAndReadOnceTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key1("111", 3);
        ConstSlice key2("111", 3);
        ConstSlice value1("abcde", 5);
        assert(kvs_container_->Put(WriteOptions(), key1, value1).IsOk());
        SliceContainer container;
        assert(kvs_container_->Get(ReadOptions(), key2, container).IsOk());
        assert(container.DoesMatch(value1));
    }

private:
    KvsContainerInterface &kvs_container_;
};

class WriteTwiceThenReadTester
{
public:
    WriteTwiceThenReadTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key("123", 3);
        ConstSlice value1("abcde", 5);
        ConstSlice value2("defg", 4);
        assert(kvs_container_->Put(WriteOptions(), key, value1).IsOk());
        assert(kvs_container_->Put(WriteOptions(), key, value2).IsOk());
        SliceContainer container;
        assert(kvs_container_->Get(ReadOptions(), key, container).IsOk());
        assert(container.DoesMatch(value2));
    }

private:
    KvsContainerInterface &kvs_container_;
};

class DeleteAndReadTester
{
public:
    DeleteAndReadTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key1("111", 3);
        ConstSlice key2("111", 3);
        ConstSlice key3("111", 3);
        ConstSlice value1("abcde", 5);
        assert(kvs_container_->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs_container_->Delete(WriteOptions(), key2).IsOk());
        SliceContainer container;
        assert(kvs_container_->Get(ReadOptions(), key3, container).IsError());
    }

private:
    KvsContainerInterface &kvs_container_;
};

class WriteSortedOrderTester
{
public:
    WriteSortedOrderTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key1("10000", 1);
        ConstSlice key2("100010", 3);
        ConstSlice key3("13", 4);
        ConstSlice value1("abcde", 5);
        ConstSlice value2("fghi", 4);
        ConstSlice value3("xy", 2);
        assert(kvs_container_->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs_container_->Put(WriteOptions(), key2, value2).IsOk());
        assert(kvs_container_->Put(WriteOptions(), key3, value3).IsOk());
        SliceContainer container;
        assert(kvs_container_->Get(ReadOptions(), key1, container).IsOk());
        assert(container.DoesMatch(value1));
        assert(kvs_container_->Get(ReadOptions(), key2, container).IsOk());
        assert(container.DoesMatch(value2));
        assert(kvs_container_->Get(ReadOptions(), key3, container).IsOk());
        assert(container.DoesMatch(value3));
    }

private:
    KvsContainerInterface &kvs_container_;
};

class WriteUnsortedOrderTester
{
public:
    WriteUnsortedOrderTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key1("3", 1);
        ConstSlice key2("132", 3);
        ConstSlice key3("1214", 4);
        ConstSlice value1("abcde", 5);
        ConstSlice value2("fghi", 4);
        ConstSlice value3("xy", 2);
        assert(kvs_container_->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs_container_->Put(WriteOptions(), key2, value2).IsOk());
        assert(kvs_container_->Put(WriteOptions(), key3, value3).IsOk());
        SliceContainer container;
        assert(kvs_container_->Get(ReadOptions(), key1, container).IsOk());
        assert(container.DoesMatch(value1));
        assert(kvs_container_->Get(ReadOptions(), key2, container).IsOk());
        assert(container.DoesMatch(value2));
        assert(kvs_container_->Get(ReadOptions(), key3, container).IsOk());
        assert(container.DoesMatch(value3));
    }

private:
    KvsContainerInterface &kvs_container_;
};

class MixedQueriesTester
{
public:
    MixedQueriesTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        for (int i = 0; i < 200; i++)
        {
            char buf1[5], buf2[5];
            sprintf(buf1, "%04d", (i % 4) * 1000 + i);
            sprintf(buf2, "%04d", i);
            assert(kvs_container_->Put(WriteOptions(), ConstSlice(buf1, 4), ConstSlice(buf2, 4)).IsOk());
        }
        for (int i = 0; i < 10; i++)
        {
            const int j = i * 20;
            char buf[5];
            sprintf(buf, "%04d", (j % 4) * 1000 + j);
            assert(kvs_container_->Delete(WriteOptions(), ConstSlice(buf, 4)).IsOk());
        }
        for (int i = 0; i < 10; i++)
        {
            const int j = i * 20 + 1;
            char buf1[5], buf2[5];
            sprintf(buf1, "%04d", (j % 4) * 1000 + j);
            sprintf(buf2, "%04d", j + 1);
            assert(kvs_container_->Put(WriteOptions(), ConstSlice(buf1, 4), ConstSlice(buf2, 4)).IsOk());
        }
        for (int i = 0; i < 200; i++)
        {
            char buf1[5];
            sprintf(buf1, "%04d", (i % 4) * 1000 + i);
            ConstSlice key(buf1, 4);
            SliceContainer container;
            if ((i % 20) == 0)
            {
                assert(kvs_container_->Get(ReadOptions(), key, container).IsError());
            }
            else if ((i % 20) == 1)
            {
                char buf2[5];
                sprintf(buf2, "%04d", i + 1);
                assert(kvs_container_->Get(ReadOptions(), key, container).IsOk());
                assert(container.DoesMatch(ConstSlice(buf2, 4)));
            }
            else
            {
                char buf2[5];
                sprintf(buf2, "%04d", i);
                assert(kvs_container_->Get(ReadOptions(), key, container).IsOk());
                assert(container.DoesMatch(ConstSlice(buf2, 4)));
            }
        }
    }

private:
    KvsContainerInterface &kvs_container_;
};

template <class KvsContainer>
static void test()
{
    START_TEST_WITH_POSTFIX(typeid(KvsContainer).name());
    {
        KvsContainer kvs_container;
        ReadWithoutWriteTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        WriteAndReadOnceTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        WriteTwiceThenReadTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        DeleteAndReadTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        WriteSortedOrderTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        WriteUnsortedOrderTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        MixedQueriesTester tester(kvs_container);
        tester.Do();
    }
}

int main()
{
    test<GenericKvsContainer<SimpleKvs>>();
    test<GenericKvsContainer<LinkedListKvs>>();
    test<HashKvsContainer>();
    test<GenericKvsContainer<SkipListKvs<4>>>();
    return 0;
}
