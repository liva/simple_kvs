#include "kvs/simple_kvs.h"
#include "./test.h"
#include "./kvs_misc.h"
#include <assert.h>
#include <memory>
#include <utility>

using namespace HayaguiKvs;
struct TestKvsEntryIteratorBase final : public KvsEntryIteratorBaseInterface
{
    bool *deleted_;
    TestKvsEntryIteratorBase() = delete;
    TestKvsEntryIteratorBase(bool &deleted)
    {
        deleted_ = &deleted;
    }
    virtual ~TestKvsEntryIteratorBase()
    {
        *deleted_ = true;
    }
    static KvsEntryIterator Create(bool &deleted)
    {
        return KvsEntryIterator(new TestKvsEntryIteratorBase(deleted));
    }
    virtual bool hasNext() override final
    {
        return false;
    }
    virtual KvsEntryIteratorBaseInterface *GetNext() override final
    {
        assert(false);
        return nullptr;
    }
    virtual Status Get(ReadOptions options, SliceContainer &container) override final
    {
        return Status::CreateOkStatus();
    }
    virtual Status Put(WriteOptions options, ConstSlice &value) override final
    {
        return Status::CreateOkStatus();
    }
    virtual Status Delete(WriteOptions options) override final
    {
        return Status::CreateOkStatus();
    }
    virtual Status GetKey(SliceContainer &container) override final
    {
        return Status::CreateOkStatus();
    }
    virtual void Destroy() override final
    {
        delete this;
    }
};

class DestroyTester
{
public:
    DestroyTester(KvsContainerInterface &kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        bool deleted = false;
        {
            KvsEntryIterator iter = TestKvsEntryIteratorBase::Create(deleted);
        }
        assert(deleted);
    }
};

class CreateTester
{
public:
    CreateTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs_container_->GetIterator(key);
        (void)iter; // suppress warning
    }

private:
    KvsContainerInterface &kvs_container_;
};

class GetInvalidEntryTester
{
public:
    GetInvalidEntryTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs_container_->GetIterator(key);
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsError());
    }

private:
    KvsContainerInterface &kvs_container_;
};

class PutGetInvalidEntryTester
{
public:
    PutGetInvalidEntryTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key1("111", 3);
        ConstSlice key2("111", 3);
        ConstSlice value1("abcde", 5);
        assert(kvs_container_->Put(WriteOptions(), key1, value1).IsOk());
        KvsEntryIterator iter = kvs_container_->GetIterator(key1);
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsOk());
        assert(container.DoesMatch(value1));
    }

private:
    KvsContainerInterface &kvs_container_;
};

class PutTester
{
public:
    PutTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice value1("abcde", 5);
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_container_->GetIterator(key);
            assert(iter.Put(WriteOptions(), value1).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_container_->GetIterator(key);
            SliceContainer container;
            assert(iter.Get(ReadOptions(), container).IsOk());
            assert(container.DoesMatch(value1));
        }
    }

private:
    KvsContainerInterface &kvs_container_;
};

class OverwriteTester
{
public:
    OverwriteTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice value1("abcde", 5);
        ConstSlice value2("xbcdefg", 7);
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_container_->GetIterator(key);
            assert(iter.Put(WriteOptions(), value1).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_container_->GetIterator(key);
            assert(iter.Put(WriteOptions(), value2).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_container_->GetIterator(key);
            SliceContainer container;
            assert(iter.Get(ReadOptions(), container).IsOk());
            assert(container.DoesMatch(value2));
        }
    }

private:
    KvsContainerInterface &kvs_container_;
};

class DeleteTester
{
public:
    DeleteTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice value1("abcde", 5);
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_container_->GetIterator(key);
            assert(iter.Put(WriteOptions(), value1).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_container_->GetIterator(key);
            assert(iter.Delete(WriteOptions()).IsOk());
        }
        {
            ConstSlice key("111", 3);
            KvsEntryIterator iter = kvs_container_->GetIterator(key);
            SliceContainer container;
            assert(iter.Get(ReadOptions(), container).IsError());
        }
    }

private:
    KvsContainerInterface &kvs_container_;
};

class NextTester
{
public:
    NextTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key1("111", 3);
        ConstSlice key2("123", 3);
        ConstSlice value("fghijkl", 7);
        {
            ConstSlice value1("abcde", 5);
            assert(kvs_container_->Put(WriteOptions(), key1, value1).IsOk());
        }
        {
            assert(kvs_container_->Put(WriteOptions(), key2, value).IsOk());
        }
        {
            KvsEntryIterator iter = kvs_container_->GetIterator(key1);
            Optional<KvsEntryIterator> next_iter = iter.GetNext();
            assert(next_iter.isPresent());
            iter = std::move(next_iter.get());
            SliceContainer container;
            assert(iter.Get(ReadOptions(), container).IsOk());
            assert(container.DoesMatch(value));
        }
    }

private:
    KvsContainerInterface &kvs_container_;
};

class JumpDeletedValueTester
{
public:
    JumpDeletedValueTester(KvsContainerInterface &kvs_container) : kvs_container_(kvs_container)
    {
    }
    void Do()
    {
        START_TEST;
        ConstSlice key1("11", 2);
        ConstSlice key2("123", 3);
        ConstSlice key3("3", 1);
        ConstSlice value1("fghijkl", 7);
        ConstSlice value2("a", 1);
        ConstSlice value3("cde", 3);
        assert(kvs_container_->Put(WriteOptions(), key1, value1).IsOk());
        assert(kvs_container_->Put(WriteOptions(), key2, value2).IsOk());
        assert(kvs_container_->Put(WriteOptions(), key3, value3).IsOk());
        KvsEntryIterator iter = kvs_container_->GetIterator(key1);
        assert(kvs_container_->Delete(WriteOptions(), key2).IsOk());
        Optional<KvsEntryIterator> next_iter = iter.GetNext();
        assert(next_iter.isPresent());
        iter = std::move(next_iter.get());
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsOk());
        assert(container.DoesMatch(value3));
    }

private:
    KvsContainerInterface &kvs_container_;
};

template <class KvsContainer>
static void test()
{
    {
        KvsContainer kvs_container;
        DestroyTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        CreateTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        GetInvalidEntryTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        PutGetInvalidEntryTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        PutTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        OverwriteTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        DeleteTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        NextTester tester(kvs_container);
        tester.Do();
    }
    {
        KvsContainer kvs_container;
        JumpDeletedValueTester tester(kvs_container);
        tester.Do();
    }
}

int main()
{
    test<GenericKvsContainer<SimpleKvs>>();
    test<GenericKvsContainer<LinkedListKvs>>();
    test<GenericKvsContainer<SkipListKvs<4>>>();
    test<HashKvsContainer>();
    return 0;
}