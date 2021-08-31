#include "kvs/simple_kvs.h"
#include "./test.h"
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

static void iterator_delete()
{
    START_TEST;
    bool deleted = false;
    {
        KvsEntryIterator iter = TestKvsEntryIteratorBase::Create(deleted);
    }
    assert(deleted);
}

static void skvs_iterator_create()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key("111", 3);
    KvsEntryIterator iter = kvs->GetIterator(key);
    (void)iter; // suppress warning
}

static void skvs_iterator_get_invalid_entry()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key("111", 3);
    KvsEntryIterator iter = kvs->GetIterator(key);
    SliceContainer container;
    assert(iter.Get(ReadOptions(), container).IsError());
}

static void skvs_iterator_put_get_valid_entry()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key1("111", 3);
    ConstSlice key2("111", 3);
    ConstSlice value1("abcde", 5);
    assert(kvs->Put(WriteOptions(), key1, value1).IsOk());
    KvsEntryIterator iter = kvs->GetIterator(key1);
    SliceContainer container;
    assert(iter.Get(ReadOptions(), container).IsOk());
    assert(container.DoesMatch(value1));
}

static void skvs_iterator_put()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice value1("abcde", 5);
    {
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs->GetIterator(key);
        assert(iter.Put(WriteOptions(), value1).IsOk());
    }
    {
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs->GetIterator(key);
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsOk());
        assert(container.DoesMatch(value1));
    }
}

static void skvs_iterator_overwrite()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice value1("abcde", 5);
    ConstSlice value2("xbcdefg", 7);
    {
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs->GetIterator(key);
        assert(iter.Put(WriteOptions(), value1).IsOk());
    }
    {
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs->GetIterator(key);
        assert(iter.Put(WriteOptions(), value2).IsOk());
    }
    {
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs->GetIterator(key);
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsOk());
        assert(container.DoesMatch(value2));
    }
}

static void skvs_iterator_delete()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice value1("abcde", 5);
    {
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs->GetIterator(key);
        assert(iter.Put(WriteOptions(), value1).IsOk());
    }
    {
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs->GetIterator(key);
        assert(iter.Delete(WriteOptions()).IsOk());
    }
    {
        ConstSlice key("111", 3);
        KvsEntryIterator iter = kvs->GetIterator(key);
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsError());
    }
}

static void skvs_iterator_next()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key1("111", 3);
    ConstSlice key2("123", 3);
    ConstSlice value("fghijkl", 7);
    {
        ConstSlice value1("abcde", 5);
        assert(kvs->Put(WriteOptions(), key1, value1).IsOk());
    }
    {
        assert(kvs->Put(WriteOptions(), key2, value).IsOk());
    }
    {
        KvsEntryIterator iter = kvs->GetIterator(key1);
        Optional<KvsEntryIterator> next_iter = iter.GetNext();
        assert(next_iter.isPresent());
        iter = std::move(next_iter.get());
        SliceContainer container;
        assert(iter.Get(ReadOptions(), container).IsOk());
        assert(container.DoesMatch(value));
    }
}

static void skvs_iterator_jump_deleted_value()
{
    START_TEST;
    std::unique_ptr<Kvs> kvs = std::unique_ptr<Kvs>(new SimpleKvs);
    ConstSlice key1("11", 2);
    ConstSlice key2("123", 3);
    ConstSlice key3("3", 1);
    ConstSlice value1("fghijkl", 7);
    ConstSlice value2("a", 1);
    ConstSlice value3("cde", 3);
    assert(kvs->Put(WriteOptions(), key1, value1).IsOk());
    assert(kvs->Put(WriteOptions(), key2, value2).IsOk());
    assert(kvs->Put(WriteOptions(), key3, value3).IsOk());
    KvsEntryIterator iter = kvs->GetIterator(key1);
    assert(kvs->Delete(WriteOptions(), key2).IsOk());
    Optional<KvsEntryIterator> next_iter = iter.GetNext();
    assert(next_iter.isPresent());
    iter = std::move(next_iter.get());
    SliceContainer container;
    assert(iter.Get(ReadOptions(), container).IsOk());
    assert(container.DoesMatch(value3));
}

void iterator_main()
{
    iterator_delete();
    skvs_iterator_create();
    skvs_iterator_get_invalid_entry();
    skvs_iterator_put_get_valid_entry();
    skvs_iterator_put();
    skvs_iterator_overwrite();
    skvs_iterator_delete();
    skvs_iterator_next();
    skvs_iterator_jump_deleted_value();
}