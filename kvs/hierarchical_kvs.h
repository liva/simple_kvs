#pragma once
#include "kvs_interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <utility>
#include <new>
#include <assert.h>

namespace HayaguiKvs
{
    class HierarchicalKvs : public Kvs
    {
    public:
        HierarchicalKvs() = delete;
        HierarchicalKvs(Kvs &base_kvs, Kvs &underlying_kvs) : base_kvs_(base_kvs), underlying_kvs_(underlying_kvs)
        {
            if (RecoverFromUnderlyingKvs().IsError())
            {
                printf("HierarchicalKvs: failed to reconstruct from underlying kvs\n");
                abort();
            }
        }
        virtual ~HierarchicalKvs() override
        {
        }
        virtual Status Get(ReadOptions options, const ConstSlice &key, SliceContainer &container) override
        {
            return base_kvs_.Get(options, key, container);
        }
        virtual Status Put(WriteOptions options, const ConstSlice &key, const ConstSlice &value) override
        {
            if (underlying_kvs_.Put(options, key, value).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return base_kvs_.Put(options, key, value);
        }
        virtual Status Delete(WriteOptions options, const ConstSlice &key) override
        {
            if (underlying_kvs_.Delete(options, key).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return base_kvs_.Delete(options, key);
        }
        virtual Optional<KvsEntryIterator> GetFirstIterator() override
        {
            Optional<KvsEntryIterator> cache_iter = base_kvs_.GetFirstIterator();
            if (!cache_iter.isPresent()) {
                return Optional<KvsEntryIterator>::CreateInvalidObj();
            }
            SliceContainer key_container;
            Status s1 = cache_iter.get().GetKey(key_container);
            assert(s1.IsOk());
            return Optional<KvsEntryIterator>::CreateValidObj(GetIterator(key_container.CreateConstSlice()));
        }
        virtual KvsEntryIterator GetIterator(const ConstSlice &key) override
        {
            GenericKvsEntryIteratorBase *base = MemAllocator::alloc<GenericKvsEntryIteratorBase>();
            new (base) GenericKvsEntryIteratorBase(*this, key);
            return KvsEntryIterator(base);
        }
        virtual Status FindNextKey(const ConstSlice &key, SliceContainer &container) override {
            return base_kvs_.FindNextKey(key, container);
        }

    private:
        Status RecoverFromIterator(Optional<KvsEntryIterator> o_iter)
        {
            if (!o_iter.isPresent())
            {
                return Status::CreateOkStatus();
            }
            KvsEntryIterator iter = o_iter.get();
            SliceContainer key_container, value_container;
            if (iter.GetKey(key_container).IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (iter.Get(ReadOptions(), value_container).IsError())
            {
                return Status::CreateErrorStatus();
            }
            ConstSlice key = key_container.CreateConstSlice();
            ConstSlice value = value_container.CreateConstSlice();
            if (Put(WriteOptions(), key, value).IsError())
            {
                return Status::CreateErrorStatus();
            }
            if (!iter.hasNext())
            {
                return Status::CreateOkStatus();
            }
            return RecoverFromIterator(iter.GetNext());
        }
        Status RecoverFromUnderlyingKvs()
        {
            if (base_kvs_.DeleteAll(WriteOptions()).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return RecoverFromIterator(underlying_kvs_.GetFirstIterator());
        }
        Kvs &base_kvs_;
        Kvs &underlying_kvs_;
    };
}