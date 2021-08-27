#pragma once
#include "kvs_interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <utility>
#include <new>
#include <memory>
#include <assert.h>

namespace HayaguiKvs
{
    class HierarchicalKvs : public Kvs
    {
    public:
        HierarchicalKvs() = delete;
        HierarchicalKvs(std::unique_ptr<Kvs> base_kvs, std::shared_ptr<Kvs> underlying_kvs) : base_kvs_(std::move(base_kvs)), underlying_kvs_(underlying_kvs)
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
        virtual Status Get(ReadOptions options, ConstSlice &key, SliceContainer &container) override
        {
            return base_kvs_->Get(options, key, container);
        }
        virtual Status Put(WriteOptions options, ConstSlice &key, ConstSlice &value) override
        {
            if (underlying_kvs_->Put(options, key, value).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return base_kvs_->Put(options, key, value);
        }
        virtual Status Delete(WriteOptions options, ConstSlice &key) override
        {
            if (underlying_kvs_->Delete(options, key).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return base_kvs_->Delete(options, key);
        }
        virtual Optional<KvsEntryIterator> GetFirstIterator() override
        {
            return base_kvs_->GetFirstIterator();
        }
        virtual KvsEntryIterator GetIterator(ConstSlice &key) override
        {
            return base_kvs_->GetIterator(key);
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
            if (base_kvs_->DeleteAll(WriteOptions()).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return RecoverFromIterator(underlying_kvs_->GetFirstIterator());
        }
        std::unique_ptr<Kvs> base_kvs_;
        std::shared_ptr<Kvs> underlying_kvs_;
    };
}