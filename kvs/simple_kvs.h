#pragma once
#include "kvs_interface.h"
#include "utils/allocator.h"
#include <stdio.h>
#include <stdlib.h>
#include <utility>
#include <new>
#include <assert.h>

namespace HayaguiKvs
{
    class SimpleKvs : public Kvs
    {
    public:
        SimpleKvs()
        {
            for (int i = 0; i < kEntryNum; i++)
            {
                entries_[i] = new InvalidEntry;
            }
        }
        SimpleKvs(const SimpleKvs &obj) = delete;
        SimpleKvs &operator=(const SimpleKvs &obj) = delete;
        virtual ~SimpleKvs() override
        {
            for (int i = 0; i < kEntryNum; i++)
            {
                delete entries_[i];
            }
        }
        virtual Status Get(ReadOptions options, const ConstSlice &key, SliceContainer &container) override
        {
            for (int i = 0; i < kEntryNum; i++)
            {
                if (!entries_[i]->DoesKeyMatch(key))
                {
                    continue;
                }
                if (entries_[i]->RetrieveValue(container).IsOk())
                {
                    return Status::CreateOkStatus();
                }
            }
            return Status::CreateErrorStatus();
        }
        virtual Status Put(WriteOptions options, const ConstSlice &key, const ConstSlice &value) override
        {
            for (int i = 0; i < kEntryNum; i++)
            {
                CmpResult result;
                if (entries_[i]->CmpKey(key, result).IsError())
                {
                    return ShiftEntriesForward(i, key, value);
                }
                if (result.IsEqual())
                {
                    SliceContainer key_container, value_container;
                    if (PushValue(i, key, value, key_container, value_container).IsError())
                    {
                        return Status::CreateErrorStatus();
                    }
                    return Status::CreateOkStatus();
                }
                if (result.IsGreater())
                {
                    return ShiftEntriesForward(i, key, value);
                }
            }
            // we need to prevent this case by introducing a variable length buffer
            printf("error: buffer overflow (TODO: should be fixed)\n");
            return Status::CreateErrorStatus();
        }
        virtual Status Delete(WriteOptions options, const ConstSlice &key) override
        {
            for (int i = 0; i < kEntryNum; i++)
            {
                if (!entries_[i]->DoesKeyMatch(key))
                {
                    continue;
                }
                ClearEntry(i);
                return ShiftEntriesBackward(i);
            }
            return Status::CreateErrorStatus();
        }
        virtual Optional<KvsEntryIterator> GetFirstIterator() override
        {
            return entries_[0]->GetIterator(*this);
        }
        virtual KvsEntryIterator GetIterator(const ConstSlice &key) override
        {
            GenericKvsEntryIteratorBase *base = MemAllocator::alloc<GenericKvsEntryIteratorBase>();
            new (base) GenericKvsEntryIteratorBase(*this, key);
            return KvsEntryIterator(base);
        }
        virtual Status FindNextKey(const ConstSlice &key, SliceContainer &container) override
        {
            for (int i = 0; i < kEntryNum; i++)
            {
                CmpResult result;
                if (entries_[i]->CmpKey(key, result).IsError())
                {
                    return Status::CreateErrorStatus();
                }
                if (result.IsGreater())
                {
                    return entries_[i]->RetrieveKey(container);
                }
            }
            return Status::CreateErrorStatus();
        }

    private:
        struct EntryInterface
        {
            virtual ~EntryInterface() = 0;
            virtual Status RetrieveKey(SliceContainer &container) const = 0;
            virtual Status RetrieveValue(SliceContainer &container) const = 0;
            virtual bool DoesKeyMatch(const ConstSlice &key) const = 0;
            virtual Status CmpKey(const ConstSlice &key, CmpResult &result) const = 0;
            virtual Optional<KvsEntryIterator> GetIterator(SimpleKvs &kvs) const = 0;
        };
        class InvalidEntry : public EntryInterface
        {
        public:
            virtual Status RetrieveKey(SliceContainer &container) const override
            {
                return Status::CreateErrorStatus();
            }
            virtual Status RetrieveValue(SliceContainer &container) const override
            {
                return Status::CreateErrorStatus();
            }
            virtual bool DoesKeyMatch(const ConstSlice &key) const override
            {
                return false;
            }
            virtual Status CmpKey(const ConstSlice &key, CmpResult &result) const override
            {
                return Status::CreateErrorStatus();
            }
            virtual Optional<KvsEntryIterator> GetIterator(SimpleKvs &kvs) const override
            {
                return KvsEntryIterator::CreateInvalidObj();
            }
        };
        class ValidEntry : public EntryInterface
        {
        public:
            ValidEntry(const ConstSlice &key, const ConstSlice &value) : key_(key), value_(value)
            {
            }
            virtual Status RetrieveKey(SliceContainer &container) const override
            {
                container.Set(key_);
                return Status::CreateOkStatus();
            }
            virtual Status RetrieveValue(SliceContainer &container) const override
            {
                container.Set(value_);
                return Status::CreateOkStatus();
            }
            virtual bool DoesKeyMatch(const ConstSlice &key) const override
            {
                return key_.DoesMatch(key);
            }
            virtual Status CmpKey(const ConstSlice &key, CmpResult &result) const override
            {
                return key_.Cmp(key, result);
            }
            virtual Optional<KvsEntryIterator> GetIterator(SimpleKvs &kvs) const override
            {
                GenericKvsEntryIteratorBase *base = MemAllocator::alloc<GenericKvsEntryIteratorBase>();
                new (base) GenericKvsEntryIteratorBase(kvs, key_);
                return Optional<KvsEntryIterator>::CreateValidObj(KvsEntryIterator(base));
            }

        private:
            ConstSlice key_;
            ConstSlice value_;
        };
        Status ShiftEntriesForward(const int start_index, const ConstSlice &key, const ConstSlice &value)
        {
            SliceContainer key_container, value_container;
            key_container.Set(key);
            value_container.Set(value);
            for (int i = start_index; i < kEntryNum; i++)
            {
                ConstSlice ckey = key_container.CreateConstSlice();
                ConstSlice cvalue = value_container.CreateConstSlice();
                key_container.Release();
                value_container.Release();
                if (PushValue(i, ckey, cvalue, key_container, value_container).IsError())
                {
                    return Status::CreateErrorStatus();
                }
                assert(key_container.IsSliceAvailable() == value_container.IsSliceAvailable());
                if (!key_container.IsSliceAvailable())
                {
                    return Status::CreateOkStatus();
                }
            }
            // we need to prevent this case by introducing a variable length buffer
            printf("error: buffer overflow (TODO: should be fixed)\n");
            return Status::CreateErrorStatus();
        }
        Status ShiftEntriesBackward(const int start_index)
        {
            for (int i = start_index + 1; i < kEntryNum; i++)
            {
                SliceContainer key_container, value_container;
                if (entries_[i]->RetrieveKey(key_container).IsError())
                {
                    return Status::CreateOkStatus();
                }
                if (entries_[i]->RetrieveValue(value_container).IsError())
                {
                    // key exists, value does not
                    return Status::CreateErrorStatus();
                }
                ClearEntry(i);
                ConstSlice ckey = key_container.CreateConstSlice();
                ConstSlice cvalue = value_container.CreateConstSlice();
                key_container.Release();
                value_container.Release();
                if (PushValue(i - 1, ckey, cvalue, key_container, value_container).IsError())
                {
                    return Status::CreateErrorStatus();
                }
            }
            // we need to prevent this case by introducing a variable length buffer
            printf("error: buffer overflow (TODO: should be fixed)\n");
            return Status::CreateErrorStatus();
        }
        void ClearEntry(int i)
        {
            delete entries_[i];
            entries_[i] = new InvalidEntry;
        }
        Status PushValue(int i, const ConstSlice &key, const ConstSlice &value, SliceContainer &popped_key, SliceContainer &popped_value)
        {
            if (entries_[i]->RetrieveKey(popped_key).IsOk())
            {
                if (entries_[i]->RetrieveValue(popped_value).IsError())
                {
                    // can't be....
                    return Status::CreateErrorStatus();
                }
            }
            delete entries_[i];
            entries_[i] = new ValidEntry(key, value);
            return Status::CreateOkStatus();
        }
        static const int kEntryNum = 4196;
        EntryInterface *entries_[kEntryNum];
    };

    inline SimpleKvs::EntryInterface::~EntryInterface()
    {
    }
}