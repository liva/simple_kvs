#pragma once
#include "kvs_interface.h"

namespace HayaguiKvs
{
    struct HashCalculatorInterface
    {
        virtual const int CalcHash(const int bucket_size, const ValidSlice &key) = 0;
    };

    class SimpleHashCalculator final : public HashCalculatorInterface
    {
    public:
        virtual const int CalcHash(const int bucket_size, const ValidSlice &key) override
        {
            int cnt = 0;
            for (int i = 0; i < key.GetLen(); i++)
            {
                ShrinkedSlice slice(key, i, 1);
                char tmp_buf;
                if (slice.CopyToBuffer(&tmp_buf).IsError())
                {
                    abort();
                }
                cnt += tmp_buf;
            }
            return cnt % bucket_size;
        }
    };

    class HashKvs final : public Kvs
    {
    public:
        HashKvs() = delete;
        HashKvs(int bucket_size, KvsAllocatorInterface &underlying_kvs_allocator, HashCalculatorInterface &hash_calcurator)
            : bucket_size_(bucket_size),
              buckets_(GenerateBuckets(bucket_size, underlying_kvs_allocator)),
              hash_calcurator_(hash_calcurator)
        {
        }
        ~HashKvs()
        {
            for (int i = 0; i < bucket_size_; i++)
            {
                delete buckets_[i];
            }
            delete[] buckets_;
        }
        HashKvs(const HashKvs &obj) = delete;
        HashKvs &operator=(const HashKvs &obj) = delete;
        virtual Status Get(ReadOptions options, const ValidSlice &key, SliceContainer &container) override
        {
            const int index = hash_calcurator_.CalcHash(bucket_size_, key);
            return buckets_[index]->Get(options, key, container);
        }
        virtual Status Put(WriteOptions options, const ValidSlice &key, const ValidSlice &value) override
        {
            const int index = hash_calcurator_.CalcHash(bucket_size_, key);
            return buckets_[index]->Put(options, key, value);
        }
        virtual Status Delete(WriteOptions options, const ValidSlice &key) override
        {
            const int index = hash_calcurator_.CalcHash(bucket_size_, key);
            return buckets_[index]->Delete(options, key);
        }
        virtual Optional<KvsEntryIterator> GetFirstIterator() override
        {
            LowerSliceContainer key_container;
            for (int i = 0; i < bucket_size_; i++)
            {
                Optional<KvsEntryIterator> optional_bucket_iter = buckets_[i]->GetFirstIterator();
                if (!optional_bucket_iter.isPresent())
                {
                    continue;
                }
                SliceContainer iter_key_container;
                Status s1 = optional_bucket_iter.get().GetKey(iter_key_container);
                assert(s1.IsOk());
                key_container.SetIfLower(std::move(iter_key_container));
            }
            if (!key_container.IsSliceAvailable())
            {
                return Optional<KvsEntryIterator>::CreateInvalidObj();
            }
            return Optional<KvsEntryIterator>::CreateValidObj(GetIterator(key_container.CreateConstSlice()));
        }
        virtual KvsEntryIterator GetIterator(const ValidSlice &key) override
        {
            GenericKvsEntryIteratorBase *base = MemAllocator::alloc<GenericKvsEntryIteratorBase>();
            new (base) GenericKvsEntryIteratorBase(*this, key);
            return KvsEntryIterator(base);
        }
        virtual Status FindNextKey(const ValidSlice &key, SliceContainer &container) override
        {
            LowerSliceContainer key_container;
            for (int i = 0; i < bucket_size_; i++)
            {
                Optional<KvsEntryIterator> optional_bucket_iter = buckets_[i]->GetFirstIterator();
                while (optional_bucket_iter.isPresent())
                {
                    SliceContainer iter_key_container;
                    KvsEntryIterator bucket_iter = optional_bucket_iter.get();
                    Status s1 = bucket_iter.GetKey(iter_key_container);
                    assert(s1.IsOk());

                    CmpResult result;
                    if (iter_key_container.Cmp(key, result).IsError())
                    {
                        abort();
                    }
                    if (result.IsGreater())
                    {
                        key_container.SetIfLower(std::move(iter_key_container));
                        break;
                    }
                    optional_bucket_iter = std::move(bucket_iter.GetNext());
                }
            }
            return key_container.SetTo(container);
        }

    private:
        class LowerSliceContainer
        {
        public:
            void SetIfLower(SliceContainer &&obj)
            {
                if (!container_.IsSliceAvailable())
                {
                    container_.Set(obj);
                }
                else
                {
                    CmpResult result;
                    if (container_.Cmp(obj, result).IsError())
                    {
                        abort();
                    }
                    if (result.IsGreater())
                    {
                        container_.Set(obj);
                    }
                }
            }
            const bool IsSliceAvailable() const
            {
                return container_.IsSliceAvailable();
            }
            ConstSlice CreateConstSlice() const
            {
                return container_.CreateConstSlice();
            }
            Status SetTo(SliceContainer &container)
            {
                if (!container_.IsSliceAvailable())
                {
                    return Status::CreateErrorStatus();
                }
                container.Set(container_);
                return Status::CreateOkStatus();
            }

        private:
            SliceContainer container_;
        };
        static Kvs **GenerateBuckets(int bucket_size, KvsAllocatorInterface &underlying_kvs_allocator)
        {
            Kvs **buckets = new Kvs *[bucket_size];
            for (int i = 0; i < bucket_size; i++)
            {
                buckets[i] = underlying_kvs_allocator.Allocate();
            }
            return buckets;
        }
        const int bucket_size_;
        Kvs **buckets_;
        HashCalculatorInterface &hash_calcurator_;
    };
}