#pragma once
#include "utils/status.h"
#include "utils/slice.h"
#include "utils/optional.h"
#include "utils/debug.h"

namespace HayaguiKvs
{
    class WriteOptions
    {
    };

    class ReadOptions
    {
    };

    struct KvsEntryIteratorBaseInterface
    {
        virtual ~KvsEntryIteratorBaseInterface() = 0;
        virtual bool hasNext() = 0;
        virtual KvsEntryIteratorBaseInterface *GetNext() = 0;
        virtual Status Get(ReadOptions options, SliceContainer &container) = 0;
        virtual Status Put(WriteOptions options, ConstSlice &value) = 0;
        virtual Status Delete(WriteOptions options) = 0;
        virtual Status GetKey(SliceContainer &container) = 0;
        virtual void Destroy() = 0;
    };
    inline KvsEntryIteratorBaseInterface::~KvsEntryIteratorBaseInterface() {}

    class KvsEntryIterator
    {
    public:
        KvsEntryIterator(KvsEntryIteratorBaseInterface *base)
        {
            assert(base != nullptr);
            base_ = base;
        }
        KvsEntryIterator(const KvsEntryIterator &obj) = delete;
        KvsEntryIterator(KvsEntryIterator &&obj)
        {
            DEBUG_SHOW_LINE;
            base_ = obj.base_;
            obj.base_ = nullptr; // obj should be never used.
        }
        KvsEntryIterator() = delete;
        ~KvsEntryIterator()
        {
            if (base_ && base_ != (KvsEntryIteratorBaseInterface *)1) // debug
            {
                base_->Destroy();
            }
        }
        KvsEntryIterator &operator=(const KvsEntryIterator &obj) = delete;
        KvsEntryIterator &operator=(KvsEntryIterator &&obj)
        {
            DEBUG_SHOW_LINE;
            if (base_ && base_ != (KvsEntryIteratorBaseInterface *)1) // debug
            {
                base_->Destroy();
            }
            DEBUG_SHOW_LINE;
            base_ = obj.base_;
            obj.base_ = nullptr; // obj should be never used.
            return *this;
        }
        bool hasNext()
        {
            return base_->hasNext();
        }
        Optional<KvsEntryIterator> GetNext()
        {
            KvsEntryIteratorBaseInterface *next = base_->GetNext();
            if (next == nullptr)
            {
                return Optional<KvsEntryIterator>::CreateInvalidObj(KvsEntryIterator((KvsEntryIteratorBaseInterface *)1)); // dummy
            }
            else
            {
                auto a = Optional<KvsEntryIterator>::CreateInvalidObj(KvsEntryIterator((KvsEntryIteratorBaseInterface *)1));
                {
                    KvsEntryIterator iter(next);
                    DEBUG_SHOW_LINE;
                    a = Optional<KvsEntryIterator>::CreateValidObj(std::move(iter));
                }
                return a;
            }
        }
        Status GetKey(SliceContainer &container)
        {
            return base_->GetKey(container);
        }
        Status Get(ReadOptions options, SliceContainer &container)
        {
            return base_->Get(options, container);
        }
        Status Put(WriteOptions options, ConstSlice &value)
        {
            return base_->Put(options, value);
        }
        Status Delete(WriteOptions options)
        {
            return base_->Delete(options);
        }

    private:
        KvsEntryIteratorBaseInterface *base_;
    };

    struct Kvs
    {
        virtual ~Kvs() = 0;
        virtual Status Get(ReadOptions options, ConstSlice &key, SliceContainer &container) = 0;
        virtual Status Put(WriteOptions options, ConstSlice &key, ConstSlice &value) = 0;
        virtual Status Delete(WriteOptions options, ConstSlice &key) = 0;
        virtual Optional<KvsEntryIterator> GetFirstIterator() = 0;
        virtual KvsEntryIterator GetIterator(ConstSlice &key) = 0; // warning: retured iterator does not promise the existence of the key.
    };
    inline Kvs::~Kvs() {}
}