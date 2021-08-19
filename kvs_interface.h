#pragma once
#include "utils/status.h"
#include "utils/slice.h"
#include "utils/optional.h"

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
            base_ = obj.base_;
            MarkUsedFlagToMovedObj(std::move(obj));
        }
        KvsEntryIterator() = delete;
        ~KvsEntryIterator()
        {
            DestroyBase();
        }
        KvsEntryIterator &operator=(const KvsEntryIterator &obj) = delete;
        KvsEntryIterator &operator=(KvsEntryIterator &&obj)
        {
            DestroyBase();
            base_ = obj.base_;
            MarkUsedFlagToMovedObj(std::move(obj));
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
                return Optional<KvsEntryIterator>::CreateInvalidObj(CreateDummy());
            }
            else
            {
                return Optional<KvsEntryIterator>::CreateValidObj(KvsEntryIterator(next));
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
        void Print()
        {
            SliceContainer container;
            if (base_->GetKey(container).IsError())
            {
                printf("(error)");
            }
            container.Print();
            printf(":");
            if (base_->Get(ReadOptions(), container).IsError())
            {
                printf("(error)");
            }
            container.Print();
            printf("\n");
        }

    private:
        static KvsEntryIterator CreateDummy()
        {
            return KvsEntryIterator((KvsEntryIteratorBaseInterface *)1);
        }
        void DestroyBase()
        {
            if (base_ && base_ != (KvsEntryIteratorBaseInterface *)1)
            {
                base_->Destroy();
            }
        }
        void MarkUsedFlagToMovedObj(KvsEntryIterator &&obj)
        {
            obj.base_ = nullptr;
        }
        KvsEntryIteratorBaseInterface *base_;
    };

    class Kvs
    {
    public:
        virtual ~Kvs() = 0;
        virtual Status Get(ReadOptions options, ConstSlice &key, SliceContainer &container) = 0;
        virtual Status Put(WriteOptions options, ConstSlice &key, ConstSlice &value) = 0;
        virtual Status Delete(WriteOptions options, ConstSlice &key) = 0;
        virtual Optional<KvsEntryIterator> GetFirstIterator() = 0;
        virtual KvsEntryIterator GetIterator(ConstSlice &key) = 0; // warning: retured iterator does not promise the existence of the key.
        Status DeleteAll(WriteOptions options)
        {
            return DeleteIterRecursive(GetFirstIterator(), options);
        }
        void Print()
        {
            printf(">>>\n");
            PrintIterRecursive(GetFirstIterator());
            printf("<<<\n");
        }

    private:
        void PrintIterRecursive(Optional<KvsEntryIterator> o_iter)
        {
            if (!o_iter.isPresent())
            {
                return;
            }
            KvsEntryIterator iter = o_iter.get();
            iter.Print();
            PrintIterRecursive(iter.GetNext());
        }
        Status DeleteIterRecursive(Optional<KvsEntryIterator> o_iter, WriteOptions options)
        {
            if (!o_iter.isPresent())
            {
                return Status::CreateOkStatus();
            }
            KvsEntryIterator iter = o_iter.get();
            if (!iter.Delete(options).IsError())
            {
                return Status::CreateErrorStatus();
            }
            return DeleteIterRecursive(iter.GetNext(), options);
        }
    };
    inline Kvs::~Kvs() {}
}