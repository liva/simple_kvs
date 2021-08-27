#pragma once
#include "utils/status.h"
#include "utils/slice.h"
#include "utils/optional.h"
#include "utils/ptr_container.h"

namespace HayaguiKvs
{
    class WriteOptions
    {
    };

    class ReadOptions
    {
    };

    struct KvsEntryIteratorBaseInterface : public PtrObjInterface
    {
        virtual ~KvsEntryIteratorBaseInterface() = 0;
        virtual bool hasNext() = 0;
        virtual KvsEntryIteratorBaseInterface *GetNext() = 0;
        virtual Status Get(ReadOptions options, SliceContainer &container) = 0;
        virtual Status Put(WriteOptions options, ConstSlice &value) = 0;
        virtual Status Delete(WriteOptions options) = 0;
        virtual Status GetKey(SliceContainer &container) = 0;
    };
    inline KvsEntryIteratorBaseInterface::~KvsEntryIteratorBaseInterface() {}

    class KvsEntryIterator : public PtrContainer<KvsEntryIteratorBaseInterface>
    {
    public:
        KvsEntryIterator(KvsEntryIteratorBaseInterface *base) : PtrContainer(base)
        {
        }
        KvsEntryIterator(const KvsEntryIterator &obj) = delete;
        KvsEntryIterator(KvsEntryIterator &&obj) : PtrContainer(std::move(obj))
        {
        }
        KvsEntryIterator &operator=(const KvsEntryIterator &obj) = delete;
        KvsEntryIterator &operator=(KvsEntryIterator &&obj)
        {
            PtrContainer::operator=(std::move(obj));
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
        // the key-value pair might not be exist, not only because Kvs returns iterators wihtout checking its validity,
        // but also the pair might be removed concurrently.
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
        static Optional<KvsEntryIterator> CreateInvalidObj()
        {
            return Optional<KvsEntryIterator>::CreateInvalidObj(CreateDummy());
        }

    private:
        KvsEntryIterator() : PtrContainer()
        {
            // should be called only for an invalid object
        }
        static KvsEntryIterator CreateDummy()
        {
            return KvsEntryIterator();
        }
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