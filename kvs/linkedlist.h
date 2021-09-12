#pragma once
#include "kvs_interface.h"
#include "utils/allocator.h"
#include <new>

namespace HayaguiKvs
{
    class LinkedListKvs final : public Kvs
    {
    public:
        LinkedListKvs()
        {
        }
        ~LinkedListKvs()
        {
        }
        LinkedListKvs(const LinkedListKvs &obj) = delete;
        LinkedListKvs &operator=(const LinkedListKvs &obj) = delete;
        virtual Status Get(ReadOptions options, const ConstSlice &key, SliceContainer &container) override
        {
            return first_element_.GetNext().Get(key, container);
        }
        virtual Status Put(WriteOptions options, const ConstSlice &key, const ConstSlice &value) override
        {
            return first_element_.PutNext(key, value);
        }
        virtual Status Delete(WriteOptions options, const ConstSlice &key) override
        {
            return first_element_.DeleteNext(key);
        }
        virtual Optional<KvsEntryIterator> GetFirstIterator() override
        {
            SliceContainer key_container;
            Status s1 = first_element_.GetNext().GetKey(key_container);
            if (s1.IsError())
            {
                return Optional<KvsEntryIterator>::CreateInvalidObj();
            }
            return Optional<KvsEntryIterator>::CreateValidObj(GetIterator(key_container.CreateConstSlice()));
        }
        virtual KvsEntryIterator GetIterator(const ConstSlice &key) override
        {
            GenericKvsEntryIteratorBase *base = MemAllocator::alloc<GenericKvsEntryIteratorBase>();
            new (base) GenericKvsEntryIteratorBase(*this, key);
            return KvsEntryIterator(base);
        }
        virtual Status FindNextKey(const ConstSlice &key, SliceContainer &container) override
        {
            return first_element_.GetNext().FindNextKey(key, container);
        }

    private:
        class Element
        {
        public:
            Element() = delete;
            Element(const ConstSlice &key, const ConstSlice &value)
                : key_(key), value_(value)
            {
            }
            ~Element()
            {
                assert(next_ == nullptr); // to ensure the linked element is referred by others
            }
            bool isKeyEqual(const ConstSlice &key) const
            {
                CmpResult result;
                cmpKey(key, result);
                return result.IsEqual();
            }
            bool isKeyGreater(const ConstSlice &key) const
            {
                CmpResult result;
                cmpKey(key, result);
                return result.IsGreater();
            }
            void cmpKey(const ConstSlice &key, CmpResult &result) const
            {
                Status s = key_.Cmp(key, result);
                assert(s.IsOk());
            }
            void RetrieveNextFrom(Element *ele)
            {
                next_ = ele->next_;
                ele->next_ = nullptr;
            }
            void SetNext(Element *ele)
            {
                assert(next_ == nullptr); // when next_ is available, it must be retrieved beforehand
                next_ = ele;
            }
            void PutKeyTo(SliceContainer &container)
            {
                container.Set(key_);
            }
            void PutValueTo(SliceContainer &container)
            {
                container.Set(value_);
            }
            Element *GetNext()
            {
                return next_;
            }

        private:
            ConstSlice key_;
            ConstSlice value_;
            Element *next_ = nullptr;
        };
        class Container
        {
        public:
            Container()
            {
            }
            Container(Element *ele) : ele_(ele)
            {
            }
            bool hasElement()
            {
                return ele_ != nullptr;
            }
            Container GetNext()
            {
                return Container(ele_->GetNext());
            }
            Status Get(const ConstSlice &key, SliceContainer &container)
            {
                if (!hasElement())
                {
                    return Status::CreateErrorStatus();
                }
                if (ele_->isKeyEqual(key))
                {
                    ele_->PutValueTo(container);
                    return Status::CreateOkStatus();
                }
                return GetNext().Get(key, container);
            }
            Status PutNext(const ConstSlice &key, const ConstSlice &value)
            {
                Container next = GetNext();
                if (!next.hasElement())
                {
                    InsertNext(key, value);
                    return Status::CreateOkStatus();
                }
                if (next.ele_->isKeyEqual(key))
                {
                    DeleteNext();
                    InsertNext(key, value);
                    return Status::CreateOkStatus();
                }
                if (next.ele_->isKeyGreater(key))
                {
                    InsertNext(key, value);
                    return Status::CreateOkStatus();
                }
                return next.PutNext(key, value);
            }
            Status DeleteNext(const ConstSlice &key)
            {
                Container next = GetNext();
                if (!next.hasElement())
                {
                    return Status::CreateErrorStatus();
                }
                if (next.ele_->isKeyEqual(key))
                {
                    DeleteNext();
                    return Status::CreateOkStatus();
                }
                return next.DeleteNext(key);
            }
            Status FindNextKey(const ConstSlice &key, SliceContainer &container)
            {
                if (!hasElement())
                {
                    return Status::CreateErrorStatus();
                }
                if (ele_->isKeyEqual(key))
                {
                    return GetNext().GetKey(container);
                }
                return FindNextKey(key, container);
            }
            Status GetKey(SliceContainer &container)
            {
                if (!hasElement())
                {
                    return Status::CreateErrorStatus();
                }
                ele_->PutKeyTo(container);
                return Status::CreateOkStatus();
            }
            static Container CreateDummy()
            {
                // for the first element
                return Container(CreateElement(ConstSlice("dummy", 5), ConstSlice("dummy", 5)));
            }

        private:
            void DeleteNext()
            {
                Container next = GetNext();
                assert(next.hasElement());
                ele_->RetrieveNextFrom(next.ele_);
                DeleteElement(next.ele_);
            }
            void InsertNext(const ConstSlice &key, const ConstSlice &value)
            {
                Element *new_ele = CreateElement(key, value);
                new_ele->RetrieveNextFrom(ele_);
                ele_->SetNext(new_ele);
            }
            static Element *CreateElement(const ConstSlice &key, const ConstSlice &value)
            {
                Element *ele = MemAllocator::alloc<Element>();
                new (ele) Element(key, value);
                return ele;
            }
            static void DeleteElement(Element *ele)
            {
                ele->~Element();
                MemAllocator::free(ele);
            }
            void SetElement(Element *ele)
            {
                if (ele_ != nullptr)
                {
                    ele->RetrieveNextFrom(ele_);
                    DeleteElement(ele_);
                }
                ele_ = ele;
            }
            Element *ele_ = nullptr;
        };
        Container first_element_ = Container::CreateDummy();
    };
}