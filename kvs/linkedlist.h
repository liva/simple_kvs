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
            return first_element_.GetNext().Put(key, value);
        }
        virtual Status Delete(WriteOptions options, const ConstSlice &key) override
        {
            return first_element_.DeleteNext(key);
        }
        virtual Optional<KvsEntryIterator> GetFirstIterator() override
        {
        }
        virtual KvsEntryIterator GetIterator(const ConstSlice &key) override
        {
        }
        virtual Status FindNextKey(const ConstSlice &key, SliceContainer &container) override
        {
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
                Status s = key_.Cmp(key, result);
                assert(s.IsOk());
                return result.IsEqual();
            }
            void GetNextFrom(Element *ele)
            {
                next_ = ele->next_;
                ele->next_ = nullptr;
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
            Status Put(const ConstSlice &key, const ConstSlice &value)
            {
                if (!hasElement() || ele_->isKeyEqual(key))
                {
                    SetElement(CreateElement(key, value));
                    return Status::CreateOkStatus();
                }
                return GetNext().Put(key, value);
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
            Status DeleteNext(const ConstSlice &key)
            {
                Container next = GetNext();
                if (!next.hasElement())
                {
                    return Status::CreateErrorStatus();
                }
                if (next.ele_->isKeyEqual(key))
                {
                    ele_->GetNextFrom(next.ele_);
                    DeleteElement(next.ele_);
                    return Status::CreateOkStatus();
                }
                return next.DeleteNext(key);
            }
            static Container CreateDummy()
            {
                // for the first element
                return Container(CreateElement(ConstSlice("dummy", 5), ConstSlice("dummy", 5)));
            }
        private:
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
                    ele->GetNextFrom(ele_);
                    DeleteElement(ele_);
                }
                ele_ = ele;
            }
            Element *ele_ = nullptr;
        };
        Container first_element_ = Container::CreateDummy();
    };
}