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
        virtual ~LinkedListKvs() override
        {
        }
        LinkedListKvs(const LinkedListKvs &obj) = delete;
        LinkedListKvs &operator=(const LinkedListKvs &obj) = delete;
        virtual Status Get(ReadOptions options, const ConstSlice &key, SliceContainer &container) override
        {
            return first_element_.GetValueRecursivelyFromNext(key, container);
        }
        virtual Status Put(WriteOptions options, const ConstSlice &key, const ConstSlice &value) override
        {
            return first_element_.PutValueRecursivelyFromNext(key, value);
        }
        virtual Status Delete(WriteOptions options, const ConstSlice &key) override
        {
            return first_element_.DeleteValueRecursivelyFromNext(key);
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
            return first_element_.FindSubsequentKeyRecursivelyFromNext(key, container);
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
            /*Container()
            {
            }*/
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
            Status GetValueRecursivelyFromNext(const ConstSlice &key, SliceContainer &container)
            {
                GetProcessor processor(key, container);
                return Walk(processor);
            }
            Status PutValueRecursivelyFromNext(const ConstSlice &key, const ConstSlice &value)
            {
                PutProcessor processor(key, value);
                return Walk(processor);
            }
            Status DeleteValueRecursivelyFromNext(const ConstSlice &key)
            {
                DeleteProcessor processor(key);
                return Walk(processor);
            }
            Status FindSubsequentKeyRecursivelyFromNext(const ConstSlice &key, SliceContainer &container)
            {
                FindNextKeyProcessor processor(key, container);
                return Walk(processor);
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
            struct ProcessorInterface
            {
                virtual const ConstSlice &GetKey() = 0;
                virtual Status ProcessTheCaseOfNoMoreEntries(Container &prev) = 0;
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) = 0;
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Container &prev) = 0;
            };
            class DeleteProcessor : public ProcessorInterface
            {
            public:
                DeleteProcessor() = delete;
                DeleteProcessor(const ConstSlice &key)
                    : key_(key)
                {
                }
                virtual const ConstSlice &GetKey() override
                {
                    return key_;
                }
                virtual Status ProcessTheCaseOfNoMoreEntries(Container &prev) override
                {
                    return Status::CreateErrorStatus();
                }
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) override
                {
                    prev.DeleteNext();
                    return Status::CreateOkStatus();
                }
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Container &prev) override
                {
                    return Status::CreateErrorStatus();
                }
            private:
                const ConstSlice &key_;
            };
            class PutProcessor : public ProcessorInterface
            {
            public:
                PutProcessor() = delete;
                PutProcessor(const ConstSlice &key, const ConstSlice &value)
                    : key_(key), value_(value)
                {
                }
                virtual const ConstSlice &GetKey() override
                {
                    return key_;
                }
                virtual Status ProcessTheCaseOfNoMoreEntries(Container &prev) override
                {
                    prev.InsertNext(key_, value_);
                    return Status::CreateOkStatus();
                }
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) override
                {
                    prev.DeleteNext();
                    prev.InsertNext(key_, value_);
                    return Status::CreateOkStatus();
                }
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Container &prev) override
                {
                    prev.InsertNext(key_, value_);
                    return Status::CreateOkStatus();
                }
            private:
                const ConstSlice &key_;
                const ConstSlice &value_;
            };
            class GetProcessor : public ProcessorInterface
            {
            public:
                GetProcessor() = delete;
                GetProcessor(const ConstSlice &key, SliceContainer &container)
                    : key_(key), container_(container)
                {
                }
                virtual const ConstSlice &GetKey() override
                {
                    return key_;
                }
                virtual Status ProcessTheCaseOfNoMoreEntries(Container &prev) override
                {
                    return Status::CreateErrorStatus();
                }
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) override
                {
                    prev.GetNext().ele_->PutValueTo(container_);
                    return Status::CreateOkStatus();
                }
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Container &prev) override
                {
                    return Status::CreateErrorStatus();
                }
            private:
                const ConstSlice &key_;
                SliceContainer &container_;
            };
            class FindNextKeyProcessor : public ProcessorInterface
            {
            public:
                FindNextKeyProcessor() = delete;
                FindNextKeyProcessor(const ConstSlice &key, SliceContainer &container)
                    : key_(key), container_(container)
                {
                }
                virtual const ConstSlice &GetKey() override
                {
                    return key_;
                }
                virtual Status ProcessTheCaseOfNoMoreEntries(Container &prev) override
                {
                    return Status::CreateErrorStatus();
                }
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) override
                {
                    return prev.GetNext().GetNext().GetKey(container_);
                }
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Container &prev) override
                {
                    return prev.GetNext().GetKey(container_);
                }
            private:
                const ConstSlice &key_;
                SliceContainer &container_;
            };
            Status Walk(ProcessorInterface &processor)
            {
                Container next = GetNext();
                if (!next.hasElement())
                {
                    return processor.ProcessTheCaseOfNoMoreEntries(*this);
                }
                if (next.ele_->isKeyEqual(processor.GetKey()))
                {
                    return processor.ProcessTheCaseOfNextEqualsToTheKey(*this);
                }
                if (next.ele_->isKeyGreater(processor.GetKey()))
                {
                    return processor.ProcessTheCaseOfNextGreaterThanTheKey(*this);
                }
                return next.Walk(processor);
            }
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
            /*void SetElement(Element *ele)
            {
                if (ele_ != nullptr)
                {
                    ele->RetrieveNextFrom(ele_);
                    DeleteElement(ele_);
                }
                ele_ = ele;
            }*/
            Element *const ele_;
        };
        Container first_element_ = Container::CreateDummy();
    };
}