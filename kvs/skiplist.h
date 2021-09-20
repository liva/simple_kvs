#pragma once
#include "kvs_interface.h"
#include "utils/allocator.h"
#include "utils/rnd.h"
#include "utils/rtc.h"
#include <new>
#include <assert.h>

namespace HayaguiKvs
{
    template <int kHeight>
    class SkipListKvs final : public Kvs
    {
    public:
        SkipListKvs() : rnd_(RtcTaker::get())
        {
        }
        virtual ~SkipListKvs() override
        {
        }
        SkipListKvs(const SkipListKvs &obj) = delete;
        SkipListKvs &operator=(const SkipListKvs &obj) = delete;
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
            Status s1 = first_element_.GetNextAtTheLowestLevel().GetKey(key_container);
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
                : key_(key), value_(DuplicateSlice(value))
            {
                InitNextsWithNull();
            }
            ~Element()
            {
                ReleaseValue();
                for (int i = 0; i < kHeight; i++)
                {
                    // to ensure the linked element is referred by others
                    assert(next_[i] == nullptr);
                }
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
            void PutKeyTo(SliceContainer &container)
            {
                container.Set(key_);
            }
            void PutValueTo(SliceContainer &container)
            {
                container.Set(*value_);
            }
            Element *GetNextAt(const int level)
            {
                return next_[level];
            }
            void ReplaceValueWith(const ConstSlice &new_value)
            {
                ReleaseValue();
                value_ = DuplicateSlice(new_value);
            }
            void Delete()
            {
                ReleaseValue();
            }
            void InsertNewElementAt(const int level, Element *new_ele)
            {
                new_ele->next_[level] = next_[level];
                next_[level] = new_ele;
            }
            bool IsValueAvailable() const
            {
                return value_ != nullptr;
            }

        private:
            void InitNextsWithNull()
            {
                for (int i = 0; i < kHeight; i++)
                {
                    next_[i] = nullptr;
                }
            }
            ConstSlice *DuplicateSlice(const ConstSlice &slice)
            {
                ConstSlice *allocated_slice_ = MemAllocator::alloc<ConstSlice>();
                new (allocated_slice_) ConstSlice(slice);
                return allocated_slice_;
            }
            void ReleaseValue()
            {
                if (value_)
                {
                    value_->~ConstSlice();
                    MemAllocator::free(value_);
                    value_ = nullptr;
                }
            }
            ConstSlice key_;
            ConstSlice *value_;
            Element *next_[kHeight];
        };

        class Container
        {
        public:
            Container() = delete;
            Container(Element *ele, const int focused_level, Random &rnd) : ele_(ele), focused_level_(focused_level), rnd_(rnd)
            {
            }
            bool hasElement()
            {
                return ele_ != nullptr;
            }
            Status GetValueRecursivelyFromNext(const ConstSlice &key, SliceContainer &container)
            {
                GetProcessor processor(key, container);
                Element *prev[kHeight];
                return Walk(prev, processor);
            }
            Status PutValueRecursivelyFromNext(const ConstSlice &key, const ConstSlice &value)
            {
                PutProcessor processor(key, value, rnd_);
                Element *prev[kHeight];
                return Walk(prev, processor);
            }
            Status DeleteValueRecursivelyFromNext(const ConstSlice &key)
            {
                DeleteProcessor processor(key);
                Element *prev[kHeight];
                return Walk(prev, processor);
            }
            Status FindSubsequentKeyRecursivelyFromNext(const ConstSlice &key, SliceContainer &container)
            {
                FindNextKeyProcessor processor(key, container, rnd_);
                Element *prev[kHeight];
                return Walk(prev, processor);
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
            static Container CreateDummy(Random &rnd)
            {
                // for the first element
                return Container(CreateElement(ConstSlice("dummy", 5), ConstSlice("dummy", 5)), kHeight - 1, rnd);
            }
            Container GetNextAtTheLowestLevel()
            {
                return Container(ele_->GetNextAt(0), 0, rnd_);
            }

        private:
            struct ProcessorInterface
            {
                virtual const ConstSlice &GetKey() = 0;
                virtual Status ProcessTheCaseOfNoMoreEntries(Element *prev[kHeight]) = 0;
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) = 0;
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Element *prev[kHeight]) = 0;
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
                virtual Status ProcessTheCaseOfNoMoreEntries(Element *prev[kHeight]) override
                {
                    return Status::CreateErrorStatus();
                }
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) override
                {
                    Container next = prev.GetNextAtTheCurrentLevel();
                    if (!next.ele_->IsValueAvailable())
                    {
                        return Status::CreateErrorStatus();
                    }
                    next.ele_->Delete();
                    return Status::CreateOkStatus();
                }
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Element *prev[kHeight]) override
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
                PutProcessor(const ConstSlice &key, const ConstSlice &value, Random &rnd)
                    : key_(key), value_(value), rnd_(rnd)
                {
                }
                virtual const ConstSlice &GetKey() override
                {
                    return key_;
                }
                virtual Status ProcessTheCaseOfNoMoreEntries(Element *prev[kHeight]) override
                {
                    Container::InsertNext(prev, key_, value_, rnd_);
                    return Status::CreateOkStatus();
                }
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) override
                {
                    Container next = prev.GetNextAtTheCurrentLevel();
                    next.ele_->ReplaceValueWith(value_);
                    return Status::CreateOkStatus();
                }
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Element *prev[kHeight]) override
                {
                    Container::InsertNext(prev, key_, value_, rnd_);
                    return Status::CreateOkStatus();
                }

            private:
                const ConstSlice &key_;
                const ConstSlice &value_;
                Random &rnd_;
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
                virtual Status ProcessTheCaseOfNoMoreEntries(Element *prev[kHeight]) override
                {
                    return Status::CreateErrorStatus();
                }
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) override
                {
                    Container next = prev.GetNextAtTheCurrentLevel();
                    if (!next.ele_->IsValueAvailable())
                    {
                        return Status::CreateErrorStatus();
                    }
                    next.ele_->PutValueTo(container_);
                    return Status::CreateOkStatus();
                }
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Element *prev[kHeight]) override
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
                FindNextKeyProcessor(const ConstSlice &key, SliceContainer &container, Random &rnd)
                    : key_(key), container_(container), rnd_(rnd)
                {
                }
                virtual const ConstSlice &GetKey() override
                {
                    return key_;
                }
                virtual Status ProcessTheCaseOfNoMoreEntries(Element *prev[kHeight]) override
                {
                    return Status::CreateErrorStatus();
                }
                virtual Status ProcessTheCaseOfNextEqualsToTheKey(Container &prev) override
                {
                    Container next = prev.GetNextAtTheCurrentLevel();
                    assert(next.hasElement());
                    Container next_next = next.GetNextAtTheLowestLevel();
                    return WalkUntilReachingAvailablePair(next_next);
                }
                virtual Status ProcessTheCaseOfNextGreaterThanTheKey(Element *prev[kHeight]) override
                {
                    Container next(prev[0], 0, rnd_);
                    return WalkUntilReachingAvailablePair(next);
                }

            private:
                Status WalkUntilReachingAvailablePair(Container &next)
                {
                    if (!next.hasElement())
                    {
                        return Status::CreateErrorStatus();
                    }
                    if (!next.ele_->IsValueAvailable())
                    {
                        Container next_next = next.GetNextAtTheCurrentLevel();
                        return WalkUntilReachingAvailablePair(next_next);
                    }
                    return next.GetKey(container_);
                }
                const ConstSlice &key_;
                SliceContainer &container_;
                Random &rnd_;
            };
            Status Walk(Element *prev[kHeight], ProcessorInterface &processor)
            {
                Container next = GetNextAtTheCurrentLevel();
                if (!next.hasElement())
                {
                    prev[focused_level_] = ele_;
                    if (focused_level_ == 0)
                    {
                        return processor.ProcessTheCaseOfNextGreaterThanTheKey(prev);
                    }
                    Container container_at_the_lower_level(ele_, focused_level_ - 1, rnd_);
                    return container_at_the_lower_level.Walk(prev, processor);
                }
                if (next.ele_->isKeyEqual(processor.GetKey()))
                {
                    return processor.ProcessTheCaseOfNextEqualsToTheKey(*this);
                }
                if (next.ele_->isKeyGreater(processor.GetKey()))
                {
                    prev[focused_level_] = ele_;
                    if (focused_level_ == 0)
                    {
                        return processor.ProcessTheCaseOfNextGreaterThanTheKey(prev);
                    }
                    Container container_at_the_lower_level(ele_, focused_level_ - 1, rnd_);
                    return container_at_the_lower_level.Walk(prev, processor);
                }
                return next.Walk(prev, processor);
            }
            static Element *CreateElement(const ConstSlice &key, const ConstSlice &value)
            {
                Element *ele = MemAllocator::alloc<Element>();
                new (ele) Element(key, value);
                return ele;
            }
            Container GetNextAtTheCurrentLevel()
            {
                return Container(ele_->GetNextAt(focused_level_), focused_level_, rnd_);
            }
            static const int CalcurateElementHeightForNewElement(Random &rnd)
            {
                return RandomHeight(kHeight, rnd);
            }
            static void InsertNext(Element *prev[kHeight], const ConstSlice &key, const ConstSlice &value, Random &rnd)
            {
                const int ele_height = CalcurateElementHeightForNewElement(rnd);
                Element *new_ele = CreateElement(key, value);
                for (int i = 0; i < ele_height; i++)
                {
                    prev[i]->InsertNewElementAt(i, new_ele);
                }
            }
            Element *const ele_;
            const int focused_level_;
            Random &rnd_;
        };
        Random rnd_;
        Container first_element_ = Container::CreateDummy(rnd_);
    };
}