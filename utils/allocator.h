#pragma once
#include <cstdlib>
#include <memory>
#include <stdint.h>

namespace HayaguiKvs
{
    class MemAllocator
    {
    public:
        template <class T>
        static inline T *alloc()
        {
            return (T *)MemAllocator::alloc(sizeof(T));
        }
        static inline char *alloc(size_t len)
        {
            return (char *)std::malloc(len);
        }
        template <class T>
        static inline void free(T *ptr)
        {
            std::free(ptr);
        }
        static inline void free(char *ptr)
        {
            std::free(ptr);
        }
    };
    struct MemAllocatorInterface
    {
        virtual ~MemAllocatorInterface() = 0;
        virtual void *alloc(size_t len) = 0;
        virtual void free(void *buf) = 0;
    };
    inline MemAllocatorInterface::~MemAllocatorInterface() {}
    class MallocBasedMemAllocator : public MemAllocatorInterface
    {
    public:
        virtual ~MallocBasedMemAllocator() {}
        virtual void *alloc(size_t len) override
        {
            return std::malloc(len);
        }
        virtual void free(void *buf) override
        {
            std::free(buf);
        }
    };
    class LocalBufferAllocator
    {
    private:
        class Pool
        {
        public:
            Pool(MemAllocatorInterface &base_allocator) : base_allocator_(base_allocator)
            {
            }
            void *Alloc(size_t len)
            {
                if (offset_ + len > kSize)
                {
                    return nullptr;
                }
                void *rval = (void *)(buf_ + offset_);
                offset_ += len;
                return rval;
            }
            void Ref()
            {
                ref_cnt_++;
            }
            void Unref()
            {
                ref_cnt_--;
                if (ref_cnt_ == 0)
                {
                    this->~Pool();
                    base_allocator_.free(this);
                }
            }

        private:
            int ref_cnt_ = 0;
            int offset_ = 0;
            MemAllocatorInterface &base_allocator_;
            static const size_t kSize = 16 * 1024 - 32;
            uint8_t buf_[kSize] __attribute__((aligned(8)));
        } * cur_pool_;

    public:
        class Container
        {
        public:
            Container(Pool *pool, MemAllocatorInterface &base_allocator, void *buf) : pool_(pool), buf_(buf), base_allocator_(base_allocator)
            {
            }
            ~Container()
            {
                if (pool_ == nullptr)
                {
                    base_allocator_.free(buf_);
                }
                else
                {
                    pool_->Unref();
                }
            }
            void *GetPtr()
            {
                return buf_;
            }

        private:
            Pool *pool_;
            void *buf_;
            MemAllocatorInterface &base_allocator_;
        };

        ~LocalBufferAllocator()
        {
            cur_pool_->Unref();
        }
        static LocalBufferAllocator *Get()
        {
            if (!allocator_)
            {
                return ResetWithDefaultAllocator();
            }
            return allocator_.get();
        }
        // mainly for unit tests
        static LocalBufferAllocator *ResetWithBaseAllocator(MemAllocatorInterface &base_allocator)
        {
            allocator_.reset(new LocalBufferAllocator(base_allocator));
            return allocator_.get();
        }
        // mainly for unit tests
        static LocalBufferAllocator *ResetWithDefaultAllocator()
        {
            allocator_.reset(new LocalBufferAllocator(*(new MallocBasedMemAllocator())));
            return allocator_.get();
        }
        Container Alloc(size_t len)
        {
            if (len >= 4096)
            {
                return Container(nullptr, base_allocator_, base_allocator_.alloc(len));
            }
            void *buf = cur_pool_->Alloc(len);
            if (buf == nullptr)
            {
                cur_pool_->Unref();
                cur_pool_ = AllocateCurrentPool(base_allocator_);
                buf = cur_pool_->Alloc(len);
            }
            return Container(cur_pool_, base_allocator_, buf);
        }

    private:
        LocalBufferAllocator(MemAllocatorInterface &base_allocator) : cur_pool_(AllocateCurrentPool(base_allocator)), base_allocator_(base_allocator)
        {
        }
        static Pool *AllocateCurrentPool(MemAllocatorInterface &base_allocator)
        {
            Pool *cur_pool = new (base_allocator.alloc(sizeof(Pool))) Pool(base_allocator);
            cur_pool->Ref();
            return cur_pool;
        }
        static std::unique_ptr<LocalBufferAllocator> allocator_;
        MemAllocatorInterface &base_allocator_;
    };

    std::unique_ptr<LocalBufferAllocator> LocalBufferAllocator::allocator_ __attribute__((weak));
}
