#pragma once
#include <cstdlib>
#include <memory>
#include <stdint.h>
#include <stdio.h>

namespace HayaguiKvs
{
    // deprecated. use GlobalBufferAllocator instead
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
    class BufContainerSignature
    {
    public:
        BufContainerSignature(uint64_t *signature) : signature_(signature)
        {
            SetSignature();
        }
        BufContainerSignature(BufContainerSignature &&obj) : signature_(obj.signature_)
        {
            CheckSignature();
            obj.signature_ = nullptr;
        }
        ~BufContainerSignature()
        {
            CheckAndClearSignature();
        }

        BufContainerSignature &operator=(BufContainerSignature &&obj)
        {
            CheckAndClearSignature();
            signature_ = obj.signature_;
            CheckSignature();
            obj.signature_ = nullptr;
            return *this;
        }

        void CheckAndClearSignature()
        {
            if (!signature_)
            {
                return;
            }
            CheckSignature();
            if (kDebug)
            {
                printf("signature at %p confirmed\n", signature_);
            }
            *signature_ = 0;
            signature_ = nullptr;
        }

    private:
        void SetSignature()
        {
            if (!signature_)
            {
                return;
            }
            *signature_ = 0xdeadbeefcafe;
            if (kDebug)
            {
                printf("signature location: %p\n", signature_);
            }
        }
        void CheckSignature()
        {
            if (signature_ && *signature_ != 0xdeadbeefcafe)
            {
                if (kDebug)
                {
                    printf("buffer overflow detected (signature at %p corrupted)\n", signature_);
                }
                abort();
            }
        }
        uint64_t *signature_;
        static const bool kDebug = false;
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
                ref_cnt_++;
                return rval;
            }
            void Unref()
            {
                ref_cnt_--;
                if (ref_cnt_ == 0)
                {
                    MemAllocatorInterface &base_allocator = base_allocator_;
                    this->~Pool();
                    base_allocator.free(this);
                }
            }

        private:
            int ref_cnt_ = 1;
            int offset_ = 0;
            MemAllocatorInterface &base_allocator_;
            static const size_t kSize = 16 * 1024 - 32;
            uint8_t buf_[kSize] __attribute__((aligned(8)));
        } * cur_pool_;

    public:
        class Container
        {
        public:
            Container(Pool *pool, MemAllocatorInterface &base_allocator, void *buf, size_t len) : pool_(pool), buf_(buf), base_allocator_(base_allocator), signature_((uint64_t *)((char *)buf + len))
            {
            }
            Container(Container &&obj) : pool_(obj.pool_), buf_(obj.buf_), base_allocator_(obj.base_allocator_), signature_(std::move(obj.signature_))
            {
                obj.buf_ = nullptr;
                obj.pool_ = nullptr;
            }
            Container(const Container &obj) = delete;
            Container &operator=(const Container &) = delete;
            Container &operator=(Container &&obj)
            {
                signature_ = std::move(obj.signature_);
                ReleaseBuffer();
                buf_ = obj.buf_;
                pool_ = obj.pool_;
                obj.buf_ = nullptr;
                obj.pool_ = nullptr;
                return *this;
            }
            ~Container()
            {
                signature_.CheckAndClearSignature();
                ReleaseBuffer();
            }
            template <class T>
            T *GetPtr()
            {
                return reinterpret_cast<T *>(buf_);
            }

        private:
            void ReleaseBuffer()
            {
                if (pool_ == nullptr)
                {
                    if (buf_)
                    {
                        base_allocator_.free(buf_);
                    }
                }
                else
                {
                    pool_->Unref();
                }
            }
            Pool *pool_;
            void *buf_;
            MemAllocatorInterface &base_allocator_;
            BufContainerSignature signature_;
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
                return Container(nullptr, base_allocator_, base_allocator_.alloc(len + 8), len);
            }
            void *buf = cur_pool_->Alloc(len + 8);
            if (buf == nullptr)
            {
                cur_pool_->Unref();
                cur_pool_ = AllocateCurrentPool(base_allocator_);
                buf = cur_pool_->Alloc(len + 8);
            }
            return Container(cur_pool_, base_allocator_, buf, len);
        }

    private:
        LocalBufferAllocator(MemAllocatorInterface &base_allocator) : cur_pool_(AllocateCurrentPool(base_allocator)), base_allocator_(base_allocator)
        {
        }
        static Pool *AllocateCurrentPool(MemAllocatorInterface &base_allocator)
        {
            Pool *cur_pool = new (base_allocator.alloc(sizeof(Pool))) Pool(base_allocator);
            return cur_pool;
        }
        static std::unique_ptr<LocalBufferAllocator> allocator_;
        MemAllocatorInterface &base_allocator_;
    };

    std::unique_ptr<LocalBufferAllocator> LocalBufferAllocator::allocator_ __attribute__((weak));

    class GlobalBufferAllocator
    {
    public:
        class Container
        {
        public:
            Container(MemAllocatorInterface &base_allocator, void *buf, size_t len) : buf_(buf), base_allocator_(base_allocator), signature_((uint64_t *)((char *)buf + len))
            {
            }
            Container(Container &&obj) : buf_(obj.buf_), base_allocator_(obj.base_allocator_), signature_(std::move(obj.signature_))
            {
                obj.buf_ = nullptr;
            }
            Container(const Container &obj) = delete;
            Container &operator=(const Container &) = delete;
            Container &operator=(Container &&obj)
            {
                signature_ = std::move(obj.signature_);
                ReleaseBuffer();
                buf_ = obj.buf_;
                obj.buf_ = nullptr;
                return *this;
            }
            ~Container()
            {
                signature_.CheckAndClearSignature();
                ReleaseBuffer();
            }
            template <class T>
            T *GetPtr()
            {
                return reinterpret_cast<T *>(buf_);
            }

        private:
            void ReleaseBuffer()
            {
                if (buf_)
                {
                    base_allocator_.free(buf_);
                }
            }
            void *buf_;
            MemAllocatorInterface &base_allocator_;
            BufContainerSignature signature_;
        };

        static GlobalBufferAllocator *Get()
        {
            if (!allocator_)
            {
                return ResetWithDefaultAllocator();
            }
            return allocator_.get();
        }
        // mainly for unit tests
        static GlobalBufferAllocator *ResetWithBaseAllocator(MemAllocatorInterface &base_allocator)
        {
            allocator_.reset(new GlobalBufferAllocator(base_allocator));
            return allocator_.get();
        }
        // mainly for unit tests
        static GlobalBufferAllocator *ResetWithDefaultAllocator()
        {
            allocator_.reset(new GlobalBufferAllocator(*(new MallocBasedMemAllocator())));
            return allocator_.get();
        }
        Container Alloc(size_t len)
        {
            return Container(base_allocator_, base_allocator_.alloc(len + 8), len);
        }

    private:
        GlobalBufferAllocator(MemAllocatorInterface &base_allocator) : base_allocator_(base_allocator)
        {
        }
        static std::unique_ptr<GlobalBufferAllocator> allocator_;
        MemAllocatorInterface &base_allocator_;
    };

    std::unique_ptr<GlobalBufferAllocator> GlobalBufferAllocator::allocator_ __attribute__((weak));
}
