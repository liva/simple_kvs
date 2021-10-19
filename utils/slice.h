#pragma once
#include "status.h"
#include "allocator.h"
#include "cmp.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <new>
#include <math.h>

namespace HayaguiKvs
{
    class InvalidSlice;
    class ValidSlice;
    class SliceContainer;
    class Slice
    {
    public:
        virtual ~Slice() = 0;
        virtual bool DoesMatch(const Slice &slice) const = 0;
        virtual bool DoesMatch(const InvalidSlice &slice) const = 0;
        virtual bool DoesMatch(const ValidSlice &slice) const = 0;
        virtual bool IsValid() const = 0;
        virtual Status Cmp(const Slice &slice, CmpResult &result) const = 0;
        virtual Status Cmp(const InvalidSlice &slice, CmpResult &result) const = 0;
        virtual Status Cmp(const ValidSlice &slice, CmpResult &result) const = 0;
        virtual Status SetToContainer(SliceContainer &container) const = 0;

        virtual void Print() const = 0;
        virtual Status GetLen(int &len) const = 0;
        virtual Status CopyToBuffer(char *buf) const = 0;
    };
    inline Slice::~Slice() {}

    class InvalidSlice final : public Slice
    {
    public:
        InvalidSlice()
        {
        }
        virtual ~InvalidSlice() override
        {
        }
        virtual bool DoesMatch(const Slice &slice) const override
        {
            return slice.DoesMatch(*this);
        }
        virtual bool DoesMatch(const InvalidSlice &slice) const override
        {
            return true;
        }
        virtual bool DoesMatch(const ValidSlice &slice) const override
        {
            return false;
        }
        virtual bool IsValid() const override
        {
            return false;
        }
        virtual Status Cmp(const Slice &slice, CmpResult &result) const override
        {
            Status s = slice.Cmp(*this, result);
            result = result.CreateReversedResult();
            return s;
        }
        virtual Status Cmp(const InvalidSlice &slice, CmpResult &result) const override
        {
            return Status::CreateErrorStatus();
        }
        virtual Status Cmp(const ValidSlice &slice, CmpResult &result) const override
        {
            return Status::CreateErrorStatus();
        }
        virtual Status SetToContainer(SliceContainer &container) const override
        {
            return Status::CreateErrorStatus();
        }
        virtual void Print() const override
        {
            printf("(InvalidSlice)");
            fflush(stdout);
        }
        virtual Status GetLen(int &len) const override
        {
            return Status::CreateErrorStatus();
        }
        virtual Status CopyToBuffer(char *buf) const override
        {
            return Status::CreateErrorStatus();
        }
    };

    class ValidSlice : public Slice
    {
    public:
        class Region
        {
        public:
            Region(const ValidSlice &obj, const size_t offset, const size_t len)
                : offset_(offset),
                  len_(len)
            {
                assert(offset + len <= obj.GetLen());
            }
            const size_t offset_;
            const size_t len_;
        };
        virtual ~ValidSlice() = 0;
        virtual Status SetToContainer(SliceContainer &container) const override final;
        virtual bool IsValid() const override final
        {
            return true;
        }
        virtual bool DoesMatch(const Slice &slice) const override final
        {
            return slice.DoesMatch(*this);
        }
        virtual bool DoesMatch(const InvalidSlice &slice) const override final
        {
            return false;
        }
        virtual bool DoesMatch(const ValidSlice &slice) const override final
        {
            if (slice.GetLen() != GetLen())
            {
                return false;
            }
            CmpResult result;
            if (Cmp(slice, result).IsError())
            {
                return false;
            }
            return result.IsEqual();
        }
        virtual Status Cmp(const Slice &slice, CmpResult &result) const override final
        {
            Status s = slice.Cmp(*this, result);
            result = result.CreateReversedResult();
            return s;
        }
        virtual Status Cmp(const InvalidSlice &slice, CmpResult &result) const override final
        {
            return Status::CreateErrorStatus();
        }
        virtual Status Cmp(const ValidSlice &slice, CmpResult &result) const override final
        {
            return CmpWithRegion(slice, Region(*this, 0, GetLen()), result);
        }
        virtual Status GetLen(int &len) const override final
        {
            len = GetLen();
            return Status::CreateOkStatus();
        }
        virtual void Print() const override final
        {
            PrintWithRegion(Region(*this, 0, GetLen()));
        }
        virtual Status CopyToBuffer(char *buf) const override final
        {
            return CopyToBufferWithRegion(buf, Region(*this, 0, GetLen()));
        }
        virtual int GetLen() const = 0;

    protected:
        friend class BufferPtrSlice;
        friend class ShrinkedSlice;
        Status CopyToBufferWithRegion(char *buf, const size_t offset, const size_t len) const
        {
            if (offset + len > GetLen())
            {
                return Status::CreateErrorStatus();
            }
            return CopyToBufferWithRegion(buf, Region(*this, offset, len));
        }

        // TODO: in principle, its possible to remove this method, but we don't have such time.
        // It's still better than before where clients call GetRawPtr() without hesitation.
        // Now, only Slice-related classes call this.
        virtual const char *const GetRawPtr() const = 0;

        virtual Status CopyToBufferWithRegion(char *buf, const Region region) const = 0;
        virtual Status CmpWithRegion(const ValidSlice &slice, const Region region, CmpResult &result) const = 0;
        virtual void PrintWithRegion(const Region region) const = 0;
    };
    inline ValidSlice::~ValidSlice() {}

    class ShrinkedSlice : public ValidSlice
    {
    public:
        ShrinkedSlice(const ValidSlice &slice, const size_t offset, const size_t len)
            : underlying_slice_(slice),
              offset_(offset),
              len_(len)
        {
            assert(offset + len <= underlying_slice_.GetLen());
        }
        virtual ~ShrinkedSlice()
        {
        }
        virtual void PrintWithRegion(const Region region) const override final
        {
            underlying_slice_.PrintWithRegion(CreateRegionForUnderlyingSlice(region));
        }
        virtual int GetLen() const override final
        {
            return len_;
        }

    private:
        virtual Status CopyToBufferWithRegion(char *buf, const Region region) const override final
        {
            return underlying_slice_.CopyToBufferWithRegion(buf, CreateRegionForUnderlyingSlice(region));
        }
        virtual Status CmpWithRegion(const ValidSlice &slice, const Region region, CmpResult &result) const override final
        {
            return underlying_slice_.CmpWithRegion(slice, CreateRegionForUnderlyingSlice(region), result);
        }
        virtual const char *const GetRawPtr() const override final
        {
            return underlying_slice_.GetRawPtr() + offset_;
        }
        const Region CreateRegionForUnderlyingSlice(const Region region) const
        {
            return Region(underlying_slice_, offset_ + region.offset_, region.len_);
        }
        const ValidSlice &underlying_slice_;
        const size_t offset_;
        const size_t len_;
    };
    class BufferPtrSlice : public ValidSlice
    {
    public:
        BufferPtrSlice() = delete;
        BufferPtrSlice(const char *const buf, const int len) : buf_(buf), len_(len)
        {
        }
        BufferPtrSlice(BufferPtrSlice &&obj) : buf_(obj.buf_), len_(obj.len_)
        {
            obj.buf_ = nullptr;
            obj.len_ = 0;
        }
        virtual ~BufferPtrSlice()
        {
        }
        virtual void PrintWithRegion(const Region region) const override
        {
            PrintWithRegionSub("BPS", region);
        }
        virtual int GetLen() const override final
        {
            return len_;
        }
        virtual Status CopyToBufferWithRegion(char *buf, const Region region) const override final
        {
            memcpy(buf, buf_ + region.offset_, region.len_);
            return Status::CreateOkStatus();
        }
        virtual Status CmpWithRegion(const ValidSlice &slice, const Region region, CmpResult &result) const override final
        {
            int result_for_substring_case;
            size_t cmp_len;
            if (slice.GetLen() > region.len_)
            {
                cmp_len = region.len_;
                result_for_substring_case = -1;
            }
            else if (slice.GetLen() < region.len_)
            {
                cmp_len = slice.GetLen();
                result_for_substring_case = 1;
            }
            else
            {
                cmp_len = region.len_;
                result_for_substring_case = 0;
            }
            int memcmp_result = memcmp(buf_ + region.offset_, slice.GetRawPtr(), cmp_len);
            if (memcmp_result == 0)
            {
                result = CmpResult(result_for_substring_case);
            }
            else
            {
                result = CmpResult(memcmp_result);
            }
            return Status::CreateOkStatus();
        }

    protected:
        virtual const char *const GetRawPtr() const override final
        {
            return buf_;
        }
        void PrintWithRegionSub(const char *const signature, const Region region) const
        {
            printf("%s<", signature);
            for (int i = region.offset_; i < region.len_; i++)
            {
                char c = buf_[i];
                if (32 <= c && c <= 126)
                {
                    printf("%c", c);
                }
                else
                {
                    printf("[%02X]", c);
                }
            }
            printf(">(len: %zu)", region.len_);
            fflush(stdout);
        }
        const char *buf_;
        int len_;
    };

    class ConstSlice : public BufferPtrSlice
    {
    public:
        ConstSlice() = delete;
        ConstSlice(const ConstSlice &slice) : BufferPtrSlice(DuplicateBuffer(slice), slice.GetLen())
        {
        }
        ConstSlice(const char *const buf, const int len) : BufferPtrSlice(DuplicateBuffer(buf, len), len)
        {
        }
        ConstSlice(ConstSlice &&obj) : BufferPtrSlice(std::move(obj))
        {
        }
        virtual ~ConstSlice()
        {
            if (buf_ != nullptr)
            {
                MemAllocator::free(const_cast<char *>(buf_));
            }
        }
        static ConstSlice CreateFromValidSlice(const ValidSlice &slice)
        {
            return ConstSlice(slice);
        }
        static ConstSlice CreateConstSliceFromSlices(const ValidSlice **slices, const int cnt)
        {
            int len = 0;
            for (int i = 0; i < cnt; i++)
            {
                len += slices[i]->GetLen();
            }
            char *const buf = (char *)MemAllocator::alloc(len);

            int offset = 0;
            for (int i = 0; i < cnt; i++)
            {
                if (slices[i]->CopyToBuffer(buf + offset).IsError())
                {
                    abort();
                }
                offset += slices[i]->GetLen();
            }

            PreAllocatedBuffer pre_allocated_buffer = {buf, len};
            return ConstSlice(pre_allocated_buffer);
        }
        virtual void PrintWithRegion(const Region region) const override final
        {
            PrintWithRegionSub("CS", region);
        }

    private:
        struct PreAllocatedBuffer
        {
            char *const buf;
            int len;
        };
        ConstSlice(const ValidSlice &slice) : BufferPtrSlice(DuplicateBuffer(slice), slice.GetLen())
        {
        }
        ConstSlice(PreAllocatedBuffer pre_allocated_buffer) : BufferPtrSlice(pre_allocated_buffer.buf, pre_allocated_buffer.len)
        {
        }
        static const char *const DuplicateBuffer(const ValidSlice &slice)
        {
            char *const new_buf = (char *)MemAllocator::alloc(slice.GetLen());
            if (slice.CopyToBuffer(new_buf).IsError())
            {
                abort();
            }
            return new_buf;
        }
        static const char *const DuplicateBuffer(const char *const buf, const int len)
        {
            char *const new_buf = (char *)MemAllocator::alloc(len);
            memcpy(new_buf, buf, len);
            return new_buf;
        }
    };

    class SliceContainer
    {
    public:
        SliceContainer()
        {
            slice_ = nullptr;
        }
        ~SliceContainer()
        {
            Release();
        }
        SliceContainer(const ValidSlice &slice)
        {
            slice_ = new (buf_) ConstSlice(ConstSlice::CreateFromValidSlice(slice));
        }
        SliceContainer(const SliceContainer &) = delete;
        SliceContainer &operator=(const SliceContainer &) = delete;
        Status Set(const Slice *slice)
        {
            return slice->SetToContainer(*this);
        }
        void Set(const SliceContainer &obj)
        {
            Release();
            if (!obj.slice_)
            {
                return;
            }
            slice_ = new (buf_) ConstSlice(ConstSlice::CreateFromValidSlice(*obj.slice_));
        }
        void Set(const ValidSlice &slice)
        {
            Release();
            slice_ = new (buf_) ConstSlice(ConstSlice::CreateFromValidSlice(slice));
        }
        void Set(const char *const buf, const int len)
        {
            Release();
            slice_ = new (buf_) ConstSlice(buf, len);
        }
        void Release()
        {
            if (slice_)
            {
                slice_->~ConstSlice();
                slice_ = nullptr;
            }
        }
        bool DoesMatch(const Slice &slice) const
        {
            return slice_->DoesMatch(slice);
        }
        bool DoesMatch(const InvalidSlice &slice) const
        {
            return slice_->DoesMatch(slice);
        }
        bool DoesMatch(const ConstSlice &slice) const
        {
            return slice_->DoesMatch(slice);
        }
        bool IsValid() const
        {
            return slice_->IsValid();
        }
        Status Cmp(const SliceContainer &container, CmpResult &result) const
        {
            if (container.slice_ == nullptr)
            {
                return Status::CreateErrorStatus();
            }
            return slice_->Cmp(*container.slice_, result);
        }
        Status Cmp(const Slice &slice, CmpResult &result) const
        {
            return slice_->Cmp(slice, result);
        }
        Status Cmp(const InvalidSlice &slice, CmpResult &result) const
        {
            return slice_->Cmp(slice, result);
        }
        Status Cmp(const ValidSlice &slice, CmpResult &result) const
        {
            return slice_->Cmp(slice, result);
        }
        void Print() const
        {
            printf("SC<");
            if (slice_)
            {
                slice_->Print();
            }
            printf(">(ptr: %p)", slice_);
        }
        Status GetLen(int &len) const
        {
            len = slice_->GetLen();
            return Status::CreateOkStatus();
        }
        Status CopyToBuffer(char *buf) const
        {
            return slice_->CopyToBuffer(buf);
        }

        const bool IsSliceAvailable() const
        {
            return slice_ != nullptr;
        }
        ConstSlice CreateConstSlice() const
        {
            // IsSliceAvailable should be called in advance.
            if (slice_ == nullptr)
            {
                abort();
            }
            // assume RVO.
            // no constructors (including move constructors) will be called
            // if the return value is used for initialization.
            return ConstSlice(*slice_);
        }

    private:
        ConstSlice *slice_;
        char buf_[sizeof(ConstSlice)] __attribute__((aligned(8)));
    };

    inline Status ValidSlice::SetToContainer(SliceContainer &container) const
    {
        container.Set(*this);
        return Status::CreateOkStatus();
    };
}