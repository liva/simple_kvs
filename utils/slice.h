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
        virtual Status CopyToBufferWithRegion(char *buf, const size_t offset, const size_t len) const = 0;
    };
    inline Slice::~Slice() {}

    class InvalidSlice : public Slice
    {
    public:
        InvalidSlice()
        {
        }
        virtual ~InvalidSlice() override final
        {
        }
        virtual bool DoesMatch(const Slice &slice) const override final
        {
            return slice.DoesMatch(*this);
        }
        virtual bool DoesMatch(const InvalidSlice &slice) const override final
        {
            return true;
        }
        virtual bool DoesMatch(const ValidSlice &slice) const override final
        {
            return false;
        }
        virtual bool IsValid() const override final
        {
            return false;
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
            return Status::CreateErrorStatus();
        }
        virtual Status SetToContainer(SliceContainer &container) const override final
        {
            return Status::CreateErrorStatus();
        }
        virtual void Print() const override final
        {
            printf("(InvalidSlice)");
            fflush(stdout);
        }
        virtual Status GetLen(int &len) const override final
        {
            return Status::CreateErrorStatus();
        }
        virtual Status CopyToBuffer(char *buf) const override final
        {
            return Status::CreateErrorStatus();
        }
        virtual Status CopyToBufferWithRegion(char *buf, const size_t offset, const size_t len) const override final
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
        virtual Status CopyToBuffer(char *buf) const override final
        {
            return CopyToBufferWithRegion(buf, Region(*this, 0, GetLen()));
        }
        virtual void Print() const override final
        {
            PrintWithRegion(Region(*this, 0, GetLen()));
        }
        virtual int GetLen() const = 0;

    protected:
        friend class ShrinkedSlice;
        friend class ConstSlice;
        virtual Status CopyToBufferWithRegion(char *buf, const size_t offset, const size_t len) const override final
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

    class ConstSlice;

    class ShrinkedSlice : public ValidSlice
    {
    public:
        ShrinkedSlice(const ConstSlice &slice, const size_t offset, const size_t len)
            : underlying_slice_((const ValidSlice &)slice),
              offset_(offset),
              len_(len)
        {
            assert(offset + len <= underlying_slice_.GetLen());
        }
        ShrinkedSlice(const ShrinkedSlice &slice, const size_t offset, const size_t len)
            : underlying_slice_((const ValidSlice &)slice),
              offset_(offset),
              len_(len)
        {
            assert(offset + len <= underlying_slice_.GetLen());
        }
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

    private:
        const Region CreateRegionForUnderlyingSlice(const Region region) const
        {
            return Region(underlying_slice_, offset_ + region.offset_, region.len_);
        }
        const ValidSlice &underlying_slice_;
        const size_t offset_;
        const size_t len_;
    };

    class ConstSlice : public ValidSlice
    {
    public:
        ConstSlice() = delete;
        ConstSlice(const ConstSlice &slice) : buf_(DuplicateBuffer(slice.GetRawPtr(), slice.GetLen())), len_(slice.GetLen())
        {
        }
        ConstSlice(const ShrinkedSlice &slice) : buf_(DuplicateBuffer(slice.GetRawPtr(), slice.GetLen())), len_(slice.GetLen())
        {
        }
        ConstSlice(const ValidSlice &slice) : buf_(DuplicateBuffer(slice.GetRawPtr(), slice.GetLen())), len_(slice.GetLen())
        {
        }
        ConstSlice(const char *const buf, const int len) : buf_(DuplicateBuffer(buf, len)), len_(len)
        {
        }
        ConstSlice(ConstSlice &&obj) : buf_(obj.buf_), len_(obj.len_)
        {
        }
        virtual ~ConstSlice()
        {
            MemAllocator::free(const_cast<char *>(buf_));
        }
        virtual void PrintWithRegion(const Region region) const override final
        {
            printf("<");
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
        virtual int GetLen() const override
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
        virtual const char *const GetRawPtr() const override final
        {
            return buf_;
        }

    private:
        static const char *const DuplicateBuffer(const char *const buf, const int len)
        {
            char *const new_buf = (char *)MemAllocator::alloc(len);
            memcpy(new_buf, buf, len);
            return new_buf;
        }

        const char *const buf_;
        int len_;
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
        SliceContainer(const SliceContainer &) = delete;
        SliceContainer &operator=(const SliceContainer &) = delete;
        SliceContainer &operator=(SliceContainer &&obj)
        {
            slice_ = obj.slice_;
            MarkUsedFlagToMovedObj(std::move(obj));
            return *this;
        }
        Status Set(const Slice *slice)
        {
            return slice->SetToContainer(*this);
        }
        void Set(const ValidSlice &slice)
        {
            Release();
            slice_ = new (MemAllocator::alloc<ConstSlice>()) ConstSlice(slice);
        }
        void Set(const char *const buf, const int len)
        {
            Release();
            slice_ = new (MemAllocator::alloc<ConstSlice>()) ConstSlice(buf, len);
        }
        void Release()
        {
            if (slice_)
            {
                slice_->~ConstSlice();
                MemAllocator::free(slice_);
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
            return slice_->Print();
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
        void MarkUsedFlagToMovedObj(SliceContainer &&obj)
        {
            obj.slice_ = nullptr;
        }
        ConstSlice *slice_;
    };

    inline Status ValidSlice::SetToContainer(SliceContainer &container) const
    {
        container.Set(*this);
        return Status::CreateOkStatus();
    };
}