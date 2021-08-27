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
    class ConstSlice;
    class SliceContainer;
    class Slice
    {
    public:
        virtual ~Slice() = 0;
        virtual bool DoesMatch(const Slice &slice) const = 0;
        virtual bool DoesMatch(const InvalidSlice &slice) const = 0;
        virtual bool DoesMatch(const ConstSlice &slice) const = 0;
        virtual bool IsValid() const = 0;
        virtual Status Cmp(const Slice &slice, CmpResult &result) const = 0;
        virtual Status Cmp(const InvalidSlice &slice, CmpResult &result) const = 0;
        virtual Status Cmp(const ConstSlice &slice, CmpResult &result) const = 0;
        virtual Status SetToContainer(SliceContainer &container) const = 0;

        virtual void Print() const = 0;
        virtual Status GetLen(int &len) const = 0;
        virtual Status CopyToBuffer(char *buf) const = 0;
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
        virtual bool DoesMatch(const ConstSlice &slice) const override final
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
        virtual Status Cmp(const ConstSlice &slice, CmpResult &result) const override final
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
    };

    class ConstSlice : public Slice
    {
    public:
        ConstSlice() = delete;
        ConstSlice(const ConstSlice &slice) : buf_(DuplicateBuffer(slice.buf_, slice.len_)), len_(slice.len_)
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
        virtual bool DoesMatch(const Slice &slice) const override final
        {
            return slice.DoesMatch(*this);
        }
        virtual bool DoesMatch(const InvalidSlice &slice) const override final
        {
            return false;
        }
        virtual bool DoesMatch(const ConstSlice &slice) const override final
        {
            if (slice.len_ != len_)
            {
                return false;
            }
            return memcmp(slice.buf_, buf_, len_) == 0;
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
        virtual Status Cmp(const ConstSlice &slice, CmpResult &result) const override final
        {
            int len;
            int result_for_substring_case;
            if (slice.len_ > len_)
            {
                len = len_;
                result_for_substring_case = -1;
            }
            else if (slice.len_ < len_)
            {
                len = slice.len_;
                result_for_substring_case = 1;
            }
            else
            {
                len = len_;
                result_for_substring_case = 0;
            }
            int memcmp_result = memcmp(buf_, slice.buf_, len);
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
        virtual bool IsValid() const override final
        {
            return true;
        }
        virtual Status SetToContainer(SliceContainer &container) const override final;
        virtual void Print() const override final
        {
            for (int i = 0; i < len_; i++)
            {
                printf("%c", buf_[i]);
            }
            fflush(stdout);
        }
        virtual Status GetLen(int &len) const override final
        {
            len = len_;
            return Status::CreateOkStatus();
        }
        virtual Status CopyToBuffer(char *buf) const override final
        {
            memcpy(buf, buf_, len_);
            return Status::CreateOkStatus();
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
        Status Set(const Slice *slice)
        {
            return slice->SetToContainer(*this);
        }
        void Set(const ConstSlice &slice)
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
        Status Cmp(const Slice &slice, CmpResult &result) const
        {
            return slice_->Cmp(slice, result);
        }
        Status Cmp(const InvalidSlice &slice, CmpResult &result) const
        {
            return slice_->Cmp(slice, result);
        }
        Status Cmp(const ConstSlice &slice, CmpResult &result) const
        {
            return slice_->Cmp(slice, result);
        }
        void Print() const
        {
            return slice_->Print();
        }
        Status GetLen(int &len) const
        {
            return slice_->GetLen(len);
        }
        Status CopyToBuffer(char *buf) const
        {
            return slice_->CopyToBuffer(buf);
        }

        bool IsSliceAvailable()
        {
            return slice_ != nullptr;
        }
        ConstSlice CreateConstSlice()
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
    };

    inline Status ConstSlice::SetToContainer(SliceContainer &container) const
    {
        container.Set(*this);
        return Status::CreateOkStatus();
    };
}