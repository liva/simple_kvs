#pragma once
#include "slice.h"
#include "status.h"
#include <stdio.h>

namespace HayaguiKvs
{
    struct MultipleValidSliceContainerReaderInterface
    {
        virtual const ValidSlice *Get() = 0;
        virtual const int GetLen() const = 0;
        virtual const int GetSliceLen() const = 0;
        virtual Status CopyToBuffer(char *buf) const = 0;
        virtual void Print() const = 0;
    };
    class MultipleValidSliceContainer : public MultipleValidSliceContainerReaderInterface
    {
    public:
        MultipleValidSliceContainer() = delete;
        MultipleValidSliceContainer(const ValidSlice **slices, const int num)
            : slices_(slices), num_(num)
        {
        }
        void Set(const ValidSlice *slice)
        {
            if (set_index_ >= num_)
            {
                abort();
            }
            slices_[set_index_] = slice;
            set_index_++;
        }
        virtual const ValidSlice *Get() override
        {
            if (get_index_ >= num_)
            {
                abort();
            }
            const ValidSlice *slice = slices_[get_index_];
            get_index_++;
            return slice;
        }
        virtual const int GetLen() const override
        {
            return set_index_;
        }
        virtual const int GetSliceLen() const override
        {
            if (num_ != set_index_) {
                abort();
            }
            int len = 0;
            for (int i = 0; i < num_; i++)
            {
                len += slices_[i]->GetLen();
            }
            return len;
        }
        virtual Status CopyToBuffer(char *buf) const override
        {
            if (num_ != set_index_) {
                abort();
            }
            int offset = 0;
            for (int i = 0; i < num_; i++)
            {
                const ValidSlice *slice = slices_[i];
                if (slice->CopyToBuffer(buf + offset).IsError())
                {
                    return Status::CreateErrorStatus();
                }
                offset += slice->GetLen();
            }
            return Status::CreateOkStatus();
        }
        virtual void Print() const override
        {
            printf("MSC<");
            for (int i = 0; i < num_; i++)
            {
                slices_[i]->Print();
            }
            printf(">(len: %d)", num_);
        }

    private:
        int get_index_ = 0;
        int set_index_ = 0;
        const ValidSlice **slices_;
        const int num_;
    };
}