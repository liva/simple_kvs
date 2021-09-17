#pragma once
#include "utils/slice.h"
#include <stdio.h>

namespace HayaguiKvs
{
    struct MultipleValidSliceContainerInterface
    {
        virtual const ValidSlice *Get() = 0;
        virtual const int GetLen() const = 0;
        virtual void Print() const = 0;
    };

    template <int N>
    class MultipleValidSliceContainer : public MultipleValidSliceContainerInterface
    {
    public:
        void Set(const ValidSlice *slice)
        {
            if (set_index_ >= N)
            {
                abort();
            }
            slices_[set_index_] = slice;
            set_index_++;
        }
        virtual const ValidSlice *Get() override
        {
            if (get_index_ >= N)
            {
                abort();
            }
            const ValidSlice *slice = slices_[get_index_];
            get_index_++;
            return slice;
        }
        virtual const int GetLen() const override
        {
            return N;
        }
        virtual void Print() const override
        {
            printf("MSC<");
            for (int i = 0; i < N; i++)
            {
                slices_[i]->Print();
            }
            printf(">(len: %d)", N);
        }

    private:
        int get_index_ = 0;
        int set_index_ = 0;
        const ValidSlice *slices_[N];
    };
}