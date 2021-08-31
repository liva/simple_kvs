#pragma once
#include <type_traits>
#include <stdlib.h>
#include <stdio.h>
#include <utility>
#include <new>

namespace HayaguiKvs
{
    template <class T>
    class Optional
    {
    public:
        ~Optional()
        {
            if (is_valid_)
            {
                GetObjPtr()->~T();
            }
        }
        Optional(Optional &&obj)
        {
            is_valid_ = obj.is_valid_;
            if (is_valid_)
            {
                new (GetObjPtr()) T(std::move(*obj.GetObjPtr()));
            }
            MarkUsedFlagToMovedObj(std::move(obj));
        }
        Optional &operator=(Optional &&obj)
        {
            if (obj.is_valid_)
            {
                if (is_valid_)
                {
                    *GetObjPtr() = std::move(*obj.GetObjPtr());
                }
                else
                {
                    new (GetObjPtr()) T(std::move(*obj.GetObjPtr()));
                }
            }
            else
            {
                if (is_valid_)
                {
                    GetObjPtr()->~T();
                }
            }
            is_valid_ = obj.is_valid_;
            MarkUsedFlagToMovedObj(std::move(obj));
            return *this;
        }
        static Optional<T> CreateValidObj(T &&obj)
        {
            return Optional<T>(std::move(obj));
        }
        static Optional<T> CreateInvalidObj()
        {
            return Optional<T>();
        }
        bool isPresent()
        {
            is_checked_ = true;
            return is_valid_;
        }
        T get()
        {
            if (!is_valid_ || !is_checked_)
            {
                printf("error: Optional::get should not be called without checking its validity");
                abort();
            }
            return T(std::move(*GetObjPtr()));
        }

    private:
        Optional() : is_valid_(false)
        {
        }
        Optional(T &&obj) : is_valid_(true)
        {
            new (GetObjPtr()) T(std::move(obj));
        }
        void MarkUsedFlagToMovedObj(Optional &&obj)
        {
            // nothing to do
        }
        T *GetObjPtr()
        {
            return reinterpret_cast<T *>(obj_);
        }
        uint8_t obj_[sizeof(T)] __attribute__((aligned(8)));
        bool is_valid_;
        bool is_checked_ = false;
    };
}