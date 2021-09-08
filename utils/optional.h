#pragma once
#include <type_traits>
#include <stdlib.h>
#include <stdio.h>
#include <utility>
#include <new>
#include <stdint.h>

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
        mutable bool is_checked_ = false;
    };
    template <class T>
    class OptionalForConstObj
    {
    public:
        ~OptionalForConstObj()
        {
            if (is_valid_)
            {
                GetObjPtr()->~T();
            }
        }
        OptionalForConstObj(const OptionalForConstObj &obj)
        {
            is_valid_ = obj.is_valid_;
            if (is_valid_)
            {
                new (GetObjPtr()) T(*obj.GetConstObjPtr());
            }
        }
        static const OptionalForConstObj<T> CreateValidObj(const T &obj)
        {
            return OptionalForConstObj<T>(obj);
        }
        static const OptionalForConstObj<T> CreateInvalidObj()
        {
            return OptionalForConstObj<T>();
        }
        bool isPresent() const
        {
            is_checked_ = true;
            return is_valid_;
        }
        const T get() const
        {
            if (!is_valid_ || !is_checked_)
            {
                printf("error: Optional::get should not be called without checking its validity");
                abort();
            }
            return T(*GetConstObjPtr());
        }

    private:
        OptionalForConstObj() : is_valid_(false)
        {
        }
        OptionalForConstObj(const T &obj) : is_valid_(true)
        {
            new (GetObjPtr()) T(obj);
        }
        const T* const GetConstObjPtr() const
        {
            return reinterpret_cast<const T* const>(obj_);
        }
        T* GetObjPtr()
        {
            return reinterpret_cast<T*>(obj_);
        }
        uint8_t obj_[sizeof(T)] __attribute__((aligned(8)));
        bool is_valid_;
        mutable bool is_checked_ = false;
    };
}