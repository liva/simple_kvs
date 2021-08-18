#pragma once
#include <type_traits>
#include <stdlib.h>
#include <stdio.h>
#include <utility>

namespace HayaguiKvs
{
    template <class T>
    class Optional
    {
    public:
        Optional() = delete;
        ~Optional()
        {
        }
        Optional(Optional &&obj) : obj_(std::move(obj.obj_)), is_valid_(obj.is_valid_)
        {
            MarkUsedFlagToMovedObj(std::move(obj));
        }
        Optional &operator=(Optional &&obj)
        {
            obj_ = std::move(obj.obj_);
            is_valid_ = obj.is_valid_;
            MarkUsedFlagToMovedObj(std::move(obj));
            return *this;
        }
        static Optional<T> CreateValidObj(T &&obj)
        {
            return Optional<T>(std::move(obj), true);
        }
        static Optional<T> CreateInvalidObj(T &&dummy_obj)
        {
            return Optional<T>(std::move(dummy_obj), false);
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
            return T(std::move(obj_));
        }

    private:
        Optional(T &&obj, bool is_valid) : obj_(std::move(obj)), is_valid_(is_valid)
        {
        }
        void MarkUsedFlagToMovedObj(Optional &&obj)
        {
            // no need to do
        }
        T obj_;
        bool is_valid_;
        bool is_checked_ = false;
    };
}