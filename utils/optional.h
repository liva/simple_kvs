#pragma once
#include <type_traits>
#include <stdlib.h>
#include <utility>
#include "./debug.h"

namespace HayaguiKvs
{
    template <class T>
    class Optional
    {
    public:
        Optional() = delete;
        ~Optional()
        {
            DEBUG_SHOW_LINE;
        }
        // debug
        Optional(Optional &&obj) : obj_(std::move(obj.obj_)), is_valid_(obj.is_valid_)
        {
        }
        // debug
        Optional &operator=(Optional &&obj) {
            DEBUG_SHOW_LINE;
            obj_ = std::move(obj.obj_);
            is_valid_ = obj.is_valid_;
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
        T &&get()
        {
            if (!is_valid_ || !is_checked_)
            {
                printf("error: Optional::get should not be called without checking its validity");
                abort();
            }
            return std::move(obj_);
        }

    private:
        Optional(T &&obj, bool is_valid) : obj_(std::move(obj)), is_valid_(is_valid)
        {
        }
        T &&obj_;
        bool is_valid_;
        bool is_checked_ = false;
    };
}