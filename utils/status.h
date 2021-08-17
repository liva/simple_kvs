#pragma once
#ifndef assert
#include <assert.h>
#endif

namespace HayaguiKvs
{
    class Status
    {
    public:
        Status(const Status &obj) = delete;
        Status(Status &&obj)
        {
            value_ = obj.value_;
            checked_ = obj.checked_;
        }
        Status &operator=(const Status &obj) = delete;
        Status &operator=(Status &&obj)
        {
            value_ = obj.value_;
            checked_ = obj.checked_;
            return *this;
        }
        ~Status()
        {
            assert(checked_);
        }
        static Status CreateOkStatus()
        {
            return Status(0);
        }
        static Status CreateErrorStatus()
        {
            return Status(1);
        }
        bool IsOk()
        {
            checked_ = true;
            return value_ == 0;
        }
        bool IsError()
        {
            checked_ = true;
            return value_ == 1;
        }

    private:
        Status() = delete;
        Status(const int value)
        {
            value_ = value;
        }
        int value_;
        bool checked_ = false;
    };
}