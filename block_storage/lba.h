#pragma once
#include "utils/cmp.h"

namespace HayaguiKvs
{
    class LogicalBlockAddress
    {
    public:
        LogicalBlockAddress() = delete;
        explicit LogicalBlockAddress(const int raw_address) : raw_address_(raw_address)
        {
        }
        LogicalBlockAddress(const LogicalBlockAddress &obj) : raw_address_(obj.raw_address_)
        {
        }
        LogicalBlockAddress &operator=(const LogicalBlockAddress &obj)
        {
            raw_address_ = obj.raw_address_;
            return *this;
        }
        CmpResult Cmp(const LogicalBlockAddress address) const
        {
            return CmpResult(raw_address_ - address.raw_address_);
        }
        int GetRaw() const
        {
            return raw_address_;
        }
        static LogicalBlockAddress GetLower(const LogicalBlockAddress address1, const LogicalBlockAddress address2)
        {
            return LogicalBlockAddress(address1.raw_address_ < address2.raw_address_ ? address1.raw_address_ : address2.raw_address_);
        }
        static LogicalBlockAddress GetGreater(const LogicalBlockAddress address1, const LogicalBlockAddress address2)
        {
            return LogicalBlockAddress(address1.raw_address_ < address2.raw_address_ ? address2.raw_address_ : address1.raw_address_);
        }
        friend LogicalBlockAddress operator+(const LogicalBlockAddress &a, const LogicalBlockAddress &b);
        friend int operator-(const LogicalBlockAddress &a, const LogicalBlockAddress &b);
        LogicalBlockAddress &operator++()
        {
            raw_address_++;
            return *this;
        }

    private:
        int raw_address_;
    };
    LogicalBlockAddress operator+(const LogicalBlockAddress &a, const LogicalBlockAddress &b)
    {
        return LogicalBlockAddress(a.raw_address_ + b.raw_address_);
    }
    int operator-(const LogicalBlockAddress &a, const LogicalBlockAddress &b)
    {
        return a.raw_address_ - b.raw_address_;
    }

    class LogicalBlockRegion
    {
    public:
        LogicalBlockRegion() = delete;
        LogicalBlockRegion(const LogicalBlockAddress start, const LogicalBlockAddress end) : start_(start), end_(end)
        {
            if (start.Cmp(end).IsGreater())
            {
                abort();
            }
        }
        static bool IsOverlapped(const LogicalBlockRegion reg1, const LogicalBlockRegion reg2)
        {
            const LogicalBlockAddress s_address = LogicalBlockAddress::GetGreater(reg1.start_, reg2.start_);
            const LogicalBlockAddress e_address = LogicalBlockAddress::GetLower(reg1.end_, reg2.end_);
            return s_address.Cmp(e_address).IsLowerOrEqual();
        }
        LogicalBlockAddress GetStart() const
        {
            return start_;
        }
        LogicalBlockAddress GetEnd() const
        {
            return end_;
        }
        size_t GetRegionSize() const
        {
            return end_.GetRaw() - start_.GetRaw() + 1;
        }

    private:
        const LogicalBlockAddress start_;
        const LogicalBlockAddress end_;
    };
}