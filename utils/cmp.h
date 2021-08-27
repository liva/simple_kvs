#pragma once
#include <math.h>

namespace HayaguiKvs
{
    class CmpResult
    {
    public:
        CmpResult() : result_(0)
        {
        }
        CmpResult(const int result) : result_(result)
        {
        }
        static CmpResult CreateLowerResult()
        {
            return CmpResult(-1);
        }
        static CmpResult CreateSameResult()
        {
            return CmpResult(0);
        }
        static CmpResult CreateGreaterResult()
        {
            return CmpResult(1);
        }
        CmpResult &operator=(const CmpResult &obj)
        {
            result_ = obj.result_;
            return *this;
        }
        bool IsLower() const
        {
            return result_ < 0;
        }
        bool IsEqual() const
        {
            return result_ == 0;
        }
        bool IsGreater() const
        {
            return result_ > 0;
        }
        bool IsLowerOrEqual() const
        {
            return result_ <= 0;
        }
        bool IsGreaterOrEqual() const
        {
            return result_ >= 0;
        }
        CmpResult CreateReversedResult() const
        {
            return CmpResult(result_ * -1);
        }
        bool IsSameResult(const CmpResult &obj) const
        {
            return GetNormalizedValue() == obj.GetNormalizedValue();
        }

    private:
        int GetNormalizedValue() const
        {
            return result_ == 0 ? 0 : result_ / abs(result_);
        }
        int result_;
    };
}