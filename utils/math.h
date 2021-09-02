#pragma once
namespace HayaguiKvs
{
    template <class T>
    static T getMax(const T a, const T b)
    {
        return a > b ? a : b;
    }
    template <class T>
    static T getMin(const T a, const T b)
    {
        return a > b ? b : a;
    }

}