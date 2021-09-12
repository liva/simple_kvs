#pragma once
#include <stdint.h>

class VeRtcTaker
{
public:
    VeRtcTaker() : x_(get())
    {
    }
    void PrintMeasuredTime()
    {
        Print(get() - x_);
    }
    static inline uint64_t get()
    {
        uint64_t ret;
        void *vehva = ((void *)0x000000001000);
        asm volatile("" ::
                         : "memory");
        asm volatile("lhm.l %0,0(%1)"
                     : "=r"(ret)
                     : "r"(vehva));
        asm volatile("" ::
                         : "memory");
        // the "800" is due to the base frequency of Tsubasa
        return ((uint64_t)1000 * ret) / 800;
    }

protected:
    virtual void Print(uint64_t time) = 0;

private:
    const uint64_t x_;
};