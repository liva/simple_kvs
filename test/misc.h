#pragma once
#include "utils/slice.h"
#include <unistd.h>
#include <stdio.h>

using namespace HayaguiKvs;

static ConstSlice CreateSliceFromChar(char c, int cnt) __attribute__ ((unused));
static ConstSlice CreateSliceFromChar(char c, int cnt) 
{
    char buf[cnt];
    for (int i = 0; i < cnt; i++)
    {
        buf[i] = c;
    }
    return ConstSlice(buf, cnt);
}

class File
{
public:
    void Init()
    {
        if (access(fname_, F_OK) == 0)
        {
            remove(fname_);
        }
    }
    void Cleanup()
    {
        remove(fname_);
    }
    static constexpr const char *const fname_ = "storage_file";
};

