#pragma once
#include "utils/slice.h"
#include <unistd.h>
#include <stdio.h>
#include <vefs.h>

using namespace HayaguiKvs;

static ConstSlice CreateSliceFromChar(char c, int cnt) __attribute__((unused));
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
    File()
    {
        if (access(fname_, F_OK) == 0)
        {
            remove(fname_);
        }
    }
    ~File()
    {
        remove(fname_);
    }
    static constexpr const char *const fname_ = "storage_file";
};

class VefsFile
{
public:
    VefsFile(): vefs_(Vefs::Get())
    {
        if (vefs_->DoesExist(std::string(fname_)))
        {
            vefs_->Delete(vefs_->Create(std::string(fname_), false));
        }
    }
    ~VefsFile()
    {
        vefs_->Delete(vefs_->Create(std::string(fname_), false));
    }
    static constexpr const char *const fname_ = "/storage_file";

private:
    Vefs *vefs_;
};