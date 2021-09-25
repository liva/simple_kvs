#pragma once
#include <stddef.h>
#include <stdlib.h>

class LocalBuffer
{
public:
    LocalBuffer() = delete;
    explicit LocalBuffer(const size_t len)
    {
        if (len > kThreshold)
        {
            dynamically_allocated_buf_ = (char *)malloc(len);
        }
        else
        {
            dynamically_allocated_buf_ = nullptr;
        }
    }
    ~LocalBuffer()
    {
        if (dynamically_allocated_buf_ != nullptr)
        {
            free(dynamically_allocated_buf_);
        }
    }
    char *GetPtr() {
        return (dynamically_allocated_buf_ == nullptr) ? internal_buf_ : dynamically_allocated_buf_;
    }
private:
    static const size_t kThreshold = 4096;
    char internal_buf_[kThreshold] __attribute__((aligned(8)));
    char *dynamically_allocated_buf_;
};