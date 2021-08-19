#pragma once
#include "utils/status.h"

namespace HayaguiKvs
{
    class LogicalBlockAddress
    {
    public:
        explicit LogicalBlockAddress(int address) : address_(address)
        {
        }
    private:
        int address_;
    };
    struct BlockBufferInterface {
        virtual ~BlockBufferInterface() = 0;
    };
    inline BlockBufferInterface::~BlockBufferInterface() {}
    struct BlockStorageInterface
    {
        virtual ~BlockStorageInterface() = 0;
        virtual Status Read(LogicalBlockAddress address, BlockBufferInterface &buffer) = 0;
        virtual Status Write(LogicalBlockAddress address, BlockBufferInterface &buffer) = 0;
        virtual LogicalBlockAddress GetMaxAddress() = 0;
        virtual Status Flush() = 0;
    };
    inline BlockStorageInterface::~BlockStorageInterface() {}
}