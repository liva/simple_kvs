#pragma once
#include "utils/slice.h"
#include "kvs_interface.h"

namespace HayaguiKvs
{
    struct StorageInterface : public Kvs
    {
        virtual ~StorageInterface() = 0;
        virtual Status Flush() = 0;
    };
    inline StorageInterface::~StorageInterface() {}
}