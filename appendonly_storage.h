#pragma once
#include "block_storage_interface.h"
#include <memory>

namespace HayaguiKvs
{
    template <class BlockBuffer>
    class AppendonlyStorage
    {
    public:
        AppendOnlyStorage(std::shared_ptr<BlockStorageInterface<BlockBuffer>> blockstorage) : blockstorage_(blockstorage)
        {
        }
        Status Append() {
        }
    private:
        std::shared_ptr<BlockStorageInterface<BlockBuffer>> blockstorage_;
    };
}