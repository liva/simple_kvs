#pragma once
#include "block_storage_interface.h"
#include "utils/status.h"
#include <memory>
#include <vector>

namespace HayaguiKvs
{
    class MultiplyRule
    {
    public:
        MultiplyRule() {}
        Status AppendRegion(LogicalBlockRegion region)
        {
            if (Validate(region).IsError())
            {
                return Status::CreateErrorStatus();
            }
            regions_.push_back(region);
            return Status::CreateOkStatus();
        }

    private:
        Status Validate(LogicalBlockRegion region)
        {
            const size_t size = regions_.size();
            for (int i = 0; i < size; i++)
            {
                if (LogicalBlockRegion::IsOverlapped(regions_[i], region))
                {
                    return Status::CreateErrorStatus();
                }
            }
            return Status::CreateOkStatus();
        }
        std::vector<LogicalBlockRegion> regions_;
    };

    template <class BlockBuffer>
    class BlockStorageMultiplier
    {
    public:
        BlockStorageMultiplier() = delete;
        BlockStorageMultiplier(std::shared_ptr<BlockStorageInterface<BlockBuffer>> blockstorage) : blockstorage_(blockstorage)
        {
        }
        class MultipliedBlockStorage
        {
        public:
            MultipliedBlockStorage(std::shared_ptr<BlockStorageMultiplier> multiplier) : multiplier_(multiplier)
            {
            }

        private:
            std::shared_ptr<BlockStorageMultiplier> multiplier_;
        };

    private:
        std::shared_ptr<BlockStorageInterface<BlockBuffer>> blockstorage_;
    };
}