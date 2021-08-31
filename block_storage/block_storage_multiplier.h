#pragma once
#include "block_storage_interface.h"
#include "utils/status.h"
#include <vector>

namespace HayaguiKvs
{
    class MultiplyRule
    {
    public:
        MultiplyRule() {}
        // assumne that regions are given in sorted order
        Status AppendRegion(const LogicalBlockRegion region, int &rule_num)
        {
            if (Validate(region).IsError())
            {
                // the given region overlapped the previous region
                // or, the given region is not a subsequent region
                return Status::CreateErrorStatus();
            }
            rule_num = regions_.size();

            regions_.push_back(region);
            return Status::CreateOkStatus();
        }
        int GetRuleCnt() const
        {
            return regions_.size();
        }
        const LogicalBlockRegion GetRule(const int index) const
        {
            if (index >= regions_.size())
            {
                abort();
            }
            return regions_[index];
        }

    private:
        Status Validate(const LogicalBlockRegion region) const
        {
            const size_t size = regions_.size();
            if (size == 0)
            {
                return Status::CreateOkStatus();
            }
            const LogicalBlockRegion prev = regions_[size - 1];
            if (prev.GetEnd().Cmp(region.GetStart()).IsGreaterOrEqual())
            {
                return Status::CreateErrorStatus();
            }
            return Status::CreateOkStatus();
        }
        std::vector<LogicalBlockRegion> regions_;
    };

    template <class BlockBuffer>
    class BlockStorageMultiplier
    {
    public:
        class MultipliedBlockStorage : public BlockStorageInterface<BlockBuffer>
        {
        public:
            MultipliedBlockStorage(BlockStorageInterface<BlockBuffer> &blockstorage, const LogicalBlockRegion region) : blockstorage_(blockstorage), region_(region)
            {
            }
            virtual Status Open() override {
                return blockstorage_.Open();
            }
            virtual Status ReadInternal(const LogicalBlockAddress address, BlockBuffer &buffer) override
            {
                return blockstorage_.Read(region_.GetStart() + address, buffer);
            }
            virtual Status WriteInternal(const LogicalBlockAddress address, BlockBuffer &buffer) override
            {
                return blockstorage_.Write(region_.GetStart() + address, buffer);
            }
            virtual LogicalBlockAddress GetMaxAddress() const override
            {
                return LogicalBlockAddress(region_.GetRegionSize() - 1);
            }

        private:
            BlockStorageInterface<BlockBuffer> &blockstorage_;
            const LogicalBlockRegion region_;
        };

        BlockStorageMultiplier() = delete;
        BlockStorageMultiplier(BlockStorageInterface<BlockBuffer> &blockstorage, const MultiplyRule rule) : blockstorage_(blockstorage), rule_(rule)
        {
        }
        MultipliedBlockStorage GetMultipliedBlockStorage(const int storage_index)
        {
            return MultipliedBlockStorage(blockstorage_, rule_.GetRule(storage_index));
        }

    private:
        BlockStorageInterface<BlockBuffer> &blockstorage_;
        const MultiplyRule rule_;
    };
}