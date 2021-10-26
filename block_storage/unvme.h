#pragma once
#include "block_storage_interface.h"
#include "common/rtc.h"
#include <assert.h>
#include <unvme.h>
#include <unvme_nvme.h>

namespace HayaguiKvs
{
    class UnvmeWrapper
    {
    public:
        UnvmeWrapper() : ns_(unvme_open("04:00.0"))
        {
            //showInfo();
        }
        ~UnvmeWrapper()
        {
            unvme_close(ns_);
        }
        void showInfo()
        {
            printf("qc=%d/%d qs=%d/%d bc=%#lx bs=%d maxbpio=%d\n", ns_->qcount,
                   ns_->maxqcount, ns_->qsize, ns_->maxqsize, ns_->blockcount,
                   ns_->blocksize, ns_->maxbpio);
        }
        void HardWrite()
        {
            vfio_dma_t dma;
            unvme_alloc2(ns_, &dma, ns_->blocksize); // dummy
            u32 cdw10_15[6];                         // dummy
            int stat = unvme_cmd(ns_, 0, NVME_CMD_FLUSH, ns_->id, dma.buf, 512, cdw10_15, 0);
            if (stat)
            {
                printf("failed to sync");
            }
            unvme_free2(ns_, &dma);
        }
        u16 GetBlockSize() const
        {
            return ns_->blocksize;
        }
        u64 GetBlockCount() const
        {
            return ns_->blockcount;
        }
        int Apoll(unvme_iod_t iod)
        {
            uint64_t endtime = RtcTaker::get() + 1000L * 1000 * 1000;
            while (true)
            {
                if (RtcTaker::get() > endtime)
                {
                    return -1;
                }
                {
                    if (iod == (unvme_iod_t)1)
                    {
                        return 0;
                    }
                    if (unvme_apoll(iod, 0) == 0)
                    {
                        return 0;
                    }
                }
                sched_yield();
            }
        }
        int ApollWithoutWait(unvme_iod_t iod)
        {
            if (iod == (unvme_iod_t)1)
            {
                return 0;
            }
            return unvme_apoll(iod, 0);
        }
        int Write(const void *buf, u64 slba, u32 nlb)
        {
            return unvme_write(ns_, 0, buf, slba, nlb);
        }
        int Read(void *buf, u64 slba, u32 nlb)
        {
            return unvme_read(ns_, 0, buf, slba, nlb);
        }
        unvme_iod_t Aread(void *buf, u64 slba, u32 nlb)
        {
            return unvme_aread(ns_, 0, buf, slba, nlb);
        }
        unvme_iod_t Awrite(const void *buf, u64 slba, u32 nlb)
        {
            return unvme_awrite(ns_, 0, buf, slba, nlb);
        }
        void Alloc(vfio_dma_t *dma, size_t size)
        {
            if (size > 2 * 1024 * 1024)
            {
                printf("unvme dma memory allocation failure.\n");
                abort();
            }
            unvme_alloc2(ns_, dma, size);
        }
        int Free(vfio_dma_t *dma)
        {
            return unvme_free2(ns_, dma);
        }

    private:
        const unvme_ns_t *ns_;
    };

    class UnvmeBlockStorage final : public BlockStorageInterface<GenericBlockBuffer>
    {
    public:
        UnvmeBlockStorage()
        {
        }
        virtual ~UnvmeBlockStorage()
        {
        }
        virtual Status Open() override
        {
            return Status::CreateOkStatus();
        }
        virtual LogicalBlockAddress GetMaxAddress() const override
        {
            return LogicalBlockAddress(unvme_wrapper_.GetBlockCount() - 1);
        }

    private:
        virtual Status ReadInternal(const LogicalBlockAddress address, GenericBlockBuffer &buffer) override
        {
            vfio_dma_t dma;
            unvme_wrapper_.Alloc(&dma, BlockBufferInterface::kSize);
            if (unvme_wrapper_.Read(dma.buf, address.GetRaw(), 1)) {
                return Status::CreateErrorStatus();
            }
            buffer.CopyFrom((uint8_t *)dma.buf, 0, BlockBufferInterface::kSize);
            unvme_wrapper_.Free(&dma);
            return Status::CreateOkStatus();
        }
        virtual Status WriteInternal(const LogicalBlockAddress address, const GenericBlockBuffer &buffer) override
        {
            vfio_dma_t dma;
            unvme_wrapper_.Alloc(&dma, BlockBufferInterface::kSize);
            buffer.CopyTo((uint8_t *)dma.buf, 0, BlockBufferInterface::kSize);
            if (unvme_wrapper_.Write(dma.buf, address.GetRaw(), 1)) {
                return Status::CreateErrorStatus();
            }
            unvme_wrapper_.Free(&dma);
            return Status::CreateOkStatus();
        }
        UnvmeWrapper unvme_wrapper_;
    };
}