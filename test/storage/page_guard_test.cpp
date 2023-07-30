//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // multi Drop
  {
    auto guarded_page2 = bpm->FetchPageBasic(page_id_temp);
    EXPECT_EQ(page0->GetData(), guarded_page2.GetData());
    EXPECT_EQ(page0->GetPageId(), guarded_page2.PageId());
    EXPECT_EQ(1, page0->GetPinCount());

    guarded_page2.Drop();

    EXPECT_EQ(0, page0->GetPinCount());
  }
  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

// NOLINTNEXTLINE
TEST(PageGuardTest, ReadWriteGuardTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = ReadPageGuard(bpm.get(), page0);  // no pin and no latch

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  // multi Drop
  {
    auto guarded_page2 = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(page0->GetData(), guarded_page2.GetData());
    EXPECT_EQ(page0->GetPageId(), guarded_page2.PageId());
    EXPECT_EQ(2, page0->GetPinCount());

    guarded_page2.Drop();

    EXPECT_EQ(1, page0->GetPinCount());
  }
  bpm->UnpinPage(page0->GetPageId(), false);
  EXPECT_EQ(0, page0->GetPinCount());

  auto write_guard = WritePageGuard(bpm.get(), page0);
  EXPECT_EQ(page0->GetData(), write_guard.GetData());
  EXPECT_EQ(page0->GetPageId(), write_guard.PageId());
  EXPECT_EQ(0, page0->GetPinCount());

  // write_guard.Drop();
  EXPECT_EQ(0, page0->GetPinCount());

  {
    auto write_guard2 = bpm->FetchPageWrite(page_id_temp);
    EXPECT_EQ(page0->GetData(), write_guard2.GetData());
    EXPECT_EQ(page0->GetPageId(), write_guard2.PageId());
    EXPECT_EQ(1, page0->GetPinCount());
  }

  EXPECT_EQ(0, page0->GetPinCount());
  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
