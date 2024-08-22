
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  const std::lock_guard<std::mutex> guard(latch_);
  auto new_page_id = AllocatePage();
  frame_id_t frame_id;
  // We need to find frame id.
  if (!free_list_.empty()) {
    // buffer pool has free frames, get frame id
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // no free frames, call lru-k, get frame id
    if (!replacer_->Evict(&frame_id)) {
      // cannot evict
      return nullptr;
    }
    // if function reach here, we have got a free frame.
    // if dirty, write back to the disk
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }
  // update meta data
  page_table_[new_page_id] = frame_id;
  auto &current_page = pages_[frame_id];

  current_page.page_id_ = new_page_id;
  current_page.is_dirty_ = false;
  current_page.pin_count_ = 1;
  // Zeroes out the data that is held within the page.
  current_page.ResetMemory();

  // update replacer
  replacer_->RecordAccess(frame_id);
  // pin this frame
  replacer_->SetEvictable(frame_id, false);
  *page_id = new_page_id;

  return &current_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  const std::lock_guard<std::mutex> guard(latch_);
  // First search for page_id in the buffer pool.
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    // set replacer
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  // If not found, pick a replacement frame from the free list
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // call replacer
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // write back if dirty
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }
  page_table_[page_id] = frame_id;
  // update meta data
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = page_id;
  // read the page from disk by calling disk_manager_->ReadPage()
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  // set replacer
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  // If page_id is not in the buffer pool or its pin count is already 0, return false.
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  // cannot replace origin dirty flag
  pages_[frame_id].is_dirty_ |= is_dirty;
  // Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
  if (pages_[frame_id].pin_count_ > 0) {
    pages_[frame_id].pin_count_--;
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
  // pin_count = 0, cannot unpin
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  // check valid
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  // check exist
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());

  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  const std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].is_dirty_ && pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);
  // If page_id is not in the buffer pool, do nothing and return true.
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  // If the page is pinned and cannot be deleted, return false immediately.
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  // if dirty, write back
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  page_table_.erase(page_id);
  // update meta
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
