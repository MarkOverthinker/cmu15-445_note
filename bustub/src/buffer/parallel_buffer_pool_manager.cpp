//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances), pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  for (size_t n = 0; n < num_instances; ++n) {
    BufferPoolManagerInstance *temp = new BufferPoolManagerInstance(pool_size, disk_manager, log_manager);
    temp->num_instances_ = static_cast<uint32_t>(num_instances);
    temp->instance_index_ = static_cast<uint32_t>(n);
    temp->next_page_id_ = temp->instance_index_;
    buffer_pool_managers_.emplace_back(temp);
  }
  start_index_ = 0;
}
// Allocate and create individual BufferPoolManagerInstances

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t n = 0; n < num_instances_; ++n) {
    delete buffer_pool_managers_[n];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  size_t ans = page_id % num_instances_;
  return buffer_pool_managers_[ans];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *pool = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return pool->FetchPgImp(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  std::lock_guard<std::mutex> lck(lat_);
  BufferPoolManagerInstance *pool = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return pool->UnpinPgImp(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *pool = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return pool->FlushPgImp(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> lck(lat_);

  for (size_t n = 0; n < num_instances_; ++n) {
    BufferPoolManagerInstance *pool = buffer_pool_managers_[start_index_];
    // pool->next_page_id_ += (num_instances_-1);
    Page *ans = pool->NewPgImp(page_id);
    start_index_ = (start_index_ + 1) % num_instances_;
    if (ans != nullptr) {
      return ans;
    }
  }
  start_index_ = (start_index_ + 1) % num_instances_;
  return nullptr;
  // for(size_t n = 0 ;n < num_instances_ ; ++n){
  // BufferPoolManagerInstance *pool = buffer_pool_managers_[starter_index_];
  // Page *ans = pool->NewPgImp(page_id);
  // starter_index_ = (starter_index_+1)%num_instances_;
  // if(ans!=nullptr) return ans;
  // }
  // return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *pool = dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return pool->DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto n : buffer_pool_managers_) {
    n->FlushAllPgsImp();
  }
}

}  // namespace bustub
