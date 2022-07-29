//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include <cassert>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  directory_page_id_ = INVALID_PAGE_ID;
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetGlobalDepthMask() & Hash(key);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *dir_page;
  table_create_latch_.WLock();
  if (directory_page_id_ == INVALID_PAGE_ID) {
    Page *new_page = buffer_pool_manager_->NewPage(&directory_page_id_);
    assert(new_page != nullptr);
    dir_page = reinterpret_cast<HashTableDirectoryPage *>(new_page->GetData());
    assert(directory_page_id_ != INVALID_PAGE_ID);
    dir_page->SetPageId(directory_page_id_);
    page_id_t new_bucket_page_id;
    new_page = nullptr;
    new_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
    assert(new_page != nullptr);
    dir_page->SetBucketPageId(0, new_bucket_page_id);
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
    assert(buffer_pool_manager_->UnpinPage(new_bucket_page_id, true));
  }
  table_create_latch_.WUnlock();
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page != nullptr);
  dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  assert(bucket_page_id != INVALID_PAGE_ID);
  Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  assert(bucket_page != nullptr);
  return bucket_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPageData(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(dir_page != nullptr);
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  assert(bucket_page_id != INVALID_PAGE_ID);
  Page *bucket_page = FetchBucketPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPageData(bucket_page);
  assert(bucket_page != nullptr);
  assert(bucket != nullptr);
  bucket_page->RLatch();
  if (bucket_page_id != static_cast<page_id_t>(KeyToPageId(key, dir_page))) {
    bucket_page->RUnlatch();
    table_latch_.RUnlock();
    return false;
  }
  bool isget = bucket->GetValue(key, comparator_, result);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  table_latch_.RUnlock();
  bucket_page->RUnlatch();
  return isget;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(dir_page != nullptr);
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  assert(bucket_page_id != INVALID_PAGE_ID);
  Page *bucket_page = FetchBucketPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPageData(bucket_page);
  assert(bucket != nullptr);
  assert(bucket_page != nullptr);
  bucket_page->WLatch();
  if (!bucket->IsFull()) {
    bool ret = bucket->Insert(key, value, comparator_);
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, ret));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    bucket_page->WUnlatch();
    table_latch_.RUnlock();
    // all_latch_.RUnlock();
    return ret;
  }
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  bucket_page->WUnlatch();
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(dir_page != nullptr);
  // declare 2 bucket page id
  page_id_t old_bucket_page_id = KeyToPageId(key, dir_page);
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  // declare and initial 2 bucket page
  Page *old_bucket_page = FetchBucketPage(old_bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *old_bucket = FetchBucketPageData(old_bucket_page);
  Page *new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *new_bucket = FetchBucketPageData(new_bucket_page);
  old_bucket_page->WLatch();
  new_bucket_page->WLatch();
  if (old_bucket->IsFull()) {
    // get all map in old bucket page and reset old bucket page
    uint32_t num_readable = 0;
    MappingType *copy = old_bucket->GetAllMap(&num_readable);
    assert(num_readable != 0);
    old_bucket->ReSet();
    new_bucket->ReSet();
    // get bucket index
    uint32_t old_bucket_idx = KeyToDirectoryIndex(key, dir_page);
    uint32_t new_bucket_idx = dir_page->GetSplitImageIndex(old_bucket_idx);
    // increase global depth
    if (dir_page->GetGlobalDepth() == dir_page->GetLocalDepth(old_bucket_idx)) {
      dir_page->IncrGlobalDepth();
    }
    // set local depth of old and new
    dir_page->IncrLocalDepth(old_bucket_idx);
    dir_page->SetLocalDepth(new_bucket_idx, dir_page->GetLocalDepth(old_bucket_idx));
    // get mass of local depth
    uint32_t local_depth_mass = dir_page->GetLocalDepthMask(old_bucket_idx);
    // repoint_directory_to_bucket
    uint32_t size_of_dir = dir_page->Size();
    for (uint32_t i = 0; i < size_of_dir; i++) {
      if ((i & local_depth_mass) == old_bucket_idx) {
        dir_page->SetBucketPageId(i, old_bucket_page_id);
      } else if ((i & local_depth_mass) == new_bucket_idx) {
        dir_page->SetBucketPageId(i, new_bucket_page_id);
      }
    }
    // reload all map into the 2 page
    for (uint32_t i = 0; i < num_readable; i++) {
      if (KeyToPageId(copy[i].first, dir_page) == static_cast<uint32_t>(old_bucket_page_id)) {
        old_bucket->Insert(copy[i].first, copy[i].second, comparator_);
      } else if (KeyToPageId(copy[i].first, dir_page) == static_cast<uint32_t>(new_bucket_page_id)) {
        new_bucket->Insert(copy[i].first, copy[i].second, comparator_);
      }
    }
    delete[] copy;
  }
  // unpin
  assert(buffer_pool_manager_->UnpinPage(old_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(new_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
  old_bucket_page->WUnlatch();
  new_bucket_page->WUnlatch();
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(dir_page != nullptr);
  uint32_t bucket_page_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  assert(bucket_page_id != INVALID_PAGE_ID);
  Page *bucket_page = FetchBucketPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPageData(bucket_page);
  assert(bucket_page != nullptr);
  assert(bucket != nullptr);
  bucket_page->WLatch();
  bool ret = bucket->Remove(key, value, comparator_);
  if (!ret) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    table_latch_.RUnlock();
    bucket_page->WUnlatch();
    return ret;
  }
  uint32_t dep = dir_page->GetLocalDepth(bucket_page_idx);
  if (bucket->IsEmpty() && dep != 0) {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    table_latch_.RUnlock();
    bucket_page->WUnlatch();
    Merge(transaction, key, value);
  } else {
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    table_latch_.RUnlock();
    bucket_page->WUnlatch();
  }
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // fetch directory page
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  assert(dir_page != nullptr);
  // fetch the bucket of key
  uint32_t bucket_page_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  assert(bucket_page_id != INVALID_PAGE_ID);
  Page *bucket_page = FetchBucketPage(bucket_page_id);
  bucket_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPageData(bucket_page);
  assert(bucket_page != nullptr);
  assert(bucket != nullptr);
  uint32_t dep = dir_page->GetLocalDepth(bucket_page_idx);
  uint32_t bro_bucket_page_idx = dir_page->GetBroIndex(bucket_page_idx);
  page_id_t bro_bucket_page_id = dir_page->GetBucketPageId(bro_bucket_page_idx);
  assert(bro_bucket_page_id != INVALID_PAGE_ID);
  Page *bro_bucket_page = FetchBucketPage(bro_bucket_page_id);
  assert(bro_bucket_page != nullptr);
  bro_bucket_page->RLatch();
  uint32_t bro_dep = dir_page->GetLocalDepth(bro_bucket_page_idx);
  HASH_TABLE_BUCKET_TYPE *bro_bucket = FetchBucketPageData(bro_bucket_page);
  assert(bro_bucket != nullptr);
  if (!bucket->IsEmpty() || dep == 0) {
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(bro_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    bucket_page->RUnlatch();
    bro_bucket_page->RUnlatch();
    table_latch_.WUnlock();
  } else {
    // merge them if the num of readable map in bro is less enough
    if (dep == bro_dep && bro_bucket->NumReadable() < BUCKET_ARRAY_SIZE / 2) {
      uint32_t dir_size = dir_page->Size();
      // decrease local depth
      dir_page->DecrLocalDepth(bucket_page_idx);
      dir_page->DecrLocalDepth(bro_bucket_page_idx);
      size_t dep = dir_page->GetLocalDepth(bro_bucket_page_idx);
      // repoint
      for (uint32_t i = 0; i < dir_size; ++i) {
        if (dir_page->GetBucketPageId(i) == bucket_page_id || dir_page->GetBucketPageId(i) == bro_bucket_page_id) {
          dir_page->SetBucketPageId(i, bro_bucket_page_id);
          dir_page->SetLocalDepth(i, dep);
        }
      }
      // delete bucket
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
      assert(buffer_pool_manager_->DeletePage(bucket_page_id));
      // decrease global depth
      while (dir_page->CanShrink()) {
        dir_page->DecrGlobalDepth();
      }
      bucket_page->RUnlatch();
      if (bro_bucket->IsEmpty() && dir_page->GetLocalDepth(bro_bucket_page_idx) != 0) {
        assert(buffer_pool_manager_->UnpinPage(bro_bucket_page_id, false));
        bro_bucket_page->RUnlatch();
        assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
        table_latch_.WUnlock();
        Merge(transaction, key, value);
        return;
      }
      assert(buffer_pool_manager_->UnpinPage(bro_bucket_page_id, false));
      bro_bucket_page->RUnlatch();
      assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
      table_latch_.WUnlock();
      return;
    }
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(bro_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
    bucket_page->RUnlatch();
    bro_bucket_page->RUnlatch();
    table_latch_.WUnlock();
  }
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
