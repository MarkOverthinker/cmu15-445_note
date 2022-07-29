//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool ans = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->emplace_back(array_[i].second);
      ans = true;
    }
  }
  return ans;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int available = -1;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i)) {
      if ((cmp(array_[i].first, key) == 0) && array_[i].second == value) {
        return false;
      }
    } else if (available == -1) {
      available = i;
    }
  }
  if (available == -1) {
    return false;
  }
  array_[available] = MappingType(key, value);
  SetOccupied(available);
  SetReadable(available);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (IsReadable(i)) {
      if ((cmp(array_[i].first, key) == 0) && array_[i].second == value) {
        RemoveAt(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  size_t index = bucket_idx / 8;
  size_t bit_index = bucket_idx % 8;
  uint8_t bit_mass = 1 << bit_index;
  bit_mass = ~bit_mass;
  readable_[index] = readable_[index] & bit_mass;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  size_t index = bucket_idx / 8;
  uint8_t bucket_bit = occupied_[index] >> (bucket_idx % 8);
  return (bucket_bit & 1) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  size_t index = bucket_idx / 8;
  uint8_t bit_mass = 1;
  bit_mass = bit_mass << (bucket_idx % 8);
  occupied_[index] = occupied_[index] | bit_mass;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  size_t index = bucket_idx / 8;
  uint8_t bucket_bit = readable_[index] >> (bucket_idx % 8);
  return (bucket_bit & 1) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  size_t index = bucket_idx / 8;
  uint8_t bit_mass = 1;
  bit_mass = bit_mass << (bucket_idx % 8);
  readable_[index] = readable_[index] | bit_mass;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  uint8_t mass = 255;
  uint8_t last_mass = 0;
  int n = BUCKET_ARRAY_SIZE % 8;
  size_t size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  last_mass = n == 8 ? mass : ((1 << n) - 1);
  if ((readable_[size - 1] & last_mass) != last_mass) {
    return false;
  }
  for (size_t i = 0; i < size - 1; i++) {
    if ((readable_[i] & mass) != mass) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t ans = 0;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      ++ans;
    }
  }
  return ans;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  uint8_t mass = 255;
  uint8_t last_mass = 0;
  size_t n = BUCKET_ARRAY_SIZE % 8;
  size_t size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  last_mass = n == 8 ? mass : ((1 << n) - 1);
  if ((readable_[size - 1] & last_mass) != 0) {
    return false;
  }
  for (size_t i = 0; i < size - 1; ++i) {
    if (readable_[i] != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetAllMap(uint32_t *num) {
  *num = NumReadable();
  MappingType *ret = new MappingType[*num];
  for (uint32_t i = 0, idx = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      ret[idx++] = array_[i];
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ReSet() {
  memset(occupied_, 0, (BUCKET_ARRAY_SIZE - 1) / 8 + 1);
  memset(readable_, 0, (BUCKET_ARRAY_SIZE - 1) / 8 + 1);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
