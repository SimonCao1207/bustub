#include "primer/hyperloglog_presto.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0), n_bits(n_leading_bits) {
  num_buckets = std::pow(2, n_leading_bits);
  dense_bucket_.assign(num_buckets, std::bitset<DENSE_BUCKET_SIZE>(0));
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::RightMostContiguousZeros(const std::bitset<BITSET_CAPACITY> &bset) -> uint64_t {
  uint64_t num_zeros = 0;
  for (uint64_t i = 0; i < BITSET_CAPACITY - n_bits; i++) {
    if (!bset.test(i)) {
      num_zeros++;
    } else {
      break;
    }
  }
  return num_zeros;
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::ReconstructFromDenseLayer(uint64_t &bucket_index) -> uint64_t {
  std::bitset<BITSET_CAPACITY> reconstruct_value;
  for (int j = 0; j < DENSE_BUCKET_SIZE; j++) {
    reconstruct_value[j] = dense_bucket_[bucket_index][j];
  }

  for (int j = 0; j < OVERFLOW_BUCKET_SIZE; j++) {
    reconstruct_value[j + DENSE_BUCKET_SIZE] = overflow_bucket_[bucket_index][j];
  }
  return static_cast<uint64_t>(reconstruct_value.to_ullong());
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash_val = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bset(hash_val);
  uint64_t bucket_index = (n_bits == 0) ? 0 : bset.to_ullong() >> (BITSET_CAPACITY - n_bits);
  uint64_t num_consecutive = RightMostContiguousZeros(bset);
  num_consecutive = std::max(num_consecutive, ReconstructFromDenseLayer(bucket_index));
  std::bitset<DENSE_BUCKET_SIZE> dense_bset;
  std::bitset<OVERFLOW_BUCKET_SIZE> overfl_bset;
  std::bitset<BITSET_CAPACITY> deltas(num_consecutive);

  for (int i = 0; i < DENSE_BUCKET_SIZE; i++) {
    dense_bset[i] = deltas[i];
  }

  for (int i = 0; i < OVERFLOW_BUCKET_SIZE; i++) {
    overfl_bset[i] = deltas[i + DENSE_BUCKET_SIZE];
  }

  dense_bucket_[bucket_index] = dense_bset;
  if (!overfl_bset.none()) {
    overflow_bucket_[bucket_index] = overfl_bset;
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (num_buckets == 0) {
    return;
  }
  double sum_ = 0;

  for (unsigned long i = 0; i < num_buckets; i++) {
    double p = std::pow(2, ReconstructFromDenseLayer(i));
    sum_ += 1.0 / p;
  }
  double harmonic_sum_ = 1.0 / sum_;
  double estimate = CONSTANT * num_buckets * num_buckets * harmonic_sum_;

  cardinality_ = static_cast<size_t>(estimate);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
