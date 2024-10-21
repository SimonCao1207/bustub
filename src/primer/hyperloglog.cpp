#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0), n_bits(n_bits) {
  num_registers = std::pow(2, n_bits);
  registers.assign(num_registers, 0);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  /** @TODO(student) Implement this function! */
  for (uint64_t i = BITSET_CAPACITY - 1 - n_bits; i >= 0; i--) {
    if (bset.test(i)) {
      return BITSET_CAPACITY - i - n_bits;
    }
  }
  return BITSET_CAPACITY - n_bits;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  hash_t hash_val = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bset = ComputeBinary(hash_val);

  uint64_t position_left_most_one = PositionOfLeftmostOne(bset);
  uint64_t index = (n_bits == 0) ? 0 : bset.to_ullong() >> (BITSET_CAPACITY - n_bits);
  registers[index] = std::max(registers[index], position_left_most_one);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  if (num_registers == 0) {
    return;
  }
  double sum_ = 0;
  for (unsigned long i = 0; i < num_registers; i++) {
    double p = std::pow(2, registers[i]);
    sum_ += 1.0 / p;
  }
  double harmonic_sum_ = 1.0 / sum_;
  double estimate = CONSTANT * num_registers * num_registers * harmonic_sum_;

  cardinality_ = static_cast<size_t>(estimate);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
