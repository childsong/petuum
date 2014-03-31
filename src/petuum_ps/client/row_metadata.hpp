#pragma once

#include <stdint.h>

namespace petuum {

struct RowMetadata {
public:
  RowMetadata() : clock_(0) { }

  explicit RowMetadata(int32_t clock):
    clock_(clock) {}

  RowMetadata(const RowMetadata& other) :
    clock_(other.clock_) { }

  RowMetadata operator=(const RowMetadata& other) {
    clock_ = other.clock_;
    return *this;
  }

  int32_t GetClock() const {
    return clock_;
  }

  void SetClock(int32_t clock) {
    clock_ = clock;
  }
private:
  int32_t clock_;
};

}
