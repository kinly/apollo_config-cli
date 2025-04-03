#pragma once
#include <atomic>
#include <chrono>
#include <cmath>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>

namespace util {
class circuit_breaker {
 public:
  struct config {
    using delay_strategy = std::function<std::chrono::milliseconds(int)>;
    using event_callback = std::function<void()>;

    int max_failures_before_cooldown = 15;
    delay_strategy get_delay = create_exponential_backoff(1000, 2);
    std::chrono::milliseconds cooldown_duration = std::chrono::seconds(30);

    event_callback on_circuit_open = [] {};
    event_callback on_circuit_close = [] {};

    static delay_strategy create_exponential_backoff(int base_delay_ms, double factor, int max_delay_ms = 60000) {
      return [=](int failures) {
        auto delay = base_delay_ms * std::pow(factor, failures);
        delay = std::min(delay, static_cast<double>(max_delay_ms));
        return std::chrono::milliseconds(static_cast<int>(delay));
      };
    }

    static delay_strategy create_linear_delay(int increment_ms) {
      return [increment_ms](int failures) {
        return std::chrono::milliseconds(failures * increment_ms);
      };
    }
  };

 private:
  struct alignas(64) atomic_state {
    std::atomic<int> failures{0};
    std::atomic<uint64_t> next_retry_time{0};
  };

  config _cfg;
  mutable std::shared_mutex _cfg_mutex;
  atomic_state _state;
  std::mutex _event_mutex;

  static uint64_t time_to_msec(const std::chrono::steady_clock::time_point& tp) {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count());
  }

  static std::chrono::steady_clock::time_point msec_to_time(uint64_t msec) {
    return std::chrono::steady_clock::time_point(std::chrono::milliseconds(msec));
  }

 public:
  explicit circuit_breaker(config cfg = {}) {
    update_config(std::move(cfg));
  }

  class raii_scoped_attempt {
    circuit_breaker& _cb;
    bool _success = false;
    bool _executed = false;

   public:
    explicit raii_scoped_attempt(circuit_breaker& cb) noexcept : _cb(cb) {
      _executed = _cb.allow();
    }

    ~raii_scoped_attempt() noexcept {
      if (_executed) {
        if (_success)
          _cb.record_success();
        else
          _cb.record_failure();
      }
    }

    explicit operator bool() const noexcept {
      return _executed;
    }

    void set_success(bool success = true) noexcept {
      _success = success;
    }
  };

  void update_config(config new_cfg) {
    if (new_cfg.max_failures_before_cooldown <= 0) {
      throw std::invalid_argument("Failure threshold must be positive");
    }

    std::unique_lock<std::shared_mutex> cfg_lock(_cfg_mutex);
    _cfg = std::move(new_cfg);
  }

  bool allow() const noexcept {
    const uint64_t current_next_retry = _state.next_retry_time.load(std::memory_order_acquire);
    const auto now_msec = time_to_msec(std::chrono::steady_clock::now());

    if (now_msec >= current_next_retry) {
      std::shared_lock<std::shared_mutex> cfg_shared_lock(_cfg_mutex);
      const int failures = _state.failures.load(std::memory_order_relaxed);

      if (failures < _cfg.max_failures_before_cooldown) {
        return true;
      }
    }
    return false;
  }

 private:
  void record_success() noexcept {
    std::shared_lock<std::shared_mutex> cfg_shared_lock(_cfg_mutex);
    _state.failures.store(0, std::memory_order_relaxed);
    _state.next_retry_time.store(0, std::memory_order_release);

    std::lock_guard<std::mutex> event_lock(_event_mutex);
    _cfg.on_circuit_close();
  }

  void record_failure() noexcept {
    std::shared_lock<std::shared_mutex> cfg_shared_lock(_cfg_mutex);
    const auto prev_failures = _state.failures.fetch_add(1, std::memory_order_acq_rel);
    const int new_failures = prev_failures + 1;

    const auto now = std::chrono::steady_clock::now();
    uint64_t new_retry_time;

    if (new_failures >= _cfg.max_failures_before_cooldown) {
      new_retry_time = time_to_msec(now + _cfg.cooldown_duration);

      std::lock_guard<std::mutex> event_lock(_event_mutex);
      _cfg.on_circuit_open();
    } else {
      const auto delay = _cfg.get_delay(new_failures);
      new_retry_time = time_to_msec(now + delay);
    }

    _state.next_retry_time.store(new_retry_time, std::memory_order_release);
  }
};
}  // namespace util
