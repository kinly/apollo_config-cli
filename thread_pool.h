#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace util {
class thread_pool {
 private:
  struct task_queue {
    std::mutex mutex;
    std::condition_variable_any cv;
    std::queue<std::function<void()>> tasks;
    std::atomic<size_t> task_count = 0;
  };

  std::atomic<bool> _stopped;
  std::vector<task_queue> _queues;
  std::vector<std::jthread> _workers;

 public:
  explicit thread_pool(size_t thread_count = std::thread::hardware_concurrency())
      : _stopped(false), _queues(thread_count > 0 ? thread_count : 1), _workers(thread_count > 0 ? thread_count : 1) {
    for (size_t i = 0; i < _workers.size(); ++i) {
      _workers[i] = std::jthread([this, i](std::stop_token stoken) {
        worker_loop(stoken, i);
      });
    }
  }

  ~thread_pool() {
    shutdown();
  }

  void shutdown() noexcept {
    if (_stopped.exchange(true))
      return;

    for (auto& worker : _workers) {
      worker.request_stop();
    }

    for (auto& one : _queues) {
      one.cv.notify_all();
    }

    for (auto& worker : _workers) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }

  template <typename function_tt>
  auto submit(function_tt&& task) -> std::future<std::invoke_result_t<function_tt>> {
    return submit("", std::forward<function_tt>(task));
  }

  template <typename function_tt>
  auto submit(const std::string& hash_key, function_tt&& task) -> std::future<std::invoke_result_t<function_tt>> {
    if (_stopped.load(std::memory_order_relaxed)) {
      throw std::runtime_error("submit to stopped thread_pool");
    }

    using result_type = std::invoke_result_t<function_tt>;
    using promise_type = std::promise<result_type>;

    auto promise = std::make_shared<promise_type>();
    auto future = promise->get_future();

    auto wrapped_task = [task = std::forward<function_tt>(task), promise]() {
      try {
        if constexpr (std::is_void_v<result_type>) {
          task();
          promise->set_value();
        } else {
          promise->set_value(task());
        }
      } catch (...) {
        promise->set_exception(std::current_exception());
      }
    };

    enqueue_task(hash_key, std::move(wrapped_task));
    return future;
  }

 private:
  size_t get_queue_index(const std::string& hash_key) const noexcept {
    return std::hash<std::string>{}(hash_key) % _queues.size();
  }

  size_t find_least_loaded_queue() const noexcept {
    size_t min_count = std::numeric_limits<size_t>::max();
    size_t selected = 0;

    for (size_t i = 0; i < _queues.size(); ++i) {
      const size_t count = _queues[i].task_count.load(std::memory_order_relaxed);
      if (count < min_count) {
        min_count = count;
        selected = i;
      }
    }
    return selected;
  }

  void enqueue_task(const std::string& hash_key, std::function<void()> task) {
    size_t idx = hash_key.empty() ? find_least_loaded_queue() : get_queue_index(hash_key);

    auto& one = _queues[idx];
    {
      std::lock_guard lock(one.mutex);
      one.tasks.emplace(std::move(task));
      one.task_count.store(one.tasks.size(), std::memory_order_relaxed);
    }
    one.cv.notify_one();
  }

  void worker_loop(std::stop_token stoken, size_t queue_idx) {
    auto& one = _queues[queue_idx];
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock lk(one.mutex);
        one.cv.wait(lk, stoken, [&one] {
          return !one.tasks.empty();
        });

        if (stoken.stop_requested() && one.tasks.empty())
          break;

        if (!one.tasks.empty()) {
          task = std::move(one.tasks.front());
          one.tasks.pop();
          one.task_count.store(one.tasks.size(), std::memory_order_relaxed);
        }
      }

      if (task)
        task();
    }

    std::unique_lock lk(one.mutex);
    while (!one.tasks.empty()) {
      auto task = std::move(one.tasks.front());
      one.tasks.pop();
      one.task_count.store(one.tasks.size(), std::memory_order_relaxed);
      lk.unlock();
      task();
      lk.lock();
    }
  }
};
}  // namespace util
