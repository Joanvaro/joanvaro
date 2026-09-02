#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

template <typename T> class ThreadSafeQueue {
private:
  mutable std::mutex mtx_;
  std::queue<T> data_queue_;
  std::condition_variable cv_;

public:
  ThreadSafeQueue() = default;

  // Disable copy to avoid accidental data duplication under concurrency
  ThreadSafeQueue(const ThreadSafeQueue &) = delete;
  ThreadSafeQueue &operator=(const ThreadSafeQueue &) = delete;

  void push(T new_value) {
    std::lock_guard<std::mutex> lock(mtx_);
    data_queue_.push(std::move(new_value));
    cv_.notify_one(); // Wake up one waiting worker thread
  }

  void wait_and_pop(T &value) {
    std::unique_lock<std::mutex> lock(mtx_);
    // Sleep until the queue is not empty (handles spurious wakeups
    // automatically)
    cv_.wait(lock, [this] { return !data_queue_.empty(); });
    value = std::move(data_queue_.front());
    data_queue_.pop();
  }

  bool try_pop(T &value) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (data_queue_.empty()) {
      return false;
    }
    value = std::move(data_queue_.front());
    data_queue_.pop();
    return true;
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return data_queue_.empty();
  }
};
