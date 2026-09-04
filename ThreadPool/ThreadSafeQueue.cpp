#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <vector>

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

  void notify_all() { cv_.notify_all(); }
};

class ThreadPool {
private:
  std::vector<std::thread> workers;
  ThreadSafeQueue<std::function<void()>> task_queue;
  std::atomic<bool> stop_flag{false};

public:
  explicit ThreadPool(size_t thread_count) {
    for (size_t i = 0; i < thread_count; ++i) {
      workers.emplace_back([this] {
        while (true) {
          std::function<void()> task;
          task_queue.wait_and_pop(task);

          if (stop_flag && task_queue.empty()) {
            break;
          }
          if (task) {
            task();
          }
        }
      });
    }
  }

  template <typename F, typename... Args>
  auto enqueue(F &&f, Args &&...args)
      -> std::future<typename std::invoke_result<F, Args...>::type> {
    using return_type = typename std::invoke_result<F, Args...>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<return_type> res = task->get_future();

    task_queue.push([task]() { (*task)(); });

    return res;
  }

  ~ThreadPool() {
    stop_flag = true;

    for (size_t i = 0; i < workers.size(); ++i) {
      task_queue.push([] {});
    }

    task_queue.notify_all();

    for (std::thread &worker : workers) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }
};
