#pragma once

#include <pybind11/pybind11.h>

/*
Why this mutex is necessary?
Every pybind function called from python naturally hold the GIL. Most of the function will hold the GIL in the entire
lifespan. But not for batch_read. batch_read will release GIL at some point, so the folly threads which get its "tasks"
can acquire the GIL to call some python function. Which, if the python script is multithreaded, another pybind function
can be called. In this case, def read() is called. def read() also use folly future too. Both function share the same
pool of thread. So, batch_read thread which does not have the GIL, can exhaust all the threads in the pool. And those
threads are all waiting for the GIL. read thread which does have the GIL, waits for the result of the future, which will
never return as there is no thread available in the pool Therefore, deadlock occurs. As a short term fix, this is added
to ensure only single thread at pybind->c++ layer. However, as mention above, every pybind already has the GIL. So when
the new function called is waiting for the mutex, the GIL is needed to be released. It enables the other running thread
(def batch_read) and its task runners can acquire the thread.
*/
class SingleThreadMutexHolder {
  private:
    inline static std::unique_ptr<std::mutex> single_thread_mutex = std::make_unique<std::mutex>();

    [[nodiscard]] static std::lock_guard<std::mutex> ensure_single_thread_cpp_pybind_entry() {
        py::gil_scoped_release release;
        single_thread_mutex->lock(); // This is a hack for the mandatory std::adopt_lock below
        return {*single_thread_mutex, std::adopt_lock
        }; // Copy list-initialization will be used if the list is incomplete.
    };
    std::lock_guard<std::mutex> single_thread_lck = ensure_single_thread_cpp_pybind_entry();

  public:
    /*
        https://man7.org/linux/man-pages/man3/pthread_atfork.3.html
        When fork is called in a multithreaded process, only the calling thread is duplicated in the child process. So
       locked mutex will stay locked in the child process. So special handling is required. The thread being forked must
       not have a running task. And the parent process may have another threads running task, locking the mutex. As
       other threads in the parent process won't be forked, at fork, it is safe to reset the mutex memory. The mutex
       cannot be simply unlocked as unlock() should only be called in the thread called lock() previously, which cannot
       be the thread being forked. Note: According to test, below mutex is automataically unlocked during fork. The
       observation deviates from the manual. So to play safe, mutex will manually unlocked anyway, if it is locked.
    */
    static void reset_mutex() {
        (void)single_thread_mutex.release();
        single_thread_mutex = std::make_unique<std::mutex>();
    }
};
