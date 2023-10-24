#pragma once

#include <pybind11/pybind11.h>

/*
Why this mutex is necessary?
Every pybind function called from python naturally hold the GIL. Most of the function will hold the GIL in the entire lifespan. But not for batch_read. 
batch_read will release GIL at some point, so the folly threads which get its "tasks" can acquire the GIL to call some python function. Which, 
if the python script is multithreaded, another pybind function can be called. In this case, def read() is called. def read() also use folly future too. 
Both function share the same pool of thread. So, batch_read thread which does not have the GIL, can exhaust all the threads in the pool. And those threads
are all waiting for the GIL.
read thread which does have the GIL, waits for the result of the future, which will never return as there is no thread available in the pool
Therefore, deadlock occurs.
As a short term fix, this is added to ensure only single thread at pybind->c++ layer. However, as mention above, every pybind already has the GIL. 
So when the new function called is waiting for the mutex, the GIL is needed to be released. It enables the other running thread (def batch_read) and its 
task runners can acquire the thread.
*/
class SingleThreadMutexHolder {
private:
    [[nodiscard]] static std::lock_guard<std::mutex> ensure_single_thread_cpp_pybind_entry() {
        py::gil_scoped_release release;
        static std::mutex single_thread_mutex;
        single_thread_mutex.lock(); //This is a hack for the mandatory std::adopt_lock below
        return {single_thread_mutex, std::adopt_lock}; //Copy list-initialization will be used if the list is incomplete. 
    };
    std::lock_guard<std::mutex> single_thread_lck = ensure_single_thread_cpp_pybind_entry();
};
