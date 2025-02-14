#ifndef BMTASK__H__INCLUDED__
#define BMTASK__H__INCLUDED__
/*
Copyright(c) 2020 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

For more information please visit:  http://bitmagic.io
*/

/*! \file bmtask.h
    \brief Task definitions for parallel programming with BitMagic

    The design intent is to make tasks reasonably compatible with
    different threading and execution models, possibly with different
    languages and non-C++ runtimes.

*/
#include <atomic>
#include <functional>
#include <vector>


#include "bmbuffer.h"

namespace bm
{

/** @defgroup bmtasks Task parallel programming
    Task parallel programming compatible with different execution models
    and runtimes 

    @ingroup bmagic
*/

/** Typedef for a call-back function pointer (pthread conformant signature)
    @ingroup bmtasks
*/
typedef void* (*task_func_type)(void*);

/** Typedef for a call-back functional for lambda capture
    @ingroup bmtasks
*/
typedef std::function<int(void*)> task_function_t;



/** BitMagic task with a captured function
    @ingroup bmtasks
*/
struct task_descr
{
    enum task_flags
    {
        no_flag = 0,     ///< no flag specified
        barrier_ok = 1u,  ///< barrier waits all prev.tasks done without error
        barrier_any = (1u << 1), ///< barrier waits all prev tasks done (success or not)
        barrier_ok_delayed = (1u << 2)
    };

    task_function_t  func;      ///< captured function callback
    void*            argp;      ///< arg pointer


    bm::id64_t              flags;     ///< task flags to designate barriers
    int                     err_code;  ///< error code
    std::atomic_bool        done;      ///< 0 - pending

    // ----------------------------------------------------
    // Construction
    //
    task_descr() BMNOEXCEPT {}

    task_descr(const task_descr& td)
    {
        func = td.func;
        argp = td.argp;
        flags = td.flags;
        err_code = td.err_code;
        done.store(td.done.load()); // atomic operation
    }

    task_descr(task_function_t f, void* argptr = 0) noexcept
    {
        this->init(f, argptr);
    }

    void init(task_function_t  f, void* argptr) noexcept
    {
        func = f; argp = argptr; done = 0; flags = no_flag;
    }

    int run()
    {
        err_code = func(argp);
        done.store(1, std::memory_order_release);
        return err_code;
    }
};



/**
    Interface definition (base class) for a group of tasks (batch)
    @ingroup bmtasks
 */
class task_batch_base
{
public:
    typedef unsigned size_type;
public:
    virtual ~task_batch_base() {}

    /// Return size of batch
    virtual size_type size() const = 0;

    /// Get task by index in the batch
    /// @param task_idx - task index in the batch
    /// @return task description
    virtual bm::task_descr* get_task(size_type task_idx) = 0;

};

/**
    Basic implementation for collection of tasks for parallel execution
    @ingroup bmtasks
 */
template<typename BVAlloc>
class task_batch : public task_batch_base
{
public:
    typedef BVAlloc                     bv_allocator_type;
    typedef task_batch_base::size_type  size_type;

    typedef
    std::vector<bm::task_descr> task_vector_type;

    // disabled (error aunder MSVC related to vector impl for complex types
    #if 0
    typedef
    bm::heap_vector<bm::task_descr, bv_allocator_type, true> task_vector_type;
    #endif

public:

    /// task_batch_base intreface implementation
    //@{
    virtual size_type size() const BMNOEXCEPT { return (size_type) task_vect_.size(); }
    virtual
    bm::task_descr* get_task(size_type task_idx)
        { return &task_vect_[task_idx]; }

    //@}


    /// Get access to internal task vector
    ///
    task_vector_type& get_task_vector()  BMNOEXCEPT { return task_vect_; }
    const task_vector_type& get_task_vector() const  BMNOEXCEPT
        { return task_vect_; }

    void add(task_function_t  f, void* argptr)
    {
        task_vect_.emplace_back(bm::task_descr(f, argptr));
//        bm::task_descr& tdescr = task_vect_.add();
//        tdescr.init(f, argptr);
    }

protected:
    task_vector_type      task_vect_; ///< list of tasks
};


/**
    Run task batch sequentially

    Function is used for testing and debugging purposes or as a reference
    to implement custom parallel executors.

    @param tasks - collection of tasks to run
    @ingroup bmtasks
 */
inline
void run_task_batch(task_batch_base & tasks)
{
    task_batch_base::size_type batch_size = tasks.size();
    for (task_batch_base::size_type i = 0; i < batch_size; ++i)
    {
        bm::task_descr* tdescr = tasks.get_task(i);
        tdescr->argp = tdescr; // restore the self referenece
        tdescr->run();
    } // for
}


/**
    "noexcept" traits detection for T::lock()
    @internal
    @ingroup bmtasks
 */
template <typename T>
struct is_lock_noexcept
{
#if BM_DONT_WANT_TYPE_TRAITS_HEADER // not used
    constexpr static bool value = noexcept(((T*)nullptr)->lock());
#else
    constexpr static bool value = noexcept(std::declval<T>().lock());
#endif
};

/**
    Simple scoped lock guard
    @internal
    @ingroup bmtasks
 */
template<typename Lock> class lock_guard
{
public:
    lock_guard(Lock& lk) noexcept(bm::is_lock_noexcept<Lock>::value)
        : lk_(lk) {
        lk_.lock();
    }
    ~lock_guard() { lk_.unlock(); }
private:
    lock_guard(const lock_guard<Lock>&) = delete;
    lock_guard<Lock>& operator=(const lock_guard<Lock>&) = delete;
private:
    Lock& lk_;
};



} // namespace bm

#endif

