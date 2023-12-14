#ifndef BMALLOC__H__INCLUDED__
#define BMALLOC__H__INCLUDED__
/*
Copyright(c) 2002-2017 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

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

/*! \file bmalloc.h
    \brief Default SIMD friendly allocator 
*/

#include <stdlib.h>
#include <new>

namespace bm
{

#if defined(BMSSE2OPT) || defined(BMSSE42OPT)
#define BM_ALLOC_ALIGN 16
#endif
#if defined(BMAVX2OPT)
#define BM_ALLOC_ALIGN 32
#endif
#if defined(BMAVX512OPT)
#define BM_ALLOC_ALIGN 64
#endif


/*! 
    @defgroup alloc Allocator
    Memory allocation for bvector
    
    @ingroup bvector
 
    @{
 */

/*! 
  @brief Default malloc based bitblock allocator class.

  Functions allocate and deallocate conform to STL allocator specs.
  @ingroup alloc
*/
class block_allocator
{
public:
    /**
    The member function allocates storage for an array of n bm::word_t 
    elements, by calling malloc. 
    @return pointer to the allocated memory. 
    */
    static bm::word_t* allocate(size_t n, const void *)
    {
        bm::word_t* ptr;

#if defined(BM_ALLOC_ALIGN)
    #ifdef _MSC_VER
        ptr = (bm::word_t*) ::_aligned_malloc(n * sizeof(bm::word_t), BM_ALLOC_ALIGN);
    #else
        ptr = (bm::word_t*) ::_mm_malloc(n * sizeof(bm::word_t), BM_ALLOC_ALIGN);
    #endif
#else
        ptr = (bm::word_t*) ::malloc(n * sizeof(bm::word_t));
#endif
        if (!ptr)
            throw std::bad_alloc();
        return ptr;
    }

    /**
    The member function frees storage for an array of n bm::word_t 
    elements, by calling free. 
    */
    static void deallocate(bm::word_t* p, size_t) BMNOEXCEPT
    {
#ifdef BM_ALLOC_ALIGN
    # ifdef _MSC_VER
            ::_aligned_free(p);
    #else
            ::_mm_free(p);
    # endif
#else
        ::free(p);
#endif
    }

};



/*! @brief Default malloc based bitblock allocator class.

  Functions allocate and deallocate conform to STL allocator specs.
*/
class ptr_allocator
{
public:
    /**
    The member function allocates storage for an array of n void* 
    elements, by calling malloc. 
    @return pointer to the allocated memory. 
    */
    static void* allocate(size_t n, const void *)
    {
        void* ptr;
#if defined(BM_ALLOC_ALIGN)
    #ifdef _MSC_VER
        ptr = (bm::word_t*) ::_aligned_malloc(n * sizeof(void*), BM_ALLOC_ALIGN);
    #else
        ptr = (bm::word_t*) ::_mm_malloc(n * sizeof(void*), BM_ALLOC_ALIGN);
    #endif
#else
        ptr = (bm::word_t*) ::malloc(n * sizeof(void*));
#endif
//        void* ptr = ::malloc(n * sizeof(void*));
        if (!ptr)
            throw std::bad_alloc();
        return ptr;
    }

    /**
    The member function frees storage for an array of n bm::word_t 
    elements, by calling free. 
    */
    static void deallocate(void* p, size_t) BMNOEXCEPT
    {
#ifdef BM_ALLOC_ALIGN
    # ifdef _MSC_VER
            ::_aligned_free(p);
    #else
            ::_mm_free(p);
    # endif
#else
        ::free(p);
#endif
    }
};

/*! 
    @brief Pool of pointers to buffer cyclic allocations
*/
class pointer_pool_array
{
public:
    enum params
    {
        n_pool_max_size = BM_DEFAULT_POOL_SIZE
    };

    pointer_pool_array() : pool_ptr_(0), size_(0) 
    {
        allocate_pool(n_pool_max_size);
    }

    pointer_pool_array(const pointer_pool_array&) = delete;
    pointer_pool_array& operator=(const pointer_pool_array&) = delete;

    ~pointer_pool_array() 
    { 
        BM_ASSERT(size_ == 0); // at destruction point should be empty (otherwise it is a leak) 
        free_pool();
    }

    /// Push pointer to the pool (if it is not full)
    ///
    /// @return 0 if pointer is not accepted (pool is full)
    unsigned push(void* ptr) BMNOEXCEPT
    {
        if (size_ == n_pool_max_size - 1)
            return 0;
        pool_ptr_[size_++] = ptr;
        return size_;
    }

    /// Get a pointer if there are any vacant
    ///
    void* pop() BMNOEXCEPT
    {
        if (!size_)
            return 0;
        return pool_ptr_[--size_];
    }

    /// return stack size
    ///
    unsigned size() const BMNOEXCEPT { return size_; }

private:
    void allocate_pool(size_t pool_size)
    {
        BM_ASSERT(!pool_ptr_);
        pool_ptr_ = (void**)::malloc(sizeof(void*) * pool_size);
        if (!pool_ptr_)
            throw std::bad_alloc();
    }

    void free_pool() BMNOEXCEPT
    {
        ::free(pool_ptr_);
    }
private:
    void**     pool_ptr_;  ///< array of pointers in the pool
    unsigned   size_;      ///< current size
};

/**
    Allocation pool object
*/
template<class BA, class PA>
class alloc_pool
{
public:
    typedef BA  block_allocator_type;
    typedef PA  ptr_allocator_type;

public:

    alloc_pool() {}
    ~alloc_pool() { free_pools(); }

    void set_block_limit(size_t limit) BMNOEXCEPT
        { block_limit_ = limit; }

    bm::word_t* alloc_bit_block()
    {
        bm::word_t* ptr = (bm::word_t*)block_pool_.pop();
        if (!ptr)
            ptr = block_alloc_.allocate(bm::set_block_size, 0);
        return ptr;
    }
    
    void free_bit_block(bm::word_t* block) BMNOEXCEPT
    {
        BM_ASSERT(IS_VALID_ADDR(block));
        if (block_limit_) // soft limit set
        {
            if (block_pool_.size() >= block_limit_)
            {
                block_alloc_.deallocate(block, bm::set_block_size);
                return;
            }
        }
        if (!block_pool_.push(block))
            block_alloc_.deallocate(block, bm::set_block_size);
    }

    void free_pools() BMNOEXCEPT
    {
        bm::word_t* block;
        do
        {
            block = (bm::word_t*)block_pool_.pop();
            if (block)
                block_alloc_.deallocate(block, bm::set_block_size);
        } while (block);
    }

    /// return stack size
    ///
    unsigned size() const BMNOEXCEPT { return block_pool_.size(); }

protected:
    pointer_pool_array  block_pool_;
    BA                  block_alloc_;
    size_t              block_limit_ = 0; ///< soft limit for the pool of blocks
};


/*! @brief BM style allocator adapter. 

  Template takes parameters: 
  BA - allocator object for bit blocks 
  PA - allocator object for pointer blocks
  APool - Allocation pool
*/
template<class BA, class PA, class APool>
class mem_alloc
{
public:

    typedef BA      block_allocator_type;
    typedef PA      ptr_allocator_type;
    typedef APool   allocator_pool_type;

public:

    mem_alloc(const BA& block_alloc = BA(), const PA& ptr_alloc = PA()) BMNOEXCEPT
    : block_alloc_(block_alloc),
      ptr_alloc_(ptr_alloc),
      alloc_pool_p_(0)
    {}

    mem_alloc(const mem_alloc& ma) BMNOEXCEPT
        : block_alloc_(ma.block_alloc_),
          ptr_alloc_(ma.ptr_alloc_),
          alloc_pool_p_(0) // do not inherit pool (has to be explicitly defined)
    {}

    mem_alloc& operator=(const mem_alloc& ma) BMNOEXCEPT
    {
        block_alloc_ = ma.block_alloc_;
        ptr_alloc_ = ma.ptr_alloc_;
        // alloc_pool_p_ - do not inherit pool (has to be explicitly defined)
        return *this;
    }
    
    /*! @brief Returns copy of the block allocator object
    */
    block_allocator_type get_block_allocator() const BMNOEXCEPT
    { 
        return BA(block_alloc_); 
    }

    /*! @brief Returns copy of the ptr allocator object
    */
    ptr_allocator_type get_ptr_allocator() const BMNOEXCEPT
    { 
       return PA(block_alloc_); 
    }

    /*! @brief set pointer to external pool */
    void set_pool(allocator_pool_type* pool) BMNOEXCEPT
    {
        alloc_pool_p_ = pool;
    }

    /*! @brief get pointer to allocation pool (if set) */
    allocator_pool_type* get_pool() BMNOEXCEPT
    {
        return alloc_pool_p_;
    }

    /*! @brief Allocates and returns bit block.
        @param alloc_factor 
            indicated how many blocks we want to allocate in chunk
            total allocation is going to be bm::set_block_size * alloc_factor
            Default allocation factor is 1
    */
    bm::word_t* alloc_bit_block(unsigned alloc_factor = 1)
    {
        if (alloc_pool_p_ && alloc_factor == 1)
            return alloc_pool_p_->alloc_bit_block();
        return block_alloc_.allocate(bm::set_block_size * alloc_factor, 0);
    }

    /*! @brief Frees bit block allocated by alloc_bit_block.
    */
    void free_bit_block(bm::word_t* block, size_t alloc_factor = 1) BMNOEXCEPT
    {
        BM_ASSERT(IS_VALID_ADDR(block));
        if (alloc_pool_p_ && alloc_factor == 1)
            alloc_pool_p_->free_bit_block(block);
        else
            block_alloc_.deallocate(block, bm::set_block_size * alloc_factor);
    }

    /*! @brief Allocates GAP block using bit block allocator (BA).

        GAP blocks in BM library belong to levels. Each level has a 
        correspondent length described in bm::gap_len_table<>
        
        @param level GAP block level.
        @param glevel_len table of level lengths
    */
    bm::gap_word_t* alloc_gap_block(unsigned level, 
                                    const bm::gap_word_t* glevel_len)
    {
        BM_ASSERT(level < bm::gap_levels);
        unsigned len = 
            (unsigned)(glevel_len[level] / (sizeof(bm::word_t) / sizeof(bm::gap_word_t)));

        return (bm::gap_word_t*)block_alloc_.allocate(len, 0);
    }

    /*! @brief Frees GAP block using bot block allocator (BA)
    */
    void free_gap_block(bm::gap_word_t*   block,
                        const bm::gap_word_t* glevel_len)
    {
        BM_ASSERT(IS_VALID_ADDR((bm::word_t*)block));
         
        unsigned len = bm::gap_capacity(block, glevel_len);
        len /= (unsigned)(sizeof(bm::word_t) / sizeof(bm::gap_word_t));
        block_alloc_.deallocate((bm::word_t*)block, len);        
    }

    /*! @brief Allocates block of pointers.
    */
    void* alloc_ptr(size_t size)
    {
        BM_ASSERT(size);
        return ptr_alloc_.allocate(size, 0);
    }

    /*! @brief Frees block of pointers.
    */
    void free_ptr(void* p, size_t size) BMNOEXCEPT
    {
        if (p)
            ptr_alloc_.deallocate(p, size);
    }

    /**
        Get access to block allocator
     */
    BA& get_block_alloc() BMNOEXCEPT { return block_alloc_; }
protected:
    BA                     block_alloc_;
    PA                     ptr_alloc_;
    allocator_pool_type*   alloc_pool_p_;
};

typedef bm::alloc_pool<block_allocator, ptr_allocator> standard_alloc_pool;
typedef bm::mem_alloc<block_allocator, ptr_allocator, standard_alloc_pool> standard_allocator;

/** @} */


/// Aligned malloc (unlike classic malloc it throws bad_alloc exception)
///
/// To allocate temp bit-block use: bm::aligned_new_malloc(bm::set_block_alloc_size);
/// @internal
inline
void* aligned_new_malloc(size_t size)
{
    void* ptr;

#ifdef BM_ALLOC_ALIGN
#ifdef _MSC_VER
    ptr = ::_aligned_malloc(size, BM_ALLOC_ALIGN);
#else
    ptr = ::_mm_malloc(size, BM_ALLOC_ALIGN);
#endif
#else
    ptr = ::malloc(size);
#endif
    if (!ptr)
    {
#ifndef BM_NO_STL
    throw std::bad_alloc();
#else
    BM_THROW(BM_ERR_BADALLOC);
#endif
    }
    return ptr;
}

/// Aligned free
///
/// @internal
inline
void aligned_free(void* ptr) BMNOEXCEPT
{
    if (!ptr)
        return;
#ifdef BM_ALLOC_ALIGN
# ifdef _MSC_VER
    ::_aligned_free(ptr);
#else
    ::_mm_free(ptr);
# endif
#else
    ::free(ptr);
#endif
}




} // namespace bm


#endif
