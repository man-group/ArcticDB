#ifndef BMBUFFER__H__INCLUDED__
#define BMBUFFER__H__INCLUDED__
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

#include <stddef.h>
#include <type_traits>

#include "bmdef.h"
#include "bmutil.h"

namespace bm
{

/**
    Byte buffer pointer
    @internal
*/
class byte_buffer_ptr
{
public:
    byte_buffer_ptr() BMNOEXCEPT
        : byte_buf_(0), size_(0)
    {}
    
    /// construct byte buffer pointer
    ///
    byte_buffer_ptr(unsigned char* in_buf, size_t in_size) BMNOEXCEPT
        : byte_buf_(in_buf), size_(in_size)
    {}
    
    /// Set buffer pointer
    void set_buf(unsigned char* in_buf, size_t in_size) BMNOEXCEPT
    {
        byte_buf_ = in_buf; size_= in_size;
    }

    /// Get buffer size
    size_t size() const BMNOEXCEPT { return size_; }
    
    /// Get read access to buffer memory
    const unsigned char* buf() const BMNOEXCEPT { return byte_buf_; }

    /// Get write access to buffer memory
    unsigned char* data() BMNOEXCEPT { return byte_buf_; }

    /// const access to buffer memory
    const unsigned char* data() const BMNOEXCEPT { return byte_buf_; }

    bool operator==(const byte_buffer_ptr& lhs) const BMNOEXCEPT
        { return equal(lhs); }
    
    /// return true if content and size is the same
    bool equal(const byte_buffer_ptr& lhs) const BMNOEXCEPT
    {
        if (this == &lhs)
            return true;
        if (size_ != lhs.size_)
            return false;
        int cmp = ::memcmp(byte_buf_, lhs.byte_buf_, size_);
        return (cmp  == 0);
    }

protected:
    unsigned char* byte_buf_;     ///< byte buffer pointer to hold data
    size_t         size_;         ///< current buffer size
};

/**
    Byte buffer template, extention of byte_buffer_ptr memory management
    \internal
*/
template<typename BVAlloc>
class byte_buffer : public byte_buffer_ptr
{
public:
    typedef BVAlloc                                          bv_allocator_type;
    typedef typename bv_allocator_type::block_allocator_type allocator_type;
    typedef size_t                                           size_type;
    
public:
    byte_buffer() BMNOEXCEPT : capacity_(0), alloc_factor_(0)
    {}
    
    byte_buffer(size_t in_capacity)
        : capacity_(0), alloc_factor_(0)
    {
        allocate(in_capacity);
    }
    
    byte_buffer(const byte_buffer& lhs) 
        : byte_buffer_ptr(lhs) 
    {
        byte_buf_ = 0;
        size_ = capacity_ = alloc_factor_ = 0;
        if (lhs.byte_buf_)
            copy_from(lhs.byte_buf_, lhs.size_);
    }
    
#ifndef BM_NO_CXX11
    /// Move constructor
    byte_buffer(byte_buffer&& in_buf) BMNOEXCEPT
    {
        byte_buf_ = in_buf.byte_buf_;
        in_buf.byte_buf_ = 0;
        this->size_ = in_buf.size_;
        capacity_ = in_buf.capacity_;
        in_buf.size_ = in_buf.capacity_ = 0;
        alloc_factor_ = in_buf.alloc_factor_;
    }
    
    /// Move assignment operator
    byte_buffer& operator=(byte_buffer&& lhs) BMNOEXCEPT
    {
        move_from(lhs);
        return *this;
    }
#endif

    byte_buffer& operator=(const byte_buffer& lhs) 
    {
        if (this == &lhs)
            return *this;

        copy_from(lhs.buf(), lhs.size());
        return *this;
    }
    
    ~byte_buffer()
    {
        free_buffer();
    }
    
    /// swap content with another buffer
    void swap(byte_buffer& other) BMNOEXCEPT
    {
        if (this == &other)
            return;
        unsigned char* btmp = byte_buf_;
        byte_buf_ = other.byte_buf_;
        other.byte_buf_ = btmp;
        
        bm::xor_swap(this->size_, other.size_);
        bm::xor_swap(capacity_, other.capacity_);
        bm::xor_swap(alloc_factor_, other.alloc_factor_);
    }
    
    /// take/move content from another buffer
    void move_from(byte_buffer& other) BMNOEXCEPT
    {
        if (this == &other)
            return;
        free_buffer();
        this->byte_buf_ = other.byte_buf_; other.byte_buf_ = 0;
        this->size_ = other.size_;
        this->capacity_ = other.capacity_;
        this->alloc_factor_ = other.alloc_factor_;
    }


    /// Free underlying memory
    void release()
    {
        free_buffer();
        this->size_ = capacity_ = 0;
    }

    /// copy data from an external buffer
    ///
    void copy_from(const unsigned char* in_buf, size_t in_size)
    {
        if (in_size)
        {
            allocate(in_size);
            ::memcpy(byte_buf_, in_buf, in_size);
        }
        this->size_ = in_size;
    }
    
    
    /// Get buffer capacity
    size_t capacity() const BMNOEXCEPT { return capacity_; }

    /// adjust current size (buffer content preserved)
    unsigned char* resize(size_t new_size, bool copy_content = true)
    {
        if (new_size <= capacity_)
        {
            this->size_ = new_size;
            return data();
        }
        byte_buffer tmp_buffer(new_size); // temp with new capacity
        if (copy_content)
            tmp_buffer = *this;
        this->swap(tmp_buffer);
        this->size_ = new_size;
        return data();
    }

    /// adjust current size (no need to reallocate)
    void resize_no_check(size_t new_size) BMNOEXCEPT
    {
        BM_ASSERT(new_size < capacity_ || !new_size);
        this->size_ = new_size;
    }

    /// reserve new capacity (buffer content preserved)
    void reserve(size_t new_capacity)
    {
        if (new_capacity <= capacity_)
            return;
        if (!capacity_)
        {
            allocate(new_capacity);
            return;
        }
        
        byte_buffer tmp_buffer(new_capacity);
        tmp_buffer = *this;
        this->swap(tmp_buffer);
    }
    
    /// reserve new capacity (buffer content NOT preserved, size set to 0)
    void reinit(size_t new_capacity)
    {
        allocate(new_capacity);
        this->size_ = 0;
    }
    
    /// reserve new capacity (buffer content NOT preserved, size set to 0)
    /// @sa reinit
    void reallocate(size_t new_capacity)
    {
        reinit(new_capacity);
    }

    /// try to shrink the capacity to more optimal size
    void optimize()
    {
        if (!byte_buf_)
            return;
        size_t words = compute_words(size_);
        if (words < alloc_factor_) // possible to shrink
        {
            byte_buffer tmp_buffer(*this);
            this->swap(tmp_buffer);
        }
    }
    
    /// return memory consumtion
    size_t mem_usage() const BMNOEXCEPT
    {
        return sizeof(capacity_) + sizeof(alloc_factor_) +
               capacity();
    }

    /**
        Free mmemory
        @internal
     */
    void free_buffer()
    {
        if (byte_buf_)
        {
            allocator_type::deallocate((bm::word_t*)byte_buf_, alloc_factor_);
            this->byte_buf_ = 0;
        }
    }

private:
    /// Override from the base class
    void set_buf(unsigned char* buf, size_t size);

    /// compute number of words for the desired capacity
    static size_t compute_words(size_t capacity) BMNOEXCEPT
    {
        size_t words = (capacity / sizeof(bm::word_t))+1;
        return words;
    }
    
    void allocate(size_t new_capacity)
    {
        if (byte_buf_ && new_capacity <= capacity_)
            return;
        
        free_buffer();
    
        size_t words = compute_words(new_capacity);
        
        bm::word_t* p = allocator_type::allocate(words, 0);
        this->byte_buf_ = (unsigned char*) p;
        this->size_ = 0;
        alloc_factor_ = (unsigned)words;
        capacity_ = alloc_factor_ * sizeof(bm::word_t);
    }


private:
    size_t         capacity_;     ///< current capacity
    size_t         alloc_factor_; ///< number of blocks allocated for buffer
};

/**
    Simple heap allocated vector based on bvector allocator
    @internal
*/
template<typename Val, typename BVAlloc, bool trivial_type>
class heap_vector
{
public:
    typedef BVAlloc                                          bv_allocator_type;
    typedef bm::byte_buffer<bv_allocator_type>               buffer_type;
    typedef Val                                              value_type;
    typedef typename buffer_type::size_type                  size_type;

    heap_vector() BMNOEXCEPT : buffer_()
    {}

    heap_vector(const heap_vector<Val, BVAlloc, trivial_type>& hv)
        : buffer_()
    {
        size_type v_size = value_size();
        size_type new_size = hv.size();
        buffer_.resize(new_size * v_size);
        unsigned char* this_data = buffer_.data();
        for (size_type i = 0; i < new_size; ++i)
        {
            unsigned char *p = this_data + (i * v_size);
            new(p) value_type(hv[i]);
        }
    }

    heap_vector& operator=(const heap_vector<Val, BVAlloc, trivial_type>& hv)
    {
        if (this == &hv)
            return *this;
        resize(0);

        size_type v_size = value_size();
        size_type new_size = hv.size();
        buffer_.resize(new_size * v_size);
        unsigned char* this_data = buffer_.data();
        for (size_type i = 0; i < new_size; ++i)
        {
            unsigned char *p = this_data + (i * v_size);
            new(p) value_type(hv[i]);
        }
        return *this;
    }
   
    ~heap_vector()
    {
        if (!trivial_type)
        {
            size_type sz = size();
            size_type v_size = value_size();
            unsigned char* this_data = buffer_.data();
            for (size_type i = 0; i < sz; ++i)
            {
                unsigned char *p = this_data + (i * v_size);
                reinterpret_cast<value_type*>(p)->~Val();
            }
        }
    }
    
    value_type* data() const BMNOEXCEPT { return (value_type*) buffer_.data(); }

    void swap(heap_vector<Val, BVAlloc, trivial_type>& other) BMNOEXCEPT
    {
        buffer_.swap(other.buffer_);
    }

    const value_type& operator[](size_type pos) const BMNOEXCEPT
    {
        BM_ASSERT(pos < size());
        return reinterpret_cast<const value_type*>(buffer_.data())[pos];
    }

    value_type& operator[](size_type pos) BMNOEXCEPT
    {
        BM_ASSERT(pos < size());
        return reinterpret_cast<value_type*>(buffer_.data())[pos];
    }

    value_type& at(size_type pos)
    {
        size_type sz = size();
        if (pos >= sz)
            throw_range_error("out of range access");
        return reinterpret_cast<value_type*>(buffer_.data())[pos];
    }

    const value_type& at(size_type pos) const
    {
        size_type sz = size();
        if (pos >= sz)
            throw_range_error("out of range access");
        return reinterpret_cast<const value_type*>(buffer_.data())[pos];
    }

    const value_type* begin() const BMNOEXCEPT
    {
        return (const value_type*) buffer_.buf();
    }

    size_type size() const BMNOEXCEPT
    {
        return buffer_.size() / value_size();
    }

    size_type capacity() const BMNOEXCEPT
    {
        return buffer_.capacity() / value_size();
    }

    bool empty() const BMNOEXCEPT
    {
        return (buffer_.size() == 0);
    }

    void reserve(size_type new_size)
    {
        size_type sz = size();
        if (new_size <= sz)
            return;
        size_type v_size = value_size();
        buffer_.reserve(new_size * v_size);
    }

    /**
        Quick resize to zero
     */
    void reset() BMNOEXCEPT
    {
        if constexpr (trivial_type)
            buffer_.resize_no_check(0);
        else
            resize(0);
    }

    /**
        @brief vector resize
        @param new_size - new number of elements
        @param init_destroy_values - need to init or destroy values 
           false - skip construction/destruction
    */
    void resize(size_type new_size)
    {
        size_type sz = size();
        size_type v_size = value_size();
        if (new_size == sz)
            return;
        if (new_size < sz) // shrink
        {
            if constexpr (!trivial_type)
            {
                unsigned char* this_data = buffer_.data();
                for (size_type i = new_size; i < sz; ++i)
                {
                    unsigned char *p = this_data + (i * v_size);
                    reinterpret_cast<value_type*>(p)->~Val();
                }
            }
            buffer_.resize(new_size * v_size);
        }
        else
        {
            buffer_.resize(new_size * v_size);
            if constexpr (!trivial_type)
            {
                unsigned char* this_data = buffer_.data();
                for (size_type i = sz; i < new_size; ++i)
                {
                    unsigned char *p = this_data + (i * v_size);
                    new(p) value_type();
                }
            }
        }
    }

    /**
        @brief resize without content preservation
        @internal
     */
    value_type* resize_no_copy(size_type new_size)
    {
        size_type v_size = value_size();
        return (value_type*) buffer_.resize(new_size * v_size, false /*no copy content*/);
    }

    /**
        @brief resize without content preservation or capacity verification
        @internal
     */
    void resize_no_check(size_type new_size)
    {
        size_type v_size = value_size();
        buffer_.resize_no_check(new_size * v_size);
    }


    /**
        @brief Add element to the end of the vector, return reference
        @return reference the the last element
     */
    value_type& add()
    {
        size_type v_size = value_size();
        size_type sz = size();
        resize_internal(sz + 1);
        unsigned char *p = buffer_.data() + (sz * v_size);
        value_type* v = new(p) value_type();
        return *v;
    }

    /**
        @brief push new element to the back of the vector
     */
    void push_back(const value_type& v)
    {
        size_type v_size = value_size();
        size_type sz = size();
        resize_internal(sz + 1);
        unsigned char *p = buffer_.data() + (sz * v_size);
        new(p) value_type(v);
    }

    /**
        Push back value without capacity check
     */
    void push_back_no_check(const value_type& v) BMNOEXCEPT
    {
        BM_ASSERT(size() < capacity());
        size_type v_size = value_size();
        size_type sz = size();

        buffer_.resize_no_check((sz+1) * v_size);

        unsigned char *p = buffer_.data() + (sz * v_size);
        new(p) value_type(v);
    }


protected:

    void resize_internal(size_type new_size)
    {
        size_type cap = capacity();
        size_type v_size = value_size();
        if (cap <= new_size)    
            buffer_.reserve((new_size + 1024) * v_size);
        buffer_.resize(new_size * v_size);
    }

    static size_type value_size() BMNOEXCEPT
    {
        size_type size_of = sizeof(value_type);
        return size_of;
    }

    void throw_range_error(const char* err_msg) const
    {
    #ifndef BM_NO_STL
        throw std::range_error(err_msg);
    #else
        (void) err_msg;
        BM_ASSERT_THROW(false, BM_ERR_RANGE);
    #endif
    }

protected:
    buffer_type     buffer_;
};

/**
    Heap allocated scalar-type matrix

    This class can be seen as a matrix (row-column)
    access adaptor on top of the heap allocated buffer.

    @internal
*/
template<typename Val, size_t ROWS, size_t COLS, typename BVAlloc>
class heap_matrix
{
public:
    typedef BVAlloc                                          bv_allocator_type;
    typedef bm::byte_buffer<bv_allocator_type>               buffer_type;
    typedef Val                                              value_type;
    typedef size_t                                           size_type;

    enum params
    {
        n_rows = ROWS,
        n_columns = COLS,
        size_in_bytes = sizeof(value_type) * COLS * ROWS,
        row_size_in_bytes = sizeof(value_type) * COLS
    };

    static size_t rows() BMNOEXCEPT { return ROWS; }
    static size_t cols() BMNOEXCEPT { return COLS; }

    /**
        By default object is constructed NOT allocated.
    */
    heap_matrix() BMNOEXCEPT
        : buffer_()
    {}

    /**
        @param init_buf - when true - perform heap buffer allocation
    */
    heap_matrix(bool init_buf)
        : buffer_(init_buf ? size_in_bytes : 0)
    {
        if (init_buf)
            buffer_.resize(size_in_bytes);
    }

    void resize(size_type rows_in, size_type cols_in, bool copy_content = true)
    {
        BM_ASSERT(0); // heap matrix cannot resize
        (void) rows_in; (void) cols_in; (void) copy_content;
    }

    /**
        Post construction allocation, initialization
    */
    void init(bool set_z=false)
    {
        buffer_.resize(size_in_bytes);
        if (set_z)
            set_zero();
    }
    
    bool is_init() const BMNOEXCEPT
    {
        return buffer_.size();
    }

    value_type get(size_type row_idx, size_type col_idx) const BMNOEXCEPT
    {
        BM_ASSERT(row_idx < ROWS);
        BM_ASSERT(col_idx < COLS);
        BM_ASSERT(buffer_.size());
        const unsigned char* buf = buffer_.buf() + row_idx * row_size_in_bytes;
        return ((const value_type*)buf)[col_idx];
    }

    const value_type* row(size_type row_idx) const BMNOEXCEPT
    {
        BM_ASSERT(row_idx < ROWS);
        BM_ASSERT(buffer_.size());
        const unsigned char* buf = buffer_.buf() + row_idx * row_size_in_bytes;
        return (const value_type*) buf;
    }

    value_type* row(size_type row_idx) BMNOEXCEPT
    {
        BM_ASSERT(row_idx < ROWS);
        BM_ASSERT(buffer_.size());

        unsigned char* buf = buffer_.data() + row_idx * row_size_in_bytes;
        return (value_type*)buf;
    }

    /** memset all buffer to all zeroes */
    void set_zero() BMNOEXCEPT
    {
        ::memset(buffer_.data(), 0, size_in_bytes);
    }
    
    /*!  swap content
    */
    void swap(heap_matrix& other) BMNOEXCEPT
    {
        buffer_.swap(other.buffer_);
    }
    
    /*!  move content from another matrix
    */
    void move_from(heap_matrix& other) BMNOEXCEPT
    {
        buffer_.move_from(other.buffer_);
    }

    /** Get low-level buffer access */
    buffer_type& get_buffer() BMNOEXCEPT { return buffer_; }
    /** Get low-level buffer access */
    const buffer_type& get_buffer() const BMNOEXCEPT { return buffer_; }

protected:
    buffer_type     buffer_;
};



/**
    Heap allocated dynamic resizable scalar-type matrix

    @internal
*/
template<typename Val, typename BVAlloc>
class dynamic_heap_matrix
{
public:
    typedef BVAlloc                                          bv_allocator_type;
    typedef bm::byte_buffer<bv_allocator_type>               buffer_type;
    typedef Val                                              value_type;
    typedef size_t                                           size_type;

public:

    /**
        By default object is constructed but NOT allocated.
    */
    dynamic_heap_matrix(size_type rows_in=0, size_type cols_in=0) BMNOEXCEPT
        : rows_(rows_in), cols_(cols_in),
        buffer_()
    {}

    /**
        Post construction allocation, initialization
    */
    void init(bool set_z=false)
    {
        buffer_.resize(size_in_bytes());
        if (set_z && size_in_bytes())
            set_zero();
    }

    /**
        Free memory
     */
    void free() BMNOEXCEPT
    {
        buffer_.free_buffer();
    }

    size_type rows() const BMNOEXCEPT { return rows_; }
    size_type cols() const BMNOEXCEPT { return cols_; }

    void resize(size_type rows_in, size_type cols_in, bool copy_content = true)
    {
        if (copy_content && this->is_init())
        {
            dynamic_heap_matrix<value_type, bv_allocator_type>
                                                tmp_matrix(rows_in, cols_in);
            tmp_matrix.init(true/*set-zero*/);
            tmp_matrix.copy_from(*this);
            this->swap(tmp_matrix);
            return;
        }
        init_resize(rows_in, cols_in);
    }

    void init_resize(size_type rows_in, size_type cols_in)
    {
        rows_ = rows_in; cols_ = cols_in;
        buffer_.resize(size_in_bytes(), false); // no-copy resize
    }

    void copy_from(const dynamic_heap_matrix<value_type, bv_allocator_type>&
                   other) BMNOEXCEPT
    {
        size_type rsize = other.rows();
        size_type csize = other.cols();

        BM_ASSERT(rows_ >= rsize);
        BM_ASSERT(cols_ >= csize);

        for (size_type i = 0; i < rsize; ++i)
        {
            const value_type* other_row = other.row(i);
            value_type* r = this->row(i);
            for (size_type j = 0; j < csize; ++j)
                r[j] = other_row[j];
        } // for i
    }
    
    bool is_init() const BMNOEXCEPT
    {
        return buffer_.size();
    }

    const value_type* row(size_type row_idx) const BMNOEXCEPT
    {
        BM_ASSERT(row_idx < rows_);
        BM_ASSERT(buffer_.size());
        const unsigned char* buf = buffer_.buf() + row_idx * row_size_in_bytes();
        return (const value_type*) buf;
    }

    value_type* row(size_type row_idx) BMNOEXCEPT
    {
        BM_ASSERT(row_idx < rows_);
        BM_ASSERT(buffer_.size());

        unsigned char* buf = buffer_.data() + row_idx * row_size_in_bytes();
        return (value_type*)buf;
    }

    value_type get(size_type row_idx, size_type col_idx) BMNOEXCEPT
    {
        BM_ASSERT(row_idx < rows_);
        BM_ASSERT(col_idx < cols_);
        const value_type* r = row(row_idx);
        return r[col_idx];
    }

    const value_type& get(size_type row_idx, size_type col_idx) const BMNOEXCEPT
    {
        BM_ASSERT(row_idx < rows_);
        BM_ASSERT(col_idx < cols_);
        const value_type* r = row(row_idx);
        return r[col_idx];
    }

    void set(size_type row_idx, size_type col_idx, value_type v) BMNOEXCEPT
    {
        BM_ASSERT(row_idx < rows_);
        BM_ASSERT(col_idx < cols_);
        value_type* r = row(row_idx);
        r[col_idx] = v;
    }

    /** memset all buffer to all zeroes */
    void set_zero() BMNOEXCEPT
    {
        BM_ASSERT(buffer_.data() && size_in_bytes());
        ::memset(buffer_.data(), 0, size_in_bytes());
    }
    
    /*!  swap content
    */
    void swap(dynamic_heap_matrix& other) BMNOEXCEPT
    {
        bm::xor_swap(rows_, other.rows_);
        bm::xor_swap(cols_, other.cols_);
        buffer_.swap(other.buffer_);
    }
    
    /*!  move content from another matrix
    */
    void move_from(dynamic_heap_matrix& other) BMNOEXCEPT
    {
        rows_ = other.rows_;
        cols_ = other.cols_;
        buffer_.move_from(other.buffer_);
    }

    /** Get low-level buffer access */
    buffer_type& get_buffer() BMNOEXCEPT { return buffer_; }
    /** Get low-level buffer access */
    const buffer_type& get_buffer() const BMNOEXCEPT { return buffer_; }

    /**
        copy values of the left triangle elements to the right triangle
        (operation specific to matrices with symmetric distances)
     */
    void replicate_triange() BMNOEXCEPT
    {
        BM_ASSERT(rows_ == cols_);
        for (size_type i = 0; i < rows_; ++i)
        {
            for (size_type j = i+1; j < cols_; ++j)
            {
                set(i, j, get(j, i));
            }
        }
    }

    /**
        Check if two matrix objects matches on the content
    */
    bool equal(const dynamic_heap_matrix<Val, BVAlloc>& dhm) const BMNOEXCEPT
    {
        if (cols() != dhm.cols())
            return false;
        if (rows() != dhm.rows())
            return false;
        return equal_sz(dhm, rows(), cols());
    }

    /**
        Approximate equals on the rows-columns overlap area
    */
    bool equal_overlap(const dynamic_heap_matrix<Val, BVAlloc>& dhm) const BMNOEXCEPT
    {
        size_type c1 = cols();
        size_type c2 = dhm.cols();
        size_type dim_cols = c1 < c2 ? c1 : c2;
        size_type r1 = rows();
        size_type r2 = dhm.rows();
        size_type dim_rows = r1 < r2 ? r1 : r2;

        return equal_sz(dhm, dim_rows, dim_cols);

    }

    /*! remapping: vect[idx] = matrix[idx, vect[idx] ]
    */
    template<typename VECT_TYPE>
    void remap(VECT_TYPE* vect, size_type size) const BMNOEXCEPT
    {
        BM_ASSERT(size <= rows());
        for (size_type i = 0; i < size; ++i)
        {
            const value_type* this_row = row(i);//buf + i * row_size_in_bytes;
            VECT_TYPE v0 = vect[i];
            BM_ASSERT(size_type(v0) < cols());
            value_type remap_v = this_row[unsigned(v0)];
            vect[i] = VECT_TYPE(remap_v);
        } // for i
    }

    template<typename VECT_TYPE>
    void remapz(VECT_TYPE* vect) const BMNOEXCEPT
    {
        size_type rsize = rows();
        for (size_type i = 0; i < rsize; ++i)
        {
            const value_type* this_row = row(i);//buf + i * row_size_in_bytes;
            VECT_TYPE v0 = vect[i];
            if (!v0)
                break;
            BM_ASSERT(size_type(v0) < cols());
            value_type remap_v = this_row[unsigned(v0)];
            vect[i] = VECT_TYPE(remap_v);
        } // for i
    }



    /**
        Sum of row elements
     */
    template<typename ACC>
    void sum(ACC& acc, size_type row_idx) const BMNOEXCEPT
    {
        BM_ASSERT(row_idx < rows_);
        ACC s = 0;
        const value_type* r = row(row_idx);
        for (size_type j = 0; j < cols_; ++j)
            s += r[j];
        acc = s;
    }

protected:
    static
    size_type size_in_bytes(size_type rows, size_type cols) BMNOEXCEPT
        { return sizeof(value_type) * cols * rows; }
    size_type size_in_bytes() const BMNOEXCEPT
        { return sizeof(value_type) * cols_ * rows_; }
    size_type row_size_in_bytes() const BMNOEXCEPT
        { return sizeof(value_type) * cols_; }

    bool equal_sz(const dynamic_heap_matrix<Val, BVAlloc>& dhm,
                  size_type dim_rows, size_type dim_cols) const BMNOEXCEPT
    {
        for (size_type i = 0; i < dim_rows; ++i)
        {
            for (size_type j = i+1; j < dim_cols; ++j)
            {
                const value_type& v1 = get(i,j);
                const value_type& v2 = dhm.get(i, j);
                if (!(v1 == v2))
                    return false;
            } // j
        } // i
        return true;
    }


protected:
    size_type       rows_;
    size_type       cols_;
    buffer_type     buffer_;
};


/**
    Simple queue based on memory controlled dynamic vector.
    Intent: better control memory allocations and exceptions than STL variant

    Important assumption: value_type is simple and not throwing exceptions
    @internal
 */
template<typename Value, typename BVAlloc, bool trivial_type>
class simple_queue
{
public:
    typedef Value         value_type;
    typedef BVAlloc       bv_allocator_type;
    typedef
    bm::heap_vector<Value, bv_allocator_type, trivial_type>   heap_vector_type;
    typedef typename heap_vector_type::size_type size_type;

public:
    simple_queue() {}

    /**
        Capacity reservation
    */
    void reserve(size_type capacity)
    {
        queue_vector_.reserve(capacity);
    }

    size_type size() const BMNOEXCEPT
    {
        return queue_vector_.size();
    }

    /**
        Return true if queue has no elements
    */
    bool empty() const BMNOEXCEPT
    {
        return !queue_vector_.size();
    }

    /**
        Try push into the queue if capacity allows
    */
    bool try_push(const value_type& v) BMNOEXCEPT
    {
        size_type cap = queue_vector_.capacity();
        size_type sz = queue_vector_.size();
        if (cap <= sz)
            return false;
        queue_vector_.push_back_no_check(v);
        return true;
    }

    /**
        Push value into the queue (with re-allocation)
     */
    void push(const value_type& v)
    {
        queue_vector_.push_back(v);
    }

    /**
        Return front element
     */
    const value_type& front() const BMNOEXCEPT
    {
        BM_ASSERT(queue_vector_.size());
        return queue_vector_[front_idx_];
    }

    /**
        Pop the front element
     */
    void pop() BMNOEXCEPT
    {
        BM_ASSERT(front_idx_ < queue_vector_.size());
        ++front_idx_;
        if (front_idx_ == queue_vector_.size())
            queue_vector_.resize(front_idx_ = 0); // reset the queue
    }

    /**
        Try to extract the front element from the queue
     */
    bool try_pop(value_type& front_value) BMNOEXCEPT
    {
        size_type sz = queue_vector_.size();
        if (!sz)
            return false;
        front_value = queue_vector_[front_idx_];
        ++front_idx_;
        if (front_idx_ == sz)
            queue_vector_.resize(front_idx_ = 0); // reset the queue
        return true;
    }

private:
    simple_queue(const simple_queue&) = delete;
    simple_queue& operator=(const simple_queue&) = delete;

protected:
    heap_vector_type  queue_vector_;
    size_type         front_idx_ = 0; ///< index of the front element
};



} // namespace bm


#endif
