#ifndef BMSPARSEVEC_COMPR_H__INCLUDED__
#define BMSPARSEVEC_COMPR_H__INCLUDED__
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

/*! \file bmsparsevec_compr.h
    \brief Compressed sparse container rsc_sparse_vector<> for integer types
*/

#include <memory.h>

#ifndef BM_NO_STL
#include <stdexcept>
#endif

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration 
// #include "bm.h" or "bm64.h" explicitly 
# error missing include (bm.h or bm64.h)
#endif


#include "bmsparsevec.h"
#include "bmdef.h"

namespace bm
{


/*!
   \brief Rank-Select compressed sparse vector
 
   Container uses Rank-Select method of compression, where
   all NULL columns gets dropped, effective address of columns is computed
   using address bit-vector.

   Use for cases, where memory efficiency is preferable over
   single element access latency.
 
   \ingroup sv
*/
template<class Val, class SV>
class rsc_sparse_vector
{
public:
    enum bit_slices
    {
        sv_slices = (sizeof(Val) * 8 + 1),
        sv_value_slices = (sizeof(Val) * 8)
    };

    typedef Val                                      value_type;
    typedef const value_type&                        const_reference;
    typedef typename SV::size_type                   size_type;
    typedef SV                                       sparse_vector_type;
    typedef typename SV::const_iterator              sparse_vector_const_iterator;
    typedef typename SV::bvector_type                bvector_type;
    typedef bvector_type*                            bvector_type_ptr;
    typedef const bvector_type*                      bvector_type_const_ptr;
    typedef typename bvector_type::allocator_type    allocator_type;
    typedef typename bvector_type::allocation_policy allocation_policy_type;
    typedef typename bvector_type::rs_index_type     rs_index_type;
    typedef typename bvector_type::enumerator        bvector_enumerator_type;
    typedef typename SV::bmatrix_type                bmatrix_type;
    typedef typename SV::unsigned_value_type         unsigned_value_type;

    enum vector_capacity
    {
        max_vector_size = 1
    };

    struct is_remap_support { enum trait { value = false }; };
    struct is_rsc_support { enum trait { value = true }; };
    struct is_dynamic_splices { enum trait { value = false }; };

public:
    /*! Statistical information about  memory allocation details. */
    struct statistics : public bv_statistics
    {};
    
public:
    /**
         Reference class to access elements via common [] operator
    */
    class reference
    {
    public:
        reference(rsc_sparse_vector<Val, SV>& csv, size_type idx) BMNOEXCEPT
        : csv_(csv), idx_(idx)
        {}
        operator value_type() const BMNOEXCEPT { return csv_.get(idx_); }
        bool operator==(const reference& ref) const BMNOEXCEPT
                                { return bool(*this) == bool(ref); }
        bool is_null() const BMNOEXCEPT { return csv_.is_null(idx_); }
    private:
        rsc_sparse_vector<Val, SV>& csv_;
        size_type                   idx_;
    };

    /**
        Const iterator to traverse the rsc sparse vector.

        Implementation uses buffer for decoding so, competing changes
        to the original vector may not match the iterator returned values.

        This iterator keeps an operational buffer, memory footprint is not
        negligable

        @ingroup sv
    */
    class const_iterator
    {
    public:
    friend class rsc_sparse_vector;

#ifndef BM_NO_STL
        typedef std::input_iterator_tag  iterator_category;
#endif
        typedef rsc_sparse_vector<Val, SV>           rsc_sparse_vector_type;
        typedef rsc_sparse_vector_type*              rsc_sparse_vector_type_ptr;
        typedef typename rsc_sparse_vector_type::value_type    value_type;
        typedef typename rsc_sparse_vector_type::size_type     size_type;
        typedef typename rsc_sparse_vector_type::bvector_type  bvector_type;
        typedef typename bvector_type::allocator_type          allocator_type;
        typedef typename
        bvector_type::allocator_type::allocator_pool_type allocator_pool_type;
        typedef bm::byte_buffer<allocator_type>            buffer_type;

        typedef unsigned                    difference_type;
        typedef unsigned*                   pointer;
        typedef value_type&                 reference;

    public:
        const_iterator() BMNOEXCEPT;
        const_iterator(const rsc_sparse_vector_type* csv) BMNOEXCEPT;
        const_iterator(const rsc_sparse_vector_type* csv, size_type pos) BMNOEXCEPT;
        const_iterator(const const_iterator& it) BMNOEXCEPT;

        bool operator==(const const_iterator& it) const BMNOEXCEPT
                                { return (pos_ == it.pos_) && (csv_ == it.csv_); }
        bool operator!=(const const_iterator& it) const BMNOEXCEPT
                                { return ! operator==(it); }
        bool operator < (const const_iterator& it) const BMNOEXCEPT
                                { return pos_ < it.pos_; }
        bool operator <= (const const_iterator& it) const BMNOEXCEPT
                                { return pos_ <= it.pos_; }
        bool operator > (const const_iterator& it) const BMNOEXCEPT
                                { return pos_ > it.pos_; }
        bool operator >= (const const_iterator& it) const BMNOEXCEPT
                                { return pos_ >= it.pos_; }

        /// \brief Get current position (value)
        value_type operator*() const { return this->value(); }


        /// \brief Advance to the next available value
        const_iterator& operator++() BMNOEXCEPT { this->advance(); return *this; }

        /// \brief Advance to the next available value
        const_iterator& operator++(int)
            { const_iterator tmp(*this);this->advance(); return tmp; }


        /// \brief Get current position (value)
        value_type value() const;

        /// \brief Get NULL status
        bool is_null() const BMNOEXCEPT;

        /// Returns true if iterator is at a valid position
        bool valid() const BMNOEXCEPT { return pos_ != bm::id_max; }

        /// Invalidate current iterator
        void invalidate() BMNOEXCEPT { pos_ = bm::id_max; }

        /// Current position (index) in the vector
        size_type pos() const BMNOEXCEPT{ return pos_; }

        /// re-position to a specified position
        void go_to(size_type pos) BMNOEXCEPT;

        /// advance iterator forward by one
        /// @return true if it is still valid
        bool advance() BMNOEXCEPT;

        void skip_zero_values() BMNOEXCEPT;
    private:
        enum buf_size_e
        {
            n_buf_size = 1024 * 8
        };

    private:
        const rsc_sparse_vector_type*     csv_;     ///!< ptr to parent
        size_type                         pos_;     ///!< Position
        mutable buffer_type               vbuffer_; ///!< value buffer
        mutable buffer_type               tbuffer_; ///!< temp buffer
        mutable value_type*               buf_ptr_; ///!< position in the buffer
    };



    /**
        Back insert iterator implements buffered insert, faster than generic
        access assignment.
     
        Limitations for buffered inserter:
        1. Do not use more than one inserter per vector at a time
        2. Use method flush() at the end to send the rest of accumulated buffer
        flush is happening automatically on destruction, but if flush produces an
        exception (for whatever reason) it will be an exception in destructor.
        As such, explicit flush() is safer way to finilize the sparse vector load.

        @ingroup sv
    */
    class back_insert_iterator
    {
    public:
#ifndef BM_NO_STL
        typedef std::output_iterator_tag  iterator_category;
#endif
        typedef rsc_sparse_vector<Val, SV>       rsc_sparse_vector_type;
        typedef rsc_sparse_vector_type*          rsc_sparse_vector_type_ptr;
        typedef typename rsc_sparse_vector_type::value_type    value_type;
        typedef typename rsc_sparse_vector_type::size_type     size_type;
        typedef typename rsc_sparse_vector_type::bvector_type  bvector_type;

        typedef void difference_type;
        typedef void pointer;
        typedef void reference;
        
    public:

        /*! @name Construction and assignment  */
        ///@{

        back_insert_iterator() BMNOEXCEPT;
        back_insert_iterator(rsc_sparse_vector_type* csv) ;

        back_insert_iterator(const back_insert_iterator& bi);
        void operator=(const back_insert_iterator& bi)
        {
            BM_ASSERT(bi.empty());
            this->flush(); sv_bi_ = bi.sv_bi_;
        }

        ~back_insert_iterator();
        ///@}

        /** push value to the vector */
        back_insert_iterator& operator=(value_type v)
            { this->add(v); return *this; }
        /** noop */
        back_insert_iterator& operator*() { return *this; }
        /** noop */
        back_insert_iterator& operator++() { return *this; }
        /** noop */
        back_insert_iterator& operator++( int ) { return *this; }
        
        /** add value to the container*/
        void add(value_type v);
        
        /** add NULL (no-value) to the container */
        void add_null() BMNOEXCEPT;
        
        /** add a series of consequitve NULLs (no-value) to the container */
        void add_null(size_type count) BMNOEXCEPT;
        
        /** flush the accumulated buffer */
        void flush();
    protected:
    
        /** add value to the buffer without changing the NULL vector
            @param v - value to push back
            @return index of added value in the internal buffer
            @internal
        */
        ///size_type add_value(value_type v);
        
        typedef rsc_sparse_vector_type::sparse_vector_type sparse_vector_type;
        typedef
        typename sparse_vector_type::back_insert_iterator  sparse_vector_bi;
    private:
        rsc_sparse_vector_type* csv_;      ///!< pointer on the parent vector
        sparse_vector_bi        sv_bi_;
    };

public:
    // ------------------------------------------------------------
    /*! @name Construction and assignment  */

    //@{

    rsc_sparse_vector(bm::null_support null_able = bm::use_null,
                      allocation_policy_type ap = allocation_policy_type(),
                      size_type bv_max_size = bm::id_max,
                      const allocator_type&   alloc  = allocator_type());

    /**
        Contructor to pre-initialize the list of assigned (not NULL) elements.

        If the list of not NULL elements is known upfront it can help to
        pre-declare it, enable rank-select index and then use set function.
        This scenario gives significant speed boost, comparing random assignment

        @param bv_null - not NULL vector for the container
    */
    rsc_sparse_vector(const bvector_type& bv_null);

    ~rsc_sparse_vector();
    
    /*! copy-ctor */
    rsc_sparse_vector(const rsc_sparse_vector<Val, SV>& csv);
    
    
    /*! copy assignmment operator */
    rsc_sparse_vector<Val,SV>& operator=(const rsc_sparse_vector<Val, SV>& csv)
    {
        if (this != &csv)
        {
            sv_ = csv.sv_;
            size_ = csv.size_; max_id_ = csv.max_id_;
            in_sync_ = csv.in_sync_;
            if (in_sync_)
            {
                rs_idx_->copy_from(*(csv.rs_idx_));
            }
        }
        return *this;
    }

#ifndef BM_NO_CXX11
    /*! move-ctor */
    rsc_sparse_vector(rsc_sparse_vector<Val,SV>&& csv) BMNOEXCEPT;

    /*! move assignmment operator */
    rsc_sparse_vector<Val,SV>& operator=(rsc_sparse_vector<Val,SV>&& csv) BMNOEXCEPT
    {
        if (this != &csv)
        {
            clear_all(true);
            sv_.swap(csv.sv_);
            size_ = csv.size_; max_id_ = csv.max_id_; in_sync_ = csv.in_sync_;
            if (in_sync_)
                rs_idx_->copy_from(*(csv.rs_idx_));
        }
        return *this;
    }
#endif

    //@}
    // ------------------------------------------------------------
    /*! @name Size, etc                                          */
    ///@{

    /*! \brief return size of the vector
        \return size of sparse vector
    */
    size_type size() const BMNOEXCEPT;
    
    /*! \brief return true if vector is empty
        \return true if empty
    */
    bool empty() const BMNOEXCEPT { return sv_.empty(); }

    /**
        \brief recalculate size to exclude tail NULL elements
        After this call size() will return the true size of the vector
     */
    void sync_size() BMNOEXCEPT;

    /*! \brief change vector size
        \param new_size - new vector size
    */
    void resize(size_type new_size);

    /*
       \brief Returns count of not NULL elements (population)
              in the given range [left..right]
       Uses rank-select index to accelerate the search (after sync())

       \param left   - index of first bit start counting from
       \param right  - index of last bit

       @sa sync
    */
    size_type
    count_range_notnull(size_type left, size_type right) const BMNOEXCEPT;
    
    ///@}

    // ------------------------------------------------------------
    /*! @name Element access */
    //@{

    /*!
        \brief get specified element without bounds checking
        \param idx - element index
        \return value of the element
    */
    value_type operator[](size_type idx) const { return this->get(idx); }

    /*!
        \brief access specified element with bounds checking
        \param idx - element index
        \return value of the element
    */
    value_type at(size_type idx) const;
    
    /*!
        \brief get specified element without bounds checking
        \param idx - element index
        \return value of the element
    */
    value_type get(size_type idx) const BMNOEXCEPT;

    /**
        \brief get specified element with NOT NULL check
        \param idx - element index
        \param v -  [out] value to get
        \return true if value was aquired (NOT NULL), false otherwise
        @sa is_null, get
     */
    bool try_get(size_type idx, value_type& v) const BMNOEXCEPT;

    /**
        \brief get specified element with NOT NULL check
        Caller guarantees that the vector is in sync mode (RS-index access).
        \param idx - element index
        \param v -  [out] value to get
        \return true if value was aquired (NOT NULL), false otherwise
        @sa is_null, get, sync
     */
    bool try_get_sync(size_type idx, value_type& v) const BMNOEXCEPT;


    /*!
        \brief set specified element with bounds checking and automatic resize
     
        Method cannot insert elements, so every new idx has to be greater or equal
        than what it used before. Elements must be loaded in a sorted order.
     
        \param idx - element index
        \param v   - element value
    */
    void push_back(size_type idx, value_type v);


    /*!
        \brief add element with automatic resize
        \param v   - element value
    */
    void push_back(value_type v)
        { this->push_back(size_, v); }

    /*!
        \brief push back specified amount of NULL values
        \param count   - number of NULLs to push back
    */
    void push_back_null(size_type count);

    /*!
        \brief push back NULL value
    */
    void push_back_null() { push_back_null(1); }

    /*!
        \brief set specified element with bounds checking and automatic resize
        \param idx - element index
        \param v   - element value
    */
    void set(size_type idx, value_type v);
    

    /*!
        \brief increment specified element by one
        \param idx - element index
    */
    void inc(size_type idx);

    /*!
        \brief increment specified element by one
        \param idx - element index
        \param v - increment value
    */
    void inc(size_type idx, value_type v);

    /*!
        \brief increment specified element by one, element MUST be NOT NULL
        Faster than just inc() if element is NULL - behavior is undefined
        \param idx - element index
        \param v - increment value
        @sa inc
    */
    void inc_not_null(size_type idx, value_type v);

    /*!
        \brief set specified element to NULL
        RSC vector actually erases element when it is set to NULL (expensive).
        \param idx - element index
    */
    void set_null(size_type idx);

    /**
        Set NULL all elements set as 1 in the argument vector.
        Function also clears all the values to 0.
        Note: this can be a very expensive function for an RS container.
        \param bv_idx - index bit-vector for elements which  to be set to NULL
     */
    void set_null(const bvector_type& bv_idx);


    /**
        Set vector elements spcified by argument bit-vector to zero
        Note that set to 0 elements are NOT going to tuned to NULL (NULL qualifier is preserved)
        \param bv_idx - index bit-vector for elements which  to be set to 0
     */
    void clear(const bvector_type& bv_idx);



    /** \brief test if specified element is NULL
        \param idx - element index
        \return true if it is NULL false if it was assigned or container
        is not configured to support assignment flags
    */
    bool is_null(size_type idx) const BMNOEXCEPT;
    
    /**
        \brief Get bit-vector of assigned values (or NULL)
    */
    const bvector_type* get_null_bvector() const BMNOEXCEPT;

    /**
        \brief find position of compressed element by its rank
        \param rank - rank  (virtual index in sparse vector)
        \param idx  - index (true position)
    */
    bool find_rank(size_type rank, size_type& idx) const BMNOEXCEPT;

    //@}
    
    // ------------------------------------------------------------
    /*! @name Export content to C-stype array                    */
    ///@{

    /**
        \brief C-style decode
        \param arr - decode target array (must be properly sized)
        \param idx_from - start address to decode
        \param size - number of elements to decode
        \param zero_mem - flag if array needs to beset to zeros first

        @return actual decoded size
        @sa decode_buf
     */
    size_type decode(value_type* arr,
                     size_type   idx_from,
                     size_type   size,
                     bool        zero_mem = true) const;


    /**
        \brief C-style decode (variant with external memory)
         Analog of decode, but requires two arrays.
         Faster than decode in many cases.

        \param arr - decode target array (must be properly sized)
        \param arr_buf_tmp - decode temp bufer (must be same size of arr)
        \param idx_from - start address to decode
        \param size - number of elements to decode
        \param zero_mem - flag if array needs to beset to zeros first

        @return actual decoded size
        @sa decode
     */
    size_type decode_buf(value_type* arr,
                         value_type* arr_buf_tmp,
                         size_type   idx_from,
                         size_type   size,
                         bool        zero_mem = true) const BMNOEXCEPT;

    /*!
        \brief Gather elements to a C-style array

        Gather collects values from different locations, for best
        performance feed it with sorted list of indexes.

        Faster than one-by-one random access.

        For efficiency, this is left as a low level function,
        it does not do any bounds checking on the target array, it will
        override memory and crash if you are not careful with allocation
        and request size.

        \param arr  - destination array
        \param idx - index list to gather elements (read only)
        \param idx_tmp_buf - temp scratch buffer for index rank-select index translation
                must be correctly allocated to match size. No value initialization requirement.
        \param size - decoding index list size (array allocation should match)
        \param sorted_idx - sort order directive for the idx array
                          (BM_UNSORTED, BM_SORTED, BM_UNKNOWN)
        Sort order affects both performance and correctness(!), use BM_UNKNOWN
        if not sure.

        \return number of actually exported elements (can be less than requested)

        \sa decode
    */
    size_type gather(value_type* arr,
                     const size_type* idx,
                     size_type*  idx_tmp_buf,
                     size_type   size,
                     bm::sort_order sorted_idx) const;


    ///@}

    
    // ------------------------------------------------------------
    /*! @name Various traits                                     */
    ///@{
    /**
        \brief if container supports NULL(unassigned) values (true)
    */
    bool is_nullable() const { return true; }
    
    /** \brief various type traits
    */
    static constexpr
    bool is_compressed() BMNOEXCEPT { return true; }

    static constexpr
    bool is_str() BMNOEXCEPT { return false; }

    ///@}

    
    // ------------------------------------------------------------
    /*! @name Comparison */
    ///@{

    /*!
        \brief check if another vector has the same content
        \return true, if it is the same
    */
    bool equal(const rsc_sparse_vector<Val, SV>& csv) const BMNOEXCEPT;
    //@}


    // ------------------------------------------------------------
    /*! @name Load-Export compressed vector with data */    
    ///@{
    /*!
        \brief Load compressed vector from a sparse vector (with NULLs)
        \param sv_src - source sparse vector
    */
    void load_from(const sparse_vector_type& sv_src);
    
    /*!
        \brief Exort compressed vector to a sparse vector (with NULLs)
        \param sv - target sparse vector
    */
    void load_to(sparse_vector_type& sv) const;
    
    ///@}
    
    // ------------------------------------------------------------
    /*! @name Iterator access */
    ///@{

    /** Provide const iterator access to container content  */
    const_iterator begin() const
    {
        if (!in_sync_)
            throw_no_rsc_index(); // call sync() to build RSC fast access index
        return const_iterator(this);
    }
        
    /** Provide const iterator access to the end    */
    const_iterator end() const BMNOEXCEPT
        { return const_iterator(this, bm::id_max); }

    /** Get const_itertor re-positioned to specific element
    @param idx - position in the sparse vector
    */
    const_iterator get_const_iterator(size_type idx) const BMNOEXCEPT
        { return const_iterator(this, idx); }

    back_insert_iterator get_back_inserter() { return back_insert_iterator(this); }
    ///@}

    // ------------------------------------------------------------
    /*! @name Memory optimization                                */
    ///@{

    /*!
        \brief run memory optimization for all vector slices
        \param temp_block - pre-allocated memory block to avoid unnecessary re-allocs
        \param opt_mode - requested compression depth
        \param stat - memory allocation statistics after optimization
    */
    void optimize(
        bm::word_t* temp_block = 0,
        typename bvector_type::optmode opt_mode = bvector_type::opt_compress,
        statistics* stat = 0);
    
    /*! \brief resize to zero, free memory
        @param free_mem - free bit vector slices if true
    */
    void clear_all(bool free_mem) BMNOEXCEPT;

    /*! \brief resize to zero, free memory
        @param free_mem - free bit vector slices if true
    */
    void clear() BMNOEXCEPT { clear_all(true); }

    /*!
        @brief Calculates memory statistics.

        Function fills statistics structure containing information about how
        this vector uses memory and estimation of max. amount of memory
        bvector needs to serialize itself.

        @param st - pointer on statistics structure to be filled in.

        @sa statistics
    */
    void calc_stat(
           struct rsc_sparse_vector<Val, SV>::statistics* st) const BMNOEXCEPT;

    /**
    @brief Turn sparse vector into immutable mode
    Read-only (immutable) vector uses less memory and allows faster searches.
    Before freezing it is recommenede to call optimize() to get full memory saving effect
    @sa optimize
     */
    void freeze() { sv_.freeze(); }

    /** Returns true if vector is read-only */
    bool is_ro() const BMNOEXCEPT { return sv_.is_ro_; }



    ///@}

    // ------------------------------------------------------------
    /*! @name Merge, split, partition data                        */
    ///@{

    /**
        @brief copy range of values from another sparse vector

        Copy [left..right] values from the source vector,
        clear everything outside the range.

        \param csv   - source vector
        \param left  - index from in losed diapason of [left..right]
        \param right - index to in losed diapason of [left..right]
    */
    void copy_range(const rsc_sparse_vector<Val, SV>& csv,
        size_type left, size_type right);

    /**
        @brief merge two vectors (argument gets destroyed)
        It is important that both vectors have the same NULL vectors
        @param csv - [in,out] argumnet vector to merge
                     (works like move so arg should not be used after the merge)
     */
    void merge_not_null(rsc_sparse_vector<Val, SV>& csv);

    ///@}

    // ------------------------------------------------------------
    /*! @name Fast access structues sync                         */
    ///@{
    /*!
        \brief Re-calculate rank-select index for faster access to vector
        \param force - force recalculation even if it is already recalculated
    */
    void sync(bool force);

    /*!
        \brief Re-calculate prefix sum table used for rank search (if necessary)
    */
    void sync() { sync(false); }

    /*!
        \brief returns true if prefix sum table is in sync with the vector
    */
    bool in_sync() const BMNOEXCEPT { return in_sync_; }
    /*!
        \brief returns true if prefix sum table is in sync with the vector
    */
    bool is_sync() const BMNOEXCEPT { return in_sync_; }

    /*!
        \brief Unsync the prefix sum table
    */
    void unsync() BMNOEXCEPT { in_sync_ = is_dense_ = false; }
    ///@}

    // ------------------------------------------------------------
    /*! @name Access to internals                                */
    ///@{

    /*!
        \brief get access to bit-plane, function checks and creates a plane
        \return bit-vector for the bit slice
    */
    bvector_type_const_ptr get_slice(unsigned i) const BMNOEXCEPT
        { return sv_.get_slice(i); }

    bvector_type_ptr get_create_slice(unsigned i)
        { return sv_.get_create_slice(i); }
    
    /*!
        Number of effective bit-slices in the value type
    */
    unsigned effective_slices() const BMNOEXCEPT
        { return sv_.effective_slices(); }
    
    /*!
        \brief get total number of bit-slices in the vector
    */
    static unsigned slices() BMNOEXCEPT
        { return sparse_vector_type::slices(); }

    /** Number of stored bit-slices (value slices + extra */
    static unsigned stored_slices()
        { return sparse_vector_type::stored_slices(); }

    /*!
        \brief access dense vector
    */
    const sparse_vector_type& get_sv() const BMNOEXCEPT { return sv_; }

    /*!
        \brief size of internal dense vector
    */
    size_type effective_size() const BMNOEXCEPT { return sv_.size(); }

    /**
        \brief Always 1 (non-matrix type)
    */
    size_type effective_vector_max() const BMNOEXCEPT { return 1; }

    /*!
        get read-only access to inetrnal bit-matrix
    */
    const bmatrix_type& get_bmatrix() const BMNOEXCEPT
        { return sv_.get_bmatrix(); }

    /*!
        get read-only access to inetrnal bit-matrix
    */
    bmatrix_type& get_bmatrix() BMNOEXCEPT
        { return sv_.get_bmatrix(); }


    /*! Get Rank-Select index pointer
        @return NULL if sync() was not called to construct the index
        @sa sync()
    */
    const rs_index_type* get_RS() const BMNOEXCEPT
        { return in_sync_ ? rs_idx_ : 0; }

    void mark_null_idx(unsigned null_idx) BMNOEXCEPT
        { sv_.mark_null_idx(null_idx); }

    /*!
        \brief Resolve logical address to access via rank compressed address

        \param idx    - input id to resolve
        \param idx_to - output id

        \return true if id is known and resolved successfully
         @internal
    */
    bool resolve(size_type idx, size_type* idx_to) const BMNOEXCEPT;

    ///@}
    
protected:
    enum octet_slices
    {
        sv_octet_slices = sizeof(value_type)
    };


    bool resolve_sync(size_type idx, size_type* idx_to) const BMNOEXCEPT;

    bool resolve_range(size_type from, size_type to, 
                       size_type* idx_from, size_type* idx_to) const BMNOEXCEPT;
    
    void resize_internal(size_type sz) 
    { 
        sv_.resize_internal(sz); 
    }
    size_type size_internal() const BMNOEXCEPT { return sv_.size(); }

    constexpr bool is_remap() const BMNOEXCEPT { return false; }
    size_t remap_size() const BMNOEXCEPT { return 0; }
    const unsigned char* get_remap_buffer() const BMNOEXCEPT { return 0; }
    unsigned char* init_remap_buffer() BMNOEXCEPT { return 0; }
    void set_remap() BMNOEXCEPT { }

    /// unused remap matrix type for compatibility with the sparse serializer
    typedef
    bm::heap_matrix<unsigned char, 1, 1,
                    typename bvector_type::allocator_type> remap_matrix_type;

    const remap_matrix_type* get_remap_matrix() const { return 0; }
    remap_matrix_type* get_remap_matrix() { return 0; }
    
    void push_back_no_check(size_type idx, value_type v);

    /**
        Convert signed value type to unsigned representation
     */
    static
    unsigned_value_type s2u(value_type v) BMNOEXCEPT
        { return  sparse_vector_type::s2u(v); }
    static
    value_type u2s(unsigned_value_type v) BMNOEXCEPT
        { return  sparse_vector_type::u2s(v); }

private:

    /// Allocate memory for RS index
    void construct_rs_index();
    /// Free rs-index
    void free_rs_index();

    /**
        \brief throw error that RSC index not constructed (call sync())
        \internal
        @sa sync
    */
    static
    void throw_no_rsc_index();

protected:
    template<class SVect, unsigned S_FACTOR> friend class sparse_vector_scanner;
    template<class SVect> friend class sparse_vector_serializer;
    template<class SVect> friend class sparse_vector_deserializer;


private:
    sparse_vector_type            sv_;       ///< transpose-sparse vector for "dense" packing
    size_type                     size_;     ///< vector size (logical)
    size_type                     max_id_;   ///< control variable for sorted load
    bool                          in_sync_;  ///< flag if prefix sum is in-sync with vector
    rs_index_type*                rs_idx_ = 0; ///< prefix sum for rank-select translation
    bool                          is_dense_ = false; ///< flag if vector is dense
};

//---------------------------------------------------------------------
//
//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::rsc_sparse_vector(bm::null_support null_able,
                                              allocation_policy_type ap,
                                              size_type bv_max_size,
                                              const allocator_type&   alloc)
: sv_(bm::use_null, ap, bv_max_size, alloc), in_sync_(false)
{
    (void) null_able;
    BM_ASSERT(null_able == bm::use_null);
    BM_ASSERT(int(sv_value_slices) == int(SV::sv_value_slices));
    size_ = max_id_ = 0;
    construct_rs_index();
}

//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::rsc_sparse_vector(const bvector_type& bv_null)
: sv_(bm::use_null), in_sync_(false)
{
    construct_rs_index();
    bvector_type* bv = sv_.get_null_bvect();
    BM_ASSERT(bv);
    *bv = bv_null;

    bool found = bv->find_reverse(max_id_);
    if (found)
    {
        size_ = max_id_ + 1;
        size_type sz = bv->count();
        sv_.resize(sz);
    }
    else
    {
        BM_ASSERT(!bv->any());
        size_ = max_id_ = 0;
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::~rsc_sparse_vector()
{
    free_rs_index();
}

//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::rsc_sparse_vector(
                          const rsc_sparse_vector<Val, SV>& csv)
: sv_(csv.sv_), size_(csv.size_), max_id_(csv.max_id_), in_sync_(csv.in_sync_)
{
    BM_ASSERT(int(sv_value_slices) == int(SV::sv_value_slices));
    
    construct_rs_index();
    if (in_sync_)
        rs_idx_->copy_from(*(csv.rs_idx_));
}

//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::rsc_sparse_vector(
                            rsc_sparse_vector<Val,SV>&& csv) BMNOEXCEPT
: sv_(bm::use_null),
  size_(0),
  max_id_(0), in_sync_(false)
{
    if (this != &csv)
    {
        sv_.swap(csv.sv_);
        size_ = csv.size_; max_id_ = csv.max_id_; in_sync_ = csv.in_sync_;
        rs_idx_ = csv.rs_idx_; csv.rs_idx_ = 0;
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
typename rsc_sparse_vector<Val, SV>::size_type
rsc_sparse_vector<Val, SV>::size() const BMNOEXCEPT
{
    return size_;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::resize(size_type new_size)
{
    BM_ASSERT(new_size < bm::id_max);
    if (!new_size) // clear memory
    {
        sv_.resize(0);
        BM_ASSERT(sv_.get_null_bvect()->none());
        in_sync_ = false;
        size_ = max_id_ = 0;
        return;
    }
    if (new_size >= size_) // vector grows
    {
        size_ = new_size;
        max_id_ = new_size - 1;
        return;
    }

    // vector shrinks
    // compute tail rank
    bvector_type* bv_null = sv_.get_null_bvect();
    size_type clear_size = bv_null->count_range(new_size, bm::id_max-1);

    if (!clear_size) // tail set/rank is empty
    {
        size_ = new_size;
        max_id_ = new_size - 1;
        BM_ASSERT(!bv_null->any_range(size_, bm::id_max-1));
        return;
    }

    BM_ASSERT(sv_.size() >= clear_size);
    size_type new_sv_size = sv_.size() - clear_size;
    sv_.resize_internal(new_sv_size, false); // without touching NULL plane
    bv_null->clear_range(new_size, bm::id_max-1);

    size_ = new_size;
    max_id_ = new_size - 1;

    BM_ASSERT(!bv_null->any_range(size_, bm::id_max-1));

    in_sync_ = false;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::push_back(size_type idx, value_type v)
{
    if (sv_.empty())
    {}
    else
    if (idx <= max_id_ && size_)
    {
        sv_.throw_range_error("compressed sparse vector push_back() range error");
    }
    push_back_no_check(idx, v);
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::push_back_null(size_type count)
{
    BM_ASSERT(size_ < bm::id_max - count); // overflow assert
    size_ += count;
    max_id_ = size_-1;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::push_back_no_check(size_type idx, value_type v)
{
    bvector_type* bv_null = sv_.get_null_bvect();
    BM_ASSERT(bv_null);
    
    bv_null->set_bit_no_check(idx);
    sv_.push_back_no_null(v);
    
    max_id_ = idx;
    size_ = idx + 1;
    in_sync_ = false;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::set_null(size_type idx)
{
    bvector_type* bv_null = sv_.get_null_bvect();
    BM_ASSERT(bv_null);
    
    bool found = bv_null->test(idx); // TODO: use extract bit
    if (found)
    {
        // TODO: maybe RS-index is available
        size_type sv_idx = bv_null->count_range(0, idx);
        bv_null->clear_bit_no_check(idx);
        sv_.erase(--sv_idx, false/*not delete NULL vector*/);
        in_sync_ = false;
    }
    else
    {
        if (idx > max_id_)
        {
            max_id_ = idx;
            size_ = max_id_ + 1;
        }
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::set_null(const bvector_type& bv_idx)
{
    bvector_type* bv_null = sv_.get_null_bvect();
    bvector_type bv_sub; // subtraction vector cleared from NOT NULLs
    bv_sub.bit_and(bv_idx, *bv_null);
    // clear the main matrix to accelerate the erase in all bit-slices
    {
        bm::rank_compressor<bvector_type> rank_compr;
        bvector_type bv_sub_rsc;
        rank_compr.compress(bv_sub_rsc, *bv_null, bv_sub);
        sv_.clear(bv_sub_rsc);
    }

    in_sync_ = false;
    typename bvector_type::enumerator en(&bv_sub, 0);
    for (size_type cnt = 0; en.valid(); ++en, ++cnt)
    {
        auto idx = *en;

        size_type sv_idx = bv_null->count_range(0, idx);
        sv_idx -= cnt; // correct rank for what we deleted previously
        sv_.erase(--sv_idx, false/*not delete the NULL vector*/);
    }
    bv_null->bit_sub(bv_sub);
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::clear(const bvector_type& bv_idx)
{
    const bvector_type* bv_null = sv_.get_null_bvector();

    bvector_type bv_sub; // subtraction vector cleared from NOT NULLs
    bv_sub.bit_and(bv_idx, *bv_null);

    bm::rank_compressor<bvector_type> rank_compr;
    bvector_type bv_sub_rsc;
    rank_compr.compress(bv_sub_rsc, *bv_null, bv_sub);

    sv_.clear(bv_sub_rsc);
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::inc(size_type idx)
{
    bvector_type* bv_null = sv_.get_null_bvect();
    BM_ASSERT(bv_null);

    size_type sv_idx;
    bool found = bv_null->test(idx);

    sv_idx = in_sync_ ? bv_null->count_to(idx, *rs_idx_)
                      : bv_null->count_range(0, idx); // TODO: make test'n'count

    if (found)
    {
        sv_.inc_no_null(--sv_idx);
    }
    else
    {
        sv_.insert_value_no_null(sv_idx, 1);
        bv_null->set_bit_no_check(idx);

        if (idx > max_id_)
        {
            max_id_ = idx;
            size_ = max_id_ + 1;
        }
        in_sync_ = false;
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::inc(size_type idx, value_type v)
{
    bvector_type* bv_null = sv_.get_null_bvect();
    BM_ASSERT(bv_null);

    size_type sv_idx;
    bool found = bv_null->test(idx);

    sv_idx = in_sync_ ? bv_null->count_to(idx, *rs_idx_)
                      : bv_null->count_range(0, idx); // TODO: make test'n'count

    if (found)
    {
        sv_.inc_no_null(--sv_idx, v);
    }
    else
    {
        sv_.insert_value_no_null(sv_idx, v);
        bv_null->set_bit_no_check(idx);

        if (idx > max_id_)
        {
            max_id_ = idx;
            size_ = max_id_ + 1;
        }
        in_sync_ = false;
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::inc_not_null(size_type idx, value_type v)
{
    bvector_type* bv_null = sv_.get_null_bvect();
    BM_ASSERT(bv_null->test(idx)); // idx must be NOT NULL

    size_type sv_idx;
    sv_idx = in_sync_ ? bv_null->count_to(idx, *rs_idx_)
                      : bv_null->count_range(0, idx); // TODO: make test'n'count
    --sv_idx;
    if (v == 1)
        sv_.inc_no_null(sv_idx);
    else
        sv_.inc_no_null(sv_idx, v);
}


//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::set(size_type idx, value_type v)
{
    bvector_type* bv_null = sv_.get_null_bvect();
    BM_ASSERT(bv_null);

    size_type sv_idx;
    bool found = bv_null->test(idx);

    sv_idx = in_sync_ ? bv_null->count_to(idx, *rs_idx_)
                      : bv_null->count_range(0, idx); // TODO: make test'n'count

    if (found)
    {
        sv_.set_value_no_null(--sv_idx, v, true);
    }
    else
    {
        sv_.insert_value_no_null(sv_idx, v);
        bv_null->set_bit_no_check(idx);

        if (idx > max_id_)
        {
            max_id_ = idx;
            size_ = max_id_ + 1;
        }
        in_sync_ = false;
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::equal(
                    const rsc_sparse_vector<Val, SV>& csv) const BMNOEXCEPT
{
    if (this == &csv)
        return true;
    if (max_id_ != csv.max_id_ || size_ != csv.size_)
        return false;
    bool same_sv = sv_.equal(csv.sv_);
    return same_sv;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::load_from(
                                        const sparse_vector_type& sv_src)
{
    max_id_ = size_ = 0;

    bvector_type* bv_null = sv_.get_null_bvect();
    BM_ASSERT(bv_null);

    const bvector_type* bv_null_src = sv_src.get_null_bvector();
    if (!bv_null_src) // dense vector (no NULL columns)
    {
        sv_ = sv_src;
        BM_ASSERT(sv_.get_null_bvect());
    }
    else
    {
        sv_.clear_all(true);
        *bv_null = *bv_null_src;
        
        bm::rank_compressor<bvector_type> rank_compr; // re-used for planes
        
        unsigned src_planes = sv_src.slices();
        for (unsigned i = 0; i < src_planes; ++i)
        {
            const bvector_type* bv_src_plane = sv_src.get_slice(i);
            if (bv_src_plane)
            {
                bvector_type* bv_plane = sv_.get_create_slice(i);
                rank_compr.compress(*bv_plane, *bv_null, *bv_src_plane);
            }
        } // for
        size_type count = bv_null->count(); // set correct sizes
        sv_.resize(count);
    }
    
    sync(true);
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::load_to(sparse_vector_type& sv) const
{
    sv.clear_all(true);
    
    const bvector_type* bv_null_src = this->get_null_bvector();
    if (!bv_null_src)
    {
        BM_ASSERT(bv_null_src);
        return;
    }
    
    bvector_type* bv_null = sv.get_null_bvect();
    BM_ASSERT(bv_null);
    *bv_null = *bv_null_src;
    
    bm::rank_compressor<bvector_type> rank_compr; // re-used for planes

    unsigned src_planes = sv_.slices();
    for (unsigned i = 0; i < src_planes; ++i)
    {
        if (const bvector_type* bv_src_plane = sv_.get_slice(i))
        {
            bvector_type* bv_plane = sv.get_create_slice(i);
            rank_compr.decompress(*bv_plane, *bv_null, *bv_src_plane);
        }
    } // for i
    sv.resize(this->size());
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::sync(bool force)
{
    if (in_sync_ && force == false)
        return;  // nothing to do
    const bvector_type* bv_null = sv_.get_null_bvector();
    BM_ASSERT(bv_null);
    bv_null->build_rs_index(rs_idx_); // compute popcount prefix list
    sv_.is_ro_ = bv_null->is_ro();

    if (force)
        sync_size();

    size_type cnt = size_ ? bv_null->count_range(0, size_-1, *rs_idx_)
                          : 0;
    is_dense_ = (cnt == size_); // dense vector?
    BM_ASSERT(size_ >= bv_null->count());

    in_sync_ = true;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::sync_size() BMNOEXCEPT
{
    const bvector_type* bv_null = sv_.get_null_bvector();
    BM_ASSERT(bv_null);
    // sync the max-id
    bool found = bv_null->find_reverse(max_id_);
    if (!found)
        max_id_ = size_ = 0;
    else
        size_ = max_id_ + 1;
    sync(false);
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::resolve(size_type idx,
                                         size_type* idx_to) const BMNOEXCEPT
{
    BM_ASSERT(idx_to);
    const bvector_type* bv_null = sv_.get_null_bvector();
    if (idx >= size_)
    {
        BM_ASSERT(bv_null->get_bit(idx) == false);
        return false;
    }
    if (in_sync_)
        return resolve_sync(idx, idx_to);

    // not in-sync: slow access
    bool found = bv_null->test(idx);
    if (!found)
        return found;
    *idx_to = bv_null->count_range_no_check(0, idx);
    return found;
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::resolve_sync(
                                    size_type  idx,
                                    size_type* idx_to) const BMNOEXCEPT
{
    BM_ASSERT(idx_to);
    BM_ASSERT(in_sync_);

    const bvector_type* bv_null = sv_.get_null_bvector();
    if (is_dense_)
    {
        *idx_to = idx+1;
        if (idx <= size_)
            return true;
        *idx_to = bm::id_max;
        BM_ASSERT(bv_null->get_bit(idx) == false);
        return false;
    }
    *idx_to = bv_null->count_to_test(idx, *rs_idx_);
    return bool(*idx_to);
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::resolve_range(
    size_type from, size_type to,
    size_type* idx_from, size_type* idx_to) const BMNOEXCEPT
{
    BM_ASSERT(idx_to && idx_from);
    const bvector_type* bv_null = sv_.get_null_bvector();
    size_type copy_sz, sv_left;
    if (in_sync_)
        copy_sz = bv_null->count_range(from, to, *rs_idx_);
    else  // slow access
        copy_sz = bv_null->count_range(from, to);
    if (!copy_sz)
        return false;

    if (in_sync_)
        sv_left = bv_null->rank_corrected(from, *rs_idx_);
    else
    {
        sv_left = bv_null->count_range(0, from);
        bool tl = bv_null->test(from); // TODO: add count and test
        sv_left -= tl; // rank correction
    }

    *idx_from = sv_left; *idx_to = sv_left + copy_sz - 1;
    return true;
}


//---------------------------------------------------------------------

template<class Val, class SV>
typename rsc_sparse_vector<Val, SV>::value_type
rsc_sparse_vector<Val, SV>::at(size_type idx) const
{
    size_type sv_idx;
    if (idx >= size())
        sv_.throw_range_error("compressed collection access error");

    bool found = resolve(idx, &sv_idx);
    if (!found)
    {
        sv_.throw_range_error("compressed collection item not found");
    }
    return sv_.at(--sv_idx);
}

//---------------------------------------------------------------------

template<class Val, class SV>
typename rsc_sparse_vector<Val, SV>::value_type
rsc_sparse_vector<Val, SV>::get(size_type idx) const BMNOEXCEPT
{
    size_type sv_idx;
    bool found = resolve(idx, &sv_idx);
    if (!found)
        return value_type(0);
    BM_ASSERT(!is_null(idx));
    return sv_.get(--sv_idx);
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::try_get(
                        size_type idx, value_type& v) const BMNOEXCEPT
{
    size_type sv_idx;
    if (!resolve(idx, &sv_idx))
        return false;
    v = sv_.get(--sv_idx);
    return true;
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::try_get_sync(
                        size_type idx, value_type& v) const BMNOEXCEPT
{
    size_type sv_idx;
    bool found = resolve_sync(idx, &sv_idx);
    if (!found)
        return found;
    v = sv_.get(--sv_idx);
    return true;
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::is_null(size_type idx) const BMNOEXCEPT
{
    const bvector_type* bv_null = sv_.get_null_bvector();
    BM_ASSERT(bv_null);
    return !(bv_null->test(idx));
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::optimize(bm::word_t*  temp_block,
                    typename bvector_type::optmode opt_mode,
                    statistics* st)
{
    sv_.optimize(temp_block, opt_mode, (typename sparse_vector_type::statistics*)st);
    if (st)
    {
        st->memory_used += sizeof(rs_index_type);
        st->memory_used += rs_idx_->get_total() *
             (sizeof(typename rs_index_type::size_type)
             + sizeof(typename rs_index_type::sb_pair_type));
    }
    if (is_sync()) // must rebuild the sync index after optimization
        this->sync(true);
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::clear_all(bool free_mem) BMNOEXCEPT
{
    sv_.clear_all(free_mem);
    in_sync_ = false;  max_id_ = size_ = 0;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::calc_stat(
            struct rsc_sparse_vector<Val, SV>::statistics* st) const BMNOEXCEPT
{
    BM_ASSERT(st);
    sv_.calc_stat((typename sparse_vector_type::statistics*)st);
    if (st)
    {
        st->memory_used += sizeof(rs_index_type);
        st->memory_used += rs_idx_->get_total() *
                   (sizeof(typename rs_index_type::size_type)
                   + sizeof(typename rs_index_type::sb_pair_type));
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
const typename rsc_sparse_vector<Val, SV>::bvector_type*
rsc_sparse_vector<Val, SV>::get_null_bvector() const BMNOEXCEPT
{
    return sv_.get_null_bvector();
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool
rsc_sparse_vector<Val, SV>::find_rank(size_type rank,
                                      size_type& idx) const BMNOEXCEPT
{
    BM_ASSERT(rank);
    bool b;
    const bvector_type* bv_null = get_null_bvector();
    if (in_sync())
        b = bv_null->select(rank, idx, *rs_idx_);
    else
        b = bv_null->find_rank(rank, 0, idx);
    return b;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::throw_no_rsc_index()
{
#ifndef BM_NO_STL
    throw std::domain_error("Rank-Select index not constructed, call sync() first");
#else
    BM_ASSERT_THROW(false, BM_ERR_RANGE);
#endif
}

//---------------------------------------------------------------------


template<class Val, class SV>
typename rsc_sparse_vector<Val, SV>::size_type
rsc_sparse_vector<Val, SV>::decode(value_type* arr,
                                   size_type   idx_from,
                                   size_type   size,
                                   bool        zero_mem) const
{
    if (size == 0)
        return 0;
    if (!in_sync_)
        throw_no_rsc_index(); // call sync() to build RSC fast access index
        
    BM_ASSERT(arr);
    BM_ASSERT(rs_idx_);
    
    if (idx_from >= this->size())
        return 0;
    
    if ((bm::id_max - size) <= idx_from)
        size = bm::id_max - idx_from;
    if ((idx_from + size) > this->size())
        size = this->size() - idx_from;

    const bvector_type* bv_null = sv_.get_null_bvector();
    size_type rank = bv_null->rank_corrected(idx_from, *rs_idx_);

    BM_ASSERT(rank == bv_null->count_range(0, idx_from) - bv_null->test(idx_from));

    size_type i = 0;

    bvector_enumerator_type en_i = bv_null->get_enumerator(idx_from);
    if (!en_i.valid())
        return i;

    if (zero_mem)
        ::memset(arr, 0, sizeof(value_type)*size);

    sparse_vector_const_iterator it = sv_.get_const_iterator(rank);
    if (it.valid())
    {
        do
        {
            size_type en_idx = *en_i;
            size_type delta = en_idx - idx_from;
            idx_from += delta;
            i += delta;
            if (i >= size)
                return size;
            arr[i++] = it.value();
            if (!en_i.advance())
                break;
            if (!it.advance())
                break;
            ++idx_from;
        } while (i < size);
    }
    return i;
}


template<class Val, class SV>
typename rsc_sparse_vector<Val, SV>::size_type
rsc_sparse_vector<Val, SV>::decode_buf(value_type*     arr,
                                       value_type*     arr_buf_tmp,
                                       size_type       idx_from,
                                       size_type       size,
                                       bool            zero_mem) const BMNOEXCEPT
{
    if (!size || (idx_from >= this->size()))
        return 0;

    BM_ASSERT(arr && arr_buf_tmp);
    BM_ASSERT(arr != arr_buf_tmp);
    BM_ASSERT(in_sync_);  // call sync() to build RSC fast access index
    BM_ASSERT(rs_idx_);

    if ((bm::id_max - size) <= idx_from)
        size = bm::id_max - idx_from;
    if ((idx_from + size) > this->size())
        size = this->size() - idx_from;

    if (zero_mem)
        ::memset(arr, 0, sizeof(value_type)*size);

    const bvector_type* bv_null = sv_.get_null_bvector();
    size_type rank = bv_null->rank_corrected(idx_from, *rs_idx_);

    BM_ASSERT(rank == bv_null->count_range(0, idx_from) - bv_null->test(idx_from));

    bvector_enumerator_type en_i = bv_null->get_enumerator(idx_from);
    if (!en_i.valid())
        return size;

    size_type i = en_i.value();
    if (idx_from + size <= i)  // empty space (all zeros)
        return size;

    size_type extract_cnt =
        bv_null->count_range_no_check(idx_from, idx_from + size - 1, *rs_idx_);

    BM_ASSERT(extract_cnt <= this->size());
    auto ex_sz = sv_.decode(arr_buf_tmp, rank, extract_cnt, true);
    BM_ASSERT(ex_sz == extract_cnt); (void) ex_sz;

    for (i = 0; i < extract_cnt; ++i)
    {
        BM_ASSERT(en_i.valid());
        size_type en_idx = *en_i;
        arr[en_idx-idx_from] = arr_buf_tmp[i];
        en_i.advance();
    } // for i

    return size;
}


//---------------------------------------------------------------------

template<class Val, class SV>
typename rsc_sparse_vector<Val, SV>::size_type
rsc_sparse_vector<Val, SV>::gather(value_type*      arr,
                                   const size_type* idx,
                                   size_type*       idx_tmp_buf,
                                   size_type        size,
                                   bm::sort_order   sorted_idx) const
{
    BM_ASSERT(arr);
    BM_ASSERT(idx);
    BM_ASSERT(idx_tmp_buf);
    BM_ASSERT(size);

    if (size == 1) // corner case: get 1 value
    {
        arr[0] = this->get(idx[0]);
        return size;
    }

    if (is_dense_) // rank-select idx recalc is not needed (with bounds check)
    {
        BM_ASSERT(in_sync_);
        for (size_type i = 0; i < size; ++i)
        {
            if (idx[i] < size_)
                idx_tmp_buf[i] = idx[i];
            else
            {
                idx_tmp_buf[i] = bm::id_max;
                if (sorted_idx == bm::BM_SORTED)
                    sorted_idx = bm::BM_UNKNOWN; // UNK will evaluate the sort-order
            }
        } // for i
    }
    else
    {
        // validate index, resolve rank addresses
        //
        for (size_type i = 0; i < size; ++i)
        {
            size_type sv_idx;
            if (resolve(idx[i], &sv_idx))
            {
                idx_tmp_buf[i] = sv_idx-1;
            }
            else
            {
                if (sorted_idx == bm::BM_SORTED)
                    sorted_idx = bm::BM_UNKNOWN; // UNK will evaluate the sort-order
                idx_tmp_buf[i] = bm::id_max;
            }
        } // for i
    }

    // gather the data using resolved indexes
    //
    size = sv_.gather(arr, idx_tmp_buf, size, sorted_idx);
    return size;
}


//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::construct_rs_index()
{
    if (rs_idx_)
        return;
    rs_idx_ = (rs_index_type*) bm::aligned_new_malloc(sizeof(rs_index_type));
    rs_idx_ = new(rs_idx_) rs_index_type(); // placement new
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::free_rs_index()
{
    if (rs_idx_)
    {
        rs_idx_->~rs_index_type();
        bm::aligned_free(rs_idx_);
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::copy_range(
                            const rsc_sparse_vector<Val, SV>& csv,
                            size_type left, size_type right)
{
    if (left > right)
        bm::xor_swap(left, right);

    if (left >= csv.size())
        return;

    size_ = csv.size_; max_id_ = csv.max_id_;
    in_sync_ = false;

    const bvector_type* arg_bv_null = csv.sv_.get_null_bvector();
    size_type sv_left, sv_right;
    bool range_valid = csv.resolve_range(left, right, &sv_left, &sv_right);
    if (!range_valid)
    {
        sv_.clear_all(true); sv_.resize(size_);
        bvector_type* bv_null = sv_.get_null_bvect();
        bv_null->copy_range(*arg_bv_null, 0, right);
        return;
    }
    bvector_type* bv_null = sv_.get_null_bvect();
    bv_null->copy_range(*arg_bv_null, 0, right); // not NULL vector gets a full copy
    sv_.copy_range(csv.sv_, sv_left, sv_right, bm::no_null); // don't copy NULL
}


//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::merge_not_null(rsc_sparse_vector<Val, SV>& csv)
{
    // MUST have the same NULL to work
    BM_ASSERT(sv_.get_null_bvector()->equal(*csv.sv_.get_null_bvector()));

    sv_.merge(csv.sv_);
}

//---------------------------------------------------------------------

template<class Val, class SV>
typename rsc_sparse_vector<Val, SV>::size_type
rsc_sparse_vector<Val, SV>::count_range_notnull(
                                        size_type left,
                                        size_type right) const BMNOEXCEPT
{
    if (left > right)
        bm::xor_swap(left, right);

    const bvector_type* bv_null = sv_.get_null_bvector();
    size_type range = right - left;
    if ((range < rs3_border0) || !in_sync_)
    {
        return bv_null->count_range_no_check(left, right);
    }
    return bv_null->count_range_no_check(left, right, *rs_idx_);
}

//---------------------------------------------------------------------
//
//---------------------------------------------------------------------


template<class Val, class SV>
rsc_sparse_vector<Val, SV>::back_insert_iterator::back_insert_iterator() BMNOEXCEPT
: csv_(0)
{}


//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::back_insert_iterator::back_insert_iterator
                                (rsc_sparse_vector_type* csv)
: csv_(csv),
  sv_bi_(csv->sv_.get_back_inserter())
{
    sv_bi_.disable_set_null(); // NULL will be handled outside
}

//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::back_insert_iterator::back_insert_iterator(
        const back_insert_iterator& bi)
: csv_(bi.csv_),
  sv_bi_(bi.sv_bi_)
{
}


//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::back_insert_iterator::~back_insert_iterator()
{
    this->flush();
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::back_insert_iterator::add(
    typename rsc_sparse_vector<Val, SV>::back_insert_iterator::value_type v)
{
    BM_ASSERT(csv_);
    BM_ASSERT(bm::id64_t(csv_->size_) + 1 < bm::id64_t(bm::id_max));

    sv_bi_.add_value_no_null(v);
    bvector_type* bv_null = sv_bi_.get_null_bvect();
    BM_ASSERT(bv_null);
    bv_null->set_bit_no_check(csv_->size_);
    
    csv_->max_id_++;
    csv_->size_++;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::back_insert_iterator::add_null() BMNOEXCEPT
{
    BM_ASSERT(csv_);
    BM_ASSERT(bm::id64_t(csv_->size_) + 1 < bm::id64_t(bm::id_max));

    csv_->max_id_++; csv_->size_++;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::back_insert_iterator::add_null(
    rsc_sparse_vector<Val, SV>::back_insert_iterator::size_type count) BMNOEXCEPT
{
    BM_ASSERT(csv_);
    BM_ASSERT(bm::id64_t(csv_->size_) + count < bm::id64_t(bm::id_max));

    csv_->max_id_+=count;
    csv_->size_+=count;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::back_insert_iterator::flush()
{
    if (sv_bi_.flush())
        csv_->in_sync_ = false;
}

//---------------------------------------------------------------------
//
//---------------------------------------------------------------------

template<class Val, class BV>
rsc_sparse_vector<Val, BV>::const_iterator::const_iterator() BMNOEXCEPT
: csv_(0), pos_(bm::id_max), buf_ptr_(0)
{}

//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::const_iterator::const_iterator(
    const typename rsc_sparse_vector<Val, SV>::const_iterator& it) BMNOEXCEPT
: csv_(it.csv_), pos_(it.pos_), buf_ptr_(0)
{}

//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::const_iterator::const_iterator(
  const typename rsc_sparse_vector<Val, SV>::const_iterator::rsc_sparse_vector_type* csv
  ) BMNOEXCEPT
: csv_(csv), buf_ptr_(0)
{
    BM_ASSERT(csv_);
    pos_ = csv_->empty() ? bm::id_max : 0u;
}

//---------------------------------------------------------------------

template<class Val, class SV>
rsc_sparse_vector<Val, SV>::const_iterator::const_iterator(
 const typename rsc_sparse_vector<Val, SV>::const_iterator::rsc_sparse_vector_type* csv,
 typename rsc_sparse_vector<Val, SV>::size_type pos) BMNOEXCEPT
: csv_(csv), buf_ptr_(0)
{
    BM_ASSERT(csv_);
    this->go_to(pos);
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::const_iterator::go_to(size_type pos) BMNOEXCEPT
{
    pos_ = (!csv_ || pos >= csv_->size()) ? bm::id_max : pos;
    buf_ptr_ = 0;
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::const_iterator::advance() BMNOEXCEPT
{
    if (pos_ == bm::id_max) // nothing to do, we are at the end
        return false;
    ++pos_;
    if (pos_ >= csv_->size())
    {
        this->invalidate();
        return false;
    }
    if (buf_ptr_)
    {
        ++buf_ptr_;
        if (buf_ptr_ - ((value_type*)vbuffer_.data()) >= n_buf_size)
            buf_ptr_ = 0;
    }
    return true;
}

//---------------------------------------------------------------------

template<class Val, class SV>
typename rsc_sparse_vector<Val, SV>::const_iterator::value_type
rsc_sparse_vector<Val, SV>::const_iterator::value() const
{
    BM_ASSERT(this->valid());
    value_type v;

    if (!buf_ptr_)
    {
        vbuffer_.reserve(n_buf_size * sizeof(value_type));
        tbuffer_.reserve(n_buf_size * sizeof(value_type));
        buf_ptr_ = (value_type*)(vbuffer_.data());
        value_type* tmp_buf_ptr = (value_type*) (tbuffer_.data());

        csv_->decode_buf(buf_ptr_, tmp_buf_ptr, pos_, n_buf_size, true);
    }
    v = *buf_ptr_;
    return v;
}

//---------------------------------------------------------------------

template<class Val, class SV>
void rsc_sparse_vector<Val, SV>::const_iterator::skip_zero_values() BMNOEXCEPT
{
    value_type v = value();
    if (buf_ptr_)
    {
        v = *buf_ptr_;
        value_type* buf_end = ((value_type*)vbuffer_.data()) + n_buf_size;
        while(!v)
        {
            ++pos_;
            if (++buf_ptr_ < buf_end)
                v = *buf_ptr_;
            else
                break;
        }
        if (pos_ >= csv_->size())
        {
            pos_ = bm::id_max;
            return;
        }
        if (buf_ptr_ >= buf_end)
            buf_ptr_ = 0;
    }
}

//---------------------------------------------------------------------

template<class Val, class SV>
bool rsc_sparse_vector<Val, SV>::const_iterator::is_null() const BMNOEXCEPT
{
    return csv_->is_null(pos_);
}


//---------------------------------------------------------------------



} // namespace bm



#endif
