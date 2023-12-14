#ifndef BMBMATRIX__H__INCLUDED__
#define BMBMATRIX__H__INCLUDED__
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

/*! \file bmbmatrix.h
    \brief basic bit-matrix class and utilities
*/

#include <stddef.h>
#include <type_traits>

#include "bmconst.h"

#ifndef BM_NO_STL
#include <stdexcept>
#endif

#include "bm.h"
#include "bmtrans.h"
#include "bmdef.h"



namespace bm
{

/**
    Basic dense bit-matrix class.
 
    Container of row-major bit-vectors, forming a bit-matrix.
    This class uses dense form of row storage.
    It is applicable as a build block for other sparse containers and
    succinct data structures, implementing high level abstractions.

    @ingroup bmagic
    @internal
*/
template<typename BV>
class basic_bmatrix
{
public:
    typedef BV                                       bvector_type;
    typedef bvector_type*                            bvector_type_ptr;
    typedef const bvector_type*                      bvector_type_const_ptr;
    typedef typename BV::allocator_type              allocator_type;
    typedef typename bvector_type::allocation_policy allocation_policy_type;
    typedef typename allocator_type::allocator_pool_type allocator_pool_type;
    typedef typename bvector_type::size_type             size_type;
    typedef typename bvector_type::block_idx_type        block_idx_type;
    typedef unsigned char                                octet_type;

public:
    // ------------------------------------------------------------
    /*! @name Construction, assignment                          */
    ///@{

    basic_bmatrix(size_type rsize,
                  allocation_policy_type ap = allocation_policy_type(),
                  size_type bv_max_size = bm::id_max,
                  const allocator_type&   alloc  = allocator_type());
    ~basic_bmatrix() BMNOEXCEPT;
    
    /*! copy-ctor */
    basic_bmatrix(const basic_bmatrix<BV>& bbm);
    basic_bmatrix<BV>& operator=(const basic_bmatrix<BV>& bbm)
    {
        copy_from(bbm);
        return *this;
    }
    
#ifndef BM_NO_CXX11
    /*! move-ctor */
    basic_bmatrix(basic_bmatrix<BV>&& bbm) BMNOEXCEPT;

    /*! move assignmment operator */
    basic_bmatrix<BV>& operator = (basic_bmatrix<BV>&& bbm) BMNOEXCEPT
    {
        if (this != &bbm)
        {
            free_rows();
            swap(bbm);
        }
        return *this;
    }

#endif

    void set_allocator_pool(allocator_pool_type* pool_ptr) BMNOEXCEPT;
    allocator_pool_type* get_allocator_pool() const BMNOEXCEPT
        { return pool_; }

    ///@}
    
    // ------------------------------------------------------------
    /*! @name content manipulation                                */
    ///@{

    /*! Swap content */
    void swap(basic_bmatrix<BV>& bbm) BMNOEXCEPT;
    
    /*! Copy content */
    void copy_from(const basic_bmatrix<BV>& bbm);

    /*! Freeze content into read-only mode drop editing overhead */
    void freeze();

    ///@}

    // ------------------------------------------------------------
    /*! @name row access                                         */
    ///@{

    /*! Get row bit-vector. Can return NULL */
    const bvector_type* row(size_type i) const BMNOEXCEPT;

    /*! Get row bit-vector. Can return NULL */
    bvector_type_const_ptr get_row(size_type i) const BMNOEXCEPT;

    /*! Get row bit-vector. Can return NULL */
    bvector_type* get_row(size_type i) BMNOEXCEPT;
    
    /*! get number of value rows */
    size_type rows() const BMNOEXCEPT { return rsize_; }

    /*! get number of value rows without  (not) NULLs bvector */
    size_type rows_not_null() const BMNOEXCEPT { return rsize_ - bool(null_idx_);}

    /*! get number of assigned avlue rows without \ NULLs bvector */
    size_type calc_effective_rows_not_null() const BMNOEXCEPT;

    
    /*! Make sure row is constructed, return bit-vector */
    bvector_type_ptr construct_row(size_type row);

    /*! Make sure row is copy-constructed, return bit-vector */
    bvector_type_ptr construct_row(size_type row, const bvector_type& bv);

    /*! destruct/deallocate row */
    void destruct_row(size_type row);

    /*! clear row bit-vector */
    void clear_row(size_type row, bool free_mem);

    /** return index of the NULL vector */
    size_type get_null_idx() const BMNOEXCEPT { return null_idx_; }

    /** set index of the NULL vector */
    void set_null_idx(size_type null_idx) BMNOEXCEPT;

    /** allocate matrix rows of bit-vectors (new rows are NULLs) */
    void allocate_rows(size_type rsize);

    /** Free all rows */
    void free_rows() BMNOEXCEPT;

    ///@}
    
    
    // ------------------------------------------------------------
    /*! @name octet access and transposition                     */
    ///@{

    /*!
        Bit-transpose an octet and assign it to a bit-matrix
     
        @param pos - column position in the matrix
        @param octet_idx - octet based row position (1 octet - 8 rows)
        @param octet - value to assign
    */
    void set_octet(size_type pos, size_type octet_idx, unsigned char octet);

    /*!
        Bit-transpose and insert an octet and assign it to a bit-matrix
     
        @param pos - column position in the matrix
        @param octet_idx - octet based row position (1 octet - 8 rows)
        @param octet - value to assign
    */
    void insert_octet(size_type pos, size_type octet_idx, unsigned char octet);

    /*!
        return octet from the matrix
     
        @param pos - column position in the matrix
        @param octet_idx - octet based row position (1 octet - 8 rows)
    */
    unsigned char get_octet(size_type pos, size_type octet_idx) const BMNOEXCEPT;
    
    /*!
        Compare vector[pos] with octet
     
        It uses regulat comparison of chars to comply with the (signed)
        char sort order.
     
        @param pos - column position in the matrix
        @param octet_idx - octet based row position (1 octet - 8 rows)
        @param octet - octet value to compare
     
        @return 0 - equal, -1 - less(vect[pos] < octet), 1 - greater
    */
    int compare_octet(size_type pos,
                      size_type octet_idx, char octet) const BMNOEXCEPT;

    /*!
            Return number of octet currently allocated rows in the matrix
     */
    size_type octet_size() const BMNOEXCEPT;
    
    ///@}


    // ------------------------------------------------------------
    /*! @name Utility functions                                  */
    ///@{
    

    /// Get low level internal access to
    const bm::word_t* get_block(size_type p,
                                unsigned i, unsigned j) const BMNOEXCEPT;

    /**
        Clear bit-planes bit
        @param slice_from - clear from [from, until)
        @param slice_until - clear until (open interval!)
        @param idx - bit index
     */
    void clear_slices_range(unsigned slice_from, unsigned slize_until,
                            size_type idx);
    
    unsigned get_half_octet(size_type pos, size_type row_idx) const BMNOEXCEPT;

    /*!
        \brief run memory optimization for all bit-vector rows
        \param temp_block - pre-allocated memory block to avoid re-allocs
        \param opt_mode - requested compression depth
        \param stat - memory allocation statistics after optimization
    */
    void optimize(
        bm::word_t* temp_block = 0,
        typename bvector_type::optmode opt_mode = bvector_type::opt_compress,
        typename bvector_type::statistics* stat = 0);
    
    /*! Optimize block in all planes
        @internal
    */
    void optimize_block(block_idx_type nb, typename BV::optmode opt_mode);

    /*! Compute memory statistics
        @param st [out] - statistics object
        @param rsize - row size (for cases when operation is limited not to touch the NULL bit-vector)
    */
    void calc_stat(typename bvector_type::statistics& st,
                   size_type rsize) const BMNOEXCEPT;

    /*! Erase column/element
        @param idx - index (of column) / element of bit-vectors to erase
        @param erase_null - erase all including NULL vector (true)
    */
    void erase_column(size_type idx, bool erase_null);

    /*! Insert value 0 into a range of rows
        @param idx - index (of column) / element of bit-vectors to erase
        @param row_from - row to start from
    */
    void insert_column(size_type idx, size_type row_from);

    /*! Clear value (set 0) into a range of rows
        @param idx - index (of column) / element of bit-vectors
        @param row_from - row to start from
    */
    void clear_column(size_type idx, size_type row_from);

    /**
        Set SUB (MINUS) operation on all existing rows
        @param bv - argument vector row[i] -= bv
        @param use_null - if true clear the NULL vector as well
     */
    void bit_sub_rows(const bvector_type& bv, bool use_null);

    /**
        Set AND (intersect) operation on all existing rows
        @param bv - argument vector row[i] &= bv
     */
    void bit_and_rows(const bvector_type& bv);


    ///@}

protected:

    bvector_type* construct_bvector(const bvector_type* bv) const;
    void destruct_bvector(bvector_type* bv) const;
    
    static
    void throw_bad_alloc() { BV::throw_bad_alloc(); }

    template<typename Val, typename BVect, unsigned MAX_SIZE>
    friend class base_sparse_vector;

protected:
    size_type                bv_size_;
    allocator_type           alloc_;
    allocation_policy_type   ap_;
    allocator_pool_type*     pool_;
    
    bvector_type_ptr*        bv_rows_;
    size_type                rsize_;
    size_type                null_idx_; ///< Index of the NULL row
};

/**
    Base class for bit-transposed(bit-sliced) sparse vector construction.
    Keeps the basic housekeeping lements like matrix of sparse elements
 
    @ingroup bmagic
    @internal
*/
template<typename Val, typename BV, unsigned MAX_SIZE>
class base_sparse_vector
{
public:
    enum bit_planes
    {
        sv_slices = (sizeof(Val) * 8 * MAX_SIZE + 1),
        sv_value_slices = (sizeof(Val) * 8 * MAX_SIZE)
    };

    enum vector_capacity
    {
        max_vector_size = MAX_SIZE
    };

    typedef Val                                      value_type;
    typedef BV                                       bvector_type;
    typedef typename BV::size_type                   size_type;
    typedef bvector_type*                            bvector_type_ptr;
    typedef const bvector_type*                      bvector_type_const_ptr;
    typedef const value_type&                        const_reference;
    typedef typename BV::allocator_type              allocator_type;
    typedef typename bvector_type::allocation_policy allocation_policy_type;
    typedef typename bvector_type::enumerator        bvector_enumerator_type;
    typedef typename allocator_type::allocator_pool_type   allocator_pool_type;
    typedef bm::basic_bmatrix<BV>                          bmatrix_type;
    typedef typename std::make_unsigned<value_type>::type  unsigned_value_type;

public:
    base_sparse_vector();

    base_sparse_vector(bm::null_support        null_able,
                       allocation_policy_type  ap = allocation_policy_type(),
                       size_type               bv_max_size = bm::id_max,
                       const allocator_type&   alloc = allocator_type());
    
    base_sparse_vector(const base_sparse_vector<Val, BV, MAX_SIZE>& bsv);

#ifndef BM_NO_CXX11
    /*! move-ctor */
    base_sparse_vector(base_sparse_vector<Val, BV, MAX_SIZE>&& bsv) BMNOEXCEPT
    {
        bmatr_.swap(bsv.bmatr_);
        size_ = bsv.size_;
        effective_slices_ = bsv.effective_slices_;
        bsv.size_ = 0;
    }
#endif

    void swap(base_sparse_vector<Val, BV, MAX_SIZE>& bsv) BMNOEXCEPT;

    size_type size() const BMNOEXCEPT { return size_; }
    
    void resize(size_type new_size, bool set_null);

    void clear_range(size_type left, size_type right, bool set_null);

    void keep_range_no_check(size_type left, size_type right,
                             bm::null_support slice_null);

    /*! \brief resize to zero, free memory
        @param free_mem - fully destroys the plane vectors if true
    */
    void clear_all(bool free_mem = true) BMNOEXCEPT;
    
    /*! return true if empty */
    bool empty() const BMNOEXCEPT { return size() == 0; }

public:

    // ------------------------------------------------------------
    /*! @name Various traits                                     */
    ///@{
    ///

    /**
        \brief returns true if value type is signed integral type
     */
    static constexpr bool is_signed() BMNOEXCEPT
        { return std::is_signed<value_type>::value; }

    /**
        \brief check if container supports NULL(unassigned) values
    */
    bool is_nullable() const BMNOEXCEPT { return bmatr_.get_null_idx(); }

    /**
        \brief check if container supports NULL (unassigned) values
    */
    bm::null_support get_null_support() const BMNOEXCEPT
        { return is_nullable() ? bm::use_null : bm::no_null; }

    /**
        \brief Get bit-vector of assigned values or NULL
        (if not constructed that way)
    */
    const bvector_type* get_null_bvector() const BMNOEXCEPT
    {
        if (size_type null_idx = bmatr_.get_null_idx())
            return bmatr_.get_row(null_idx);
        return 0;
    }
    
    /** \brief test if specified element is NULL
        \param idx - element index
        \return true if it is NULL false if it was assigned or container
        is not configured to support assignment flags
    */
    bool is_null(size_type idx) const BMNOEXCEPT;

    /**
        Set allocation pool
     */
    void set_allocator_pool(allocator_pool_type* pool_ptr) BMNOEXCEPT
        { bmatr_.set_allocator_pool(pool_ptr); }

    /**
        Get allocation pool
     */
    allocator_pool_type* get_allocator_pool() const BMNOEXCEPT
        { return bmatr_.get_allocator_pool(); }

    ///@}


    // ------------------------------------------------------------
    /*! @name Access to internals                                */
    ///@{

    /*!
        \brief get access to bit-plain, function checks and creates a plane
        \return bit-vector for the bit plain
        @internal
    */
    bvector_type_ptr get_create_slice(unsigned i);

    /*!
        \brief get read-only access to bit-plane
        \return bit-vector for the bit plane or NULL
        @internal
    */
    bvector_type_const_ptr
    get_slice(unsigned i) const BMNOEXCEPT { return bmatr_.row(i); }

    /*!
        \brief get total number of bit-planes in the vector
        @internal
    */
    static unsigned slices() BMNOEXCEPT { return value_bits(); }

    /** Number of stored bit-planes (value planes + extra
        @internal
    */
    static unsigned stored_slices() BMNOEXCEPT { return value_bits()+1; }


    /** Number of effective bit-planes in the value type
        @internal
    */
    unsigned effective_slices() const BMNOEXCEPT
        { return effective_slices_ + 1; }

    /*!
        \brief get access to bit-plane as is (can return NULL)
        @internal
    */
    bvector_type_ptr slice(unsigned i) BMNOEXCEPT { return bmatr_.get_row(i); }
    bvector_type_const_ptr slice(unsigned i) const BMNOEXCEPT
                                    { return bmatr_.get_row(i); }

    bvector_type* get_null_bvect() BMNOEXCEPT
    {
        if (size_type null_idx = bmatr_.get_null_idx())
            return bmatr_.get_row(null_idx);
        return 0;
    }

    /*!
        \brief free memory in bit-plane
        @internal
    */
    void free_slice(unsigned i) { bmatr_.destruct_row(i); }
    
    /*!
        return mask of allocated bit-planes
        1 in the mask - means bit-plane N is present
        returns 64-bit unsigned mask for sub 64-bit types (like int)
        unallocated mask bits will be zero extended
     
        @return 64-bit mask
        @internal
    */
    bm::id64_t get_slice_mask(unsigned element_idx) const BMNOEXCEPT;

    /*!
        get read-only access to inetrnal bit-matrix
        @internal
    */
    const bmatrix_type& get_bmatrix() const BMNOEXCEPT { return bmatr_; }

    /**
        access to internal bit-matrix
        @internal
     */
    bmatrix_type& get_bmatrix() BMNOEXCEPT { return bmatr_; }

    /**
        Set NULL plain index
        @internal
     */
    void mark_null_idx(unsigned null_idx) BMNOEXCEPT
        { bmatr_.null_idx_ = null_idx; }
    /**
        Convert signed value type to unsigned representation
        @internal
     */
    static
    unsigned_value_type s2u(value_type v) BMNOEXCEPT;

    /**
        Convert unsigned value type to signed representation
        @internal
    */
    static
    value_type u2s(unsigned_value_type v) BMNOEXCEPT;


    ///@}
    ///
    // -------------------------------------------------------------------
    
    /*!
        \brief run memory optimization for all bit-vector rows
        \param temp_block - pre-allocated memory block to avoid unnecessary re-allocs
        \param opt_mode - requested compression depth
        \param stat - memory allocation statistics after optimization
    */
    void optimize(bm::word_t* temp_block = 0,
                  typename bvector_type::optmode opt_mode = bvector_type::opt_compress,
                  typename bvector_type::statistics* stat = 0);

    /*!
        @brief Calculates memory statistics.

        Function fills statistics structure containing information about how
        this vector uses memory and estimation of max. amount of memory
        bvector needs to serialize itself.

        @param st - pointer on statistics structure to be filled in.

        @sa statistics
    */
    void calc_stat(typename bvector_type::statistics* st) const BMNOEXCEPT;

    /*!
        \brief check if another sparse vector has the same content and size
     
        \param sv        - sparse vector for comparison
        \param null_able - flag to consider NULL vector in comparison (default)
                           or compare only value content planes
     
        \return true, if it is the same
    */
    bool equal(const base_sparse_vector<Val, BV, MAX_SIZE>& sv,
               bm::null_support null_able = bm::use_null) const BMNOEXCEPT;

protected:
    void copy_from(const base_sparse_vector<Val, BV, MAX_SIZE>& bsv);

    /**
        Merge plane bvectors from an outside base matrix
        Note: outside base matrix gets destroyed
     */
    void merge_matr(bmatrix_type& bmatr);

    /**
        Turn on RO mode
     */
    void freeze_matr() { bmatr_.freeze(); is_ro_ = true; }

    /*!
        clear column in all value planes
        \param plane_idx - row (plane index to start from)
        \param idx       - bit (column) to clear
    */
    void clear_value_planes_from(unsigned plane_idx, size_type idx);

    /*!
        insert false (clear) column in all value planes
        \param plane_idx - row (plane index to start from)
        \param idx       - bit (column) to clear insert
    */
    void insert_clear_value_planes_from(unsigned plane_idx, size_type idx);
    
    /*!
        erase bit (column) from all planes
        \param idx - bit (column) to erase
        \param erase_null - erase the NULL vector
    */
    void erase_column(size_type idx, bool erase_null);
    
    /*!
        insert (NOT) NULL value
    */
    void insert_null(size_type idx, bool not_null);

    /**
        Set SUB (MINUS) operation on all existing bit-slices
        @param bv - argument vector row[i] -= bv
     */
    void bit_sub_rows(const bvector_type& bv, bool use_null)
        { bmatr_.bit_sub_rows(bv, use_null); }

    /**
        Set AND (intersect) operation on all existing bit-slices
        @param bv - argument vector row[i] -= bv
     */
    void bit_and_rows(const bvector_type& bv) { bmatr_.bit_and_rows(bv); }

protected:
    typedef typename bvector_type::block_idx_type block_idx_type;

    /** Number of total bit-planes in the value type*/
    static constexpr unsigned value_bits() BMNOEXCEPT
        { return base_sparse_vector<Val, BV, MAX_SIZE>::sv_value_slices; }
    
    /** plane index for the "NOT NULL" flags plane */
    //static constexpr unsigned null_plane() BMNOEXCEPT { return value_bits(); }
    
    /** optimize block in all matrix planes */
    void optimize_block(block_idx_type nb, typename BV::optmode opt_mode)
        { bmatr_.optimize_block(nb, opt_mode); }

    /// Sybc read-only state
    void sync_ro() BMNOEXCEPT;

    /**
        Perform copy_range() on a set of planes
    */
    void copy_range_slices(
        const base_sparse_vector<Val, BV, MAX_SIZE>& bsv,
        typename base_sparse_vector<Val, BV, MAX_SIZE>::size_type left,
        typename base_sparse_vector<Val, BV, MAX_SIZE>::size_type right,
        bm::null_support slice_null);

protected:
    bmatrix_type             bmatr_;              ///< bit-transposed matrix
    unsigned_value_type      slice_mask_ = 0;     ///< slice presence bit-mask
    size_type                size_ = 0;           ///< array size
    unsigned                 effective_slices_=0; ///< number of bit slices actually allocated
    bool                     is_ro_=false; ///< read-only
};

//---------------------------------------------------------------------
//
//---------------------------------------------------------------------

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4146 )
#endif

template<typename BV>
basic_bmatrix<BV>::basic_bmatrix(size_type rsize,
              allocation_policy_type ap,
              size_type bv_max_size,
              const allocator_type&   alloc)
: bv_size_(bv_max_size),
  alloc_(alloc),
  ap_(ap),
  pool_(0),
  bv_rows_(0),
  rsize_(0),
  null_idx_(0)
{
    allocate_rows(rsize);
}

//---------------------------------------------------------------------

template<typename BV>
basic_bmatrix<BV>::~basic_bmatrix() BMNOEXCEPT
{
    free_rows();
}

//---------------------------------------------------------------------

template<typename BV>
basic_bmatrix<BV>::basic_bmatrix(const basic_bmatrix<BV>& bbm)
: bv_size_(bbm.bv_size_),
  alloc_(bbm.alloc_),
  ap_(bbm.ap_),
  pool_(0),
  bv_rows_(0),
  rsize_(0),
  null_idx_(0)
{
    copy_from(bbm);
}

//---------------------------------------------------------------------

template<typename BV>
basic_bmatrix<BV>::basic_bmatrix(basic_bmatrix<BV>&& bbm) BMNOEXCEPT
: bv_size_(bbm.bv_size_),
  alloc_(bbm.alloc_),
  ap_(bbm.ap_),
  pool_(0),
  bv_rows_(0),
  rsize_(0),
  null_idx_(0)
{
    swap(bbm);
}

//---------------------------------------------------------------------

template<typename BV>
const typename basic_bmatrix<BV>::bvector_type*
basic_bmatrix<BV>::row(size_type i) const BMNOEXCEPT
{
    BM_ASSERT(i < rsize_);
    return bv_rows_[i];
}

//---------------------------------------------------------------------

template<typename BV>
const typename basic_bmatrix<BV>::bvector_type*
basic_bmatrix<BV>::get_row(size_type i) const BMNOEXCEPT
{
    BM_ASSERT(i < rsize_);
    return bv_rows_[i];
}

//---------------------------------------------------------------------

template<typename BV>
typename basic_bmatrix<BV>::bvector_type*
basic_bmatrix<BV>::get_row(size_type i) BMNOEXCEPT
{
    BM_ASSERT(i < rsize_);
    return bv_rows_[i];
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::set_null_idx(size_type null_idx) BMNOEXCEPT
{
    BM_ASSERT(null_idx);
    BM_ASSERT(get_row(null_idx));
    null_idx_ = null_idx;
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::copy_from(const basic_bmatrix<BV>& bbm)
{
    if (this == &bbm) // nothing to do
        return;
    free_rows();

    bv_size_ = bbm.bv_size_;
    alloc_ = bbm.alloc_;
    ap_ = bbm.ap_;

    null_idx_ = bbm.null_idx_;

    size_type rsize = bbm.rsize_;
    if (rsize)
    {
        bv_rows_ = (bvector_type_ptr*)alloc_.alloc_ptr(rsize);
        if (!bv_rows_)
            throw_bad_alloc();
        rsize_ = rsize;
        for (size_type i = 0; i < rsize_; ++i)
        {
            const bvector_type_ptr bv = bbm.bv_rows_[i];
            bv_rows_[i] = bv ?  construct_bvector(bv) : 0;
        }
    }
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::erase_column(size_type idx, bool erase_null)
{
    // relies that NULL bvector is the last
    size_type r_to = (!erase_null && null_idx_) ? null_idx_ : rsize_;
    for (size_type i = 0; i < r_to; ++i)
        if (bvector_type* bv = get_row(i))
            bv->erase(idx);
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::insert_column(size_type idx,
                                      size_type row_from)
{
    for (size_type i = row_from; i < rsize_; ++i)
        if (bvector_type* bv = get_row(i))
            bv->insert(idx, false);
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::clear_column(size_type idx,
                                     size_type row_from)
{
    for (size_type i = row_from; i < rsize_; ++i)
        if (bvector_type* bv = get_row(i))
            bv->clear_bit_no_check(idx);
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::allocate_rows(size_type rsize)
{
    size_type rsize_prev(rsize_);
    if (rsize <= rsize_prev)
        return; // nothing to do
    bvector_type_ptr* bv_rows_prev(bv_rows_);

    BM_ASSERT(rsize);
    bv_rows_ = (bvector_type_ptr*)alloc_.alloc_ptr(unsigned(rsize));
    if (!bv_rows_)
        throw_bad_alloc();
    rsize_ = rsize;
    BM_ASSERT((!null_idx_) || (rsize & 1u));
    ::memset(&bv_rows_[0], 0, rsize * sizeof(bv_rows_[0]));
    if (bv_rows_prev) // deallocate prev, reset NULL
    {
        for (size_type i = 0; i < rsize_prev; ++i)
            bv_rows_[i] = bv_rows_prev[i];
        alloc_.free_ptr(bv_rows_prev, unsigned(rsize_prev));
        if (null_idx_) // shift-up the NULL row
        {
            bv_rows_[rsize-1] = bv_rows_[null_idx_];
            bv_rows_[null_idx_] = 0;
            null_idx_ = rsize-1;
        }
    }
}

//---------------------------------------------------------------------

template<typename BV>
typename basic_bmatrix<BV>::size_type
basic_bmatrix<BV>::octet_size() const BMNOEXCEPT
{
    size_type osize = (7 + rows_not_null()) / 8;
    return osize;
}


//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::free_rows() BMNOEXCEPT
{
    for (size_type i = 0; i < rsize_; ++i)
    {
        if (bvector_type_ptr bv = bv_rows_[i])
        {
            destruct_bvector(bv);
            bv_rows_[i] = 0;
        }
    } // for i
    if (bv_rows_)
        alloc_.free_ptr(bv_rows_, unsigned(rsize_));
    bv_rows_ = 0;
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::set_allocator_pool(allocator_pool_type* pool_ptr) BMNOEXCEPT
{
    if (pool_ != pool_ptr)
    {
        for (size_type i = 0; i < rsize_; ++i)
            if (bvector_type_ptr bv = bv_rows_[i])
                bv->set_allocator_pool(pool_ptr);
    }
    pool_ = pool_ptr;
}


//---------------------------------------------------------------------

template<typename BV>
typename basic_bmatrix<BV>::size_type
basic_bmatrix<BV>::calc_effective_rows_not_null() const BMNOEXCEPT
{
    // TODO: SIMD
    auto i = rows_not_null();
    for (--i; i > 0; --i)
    {
        if (bv_rows_[i])
        {
            ++i;
            break;
        }
    }
    return i;
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::bit_sub_rows(const bvector_type& bv, bool use_null)
{
    size_type sz = use_null ? rsize_: calc_effective_rows_not_null();
    for (size_type i = 0; i < sz; ++i)
    {
        if (bvector_type_ptr bv_r = bv_rows_[i])
            bv_r->bit_sub(bv);
    } // for i
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::bit_and_rows(const bvector_type& bv)
{
    for (size_type i = 0; i < rsize_; ++i)
    {
        if (bvector_type_ptr bv_r = bv_rows_[i])
            bv_r->bit_and(bv);
    } // for i
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::swap(basic_bmatrix<BV>& bbm) BMNOEXCEPT
{
    if (this == &bbm)
        return;
    
    bm::xor_swap(bv_size_, bbm.bv_size_);

    allocator_type alloc_tmp = alloc_;
    alloc_ = bbm.alloc_;
    bbm.alloc_ = alloc_tmp;

    allocation_policy_type ap_tmp = ap_;
    ap_ = bbm.ap_;
    bbm.ap_ = ap_tmp;
    
    allocator_pool_type*     pool_tmp = pool_;
    pool_ = bbm.pool_;
    bbm.pool_ = pool_tmp;

    bm::xor_swap(rsize_, bbm.rsize_);
    bm::xor_swap(null_idx_, bbm.null_idx_);
    
    bvector_type_ptr* rtmp = bv_rows_;
    bv_rows_ = bbm.bv_rows_;
    bbm.bv_rows_ = rtmp;
}

//---------------------------------------------------------------------

template<typename BV>
typename basic_bmatrix<BV>::bvector_type_ptr
basic_bmatrix<BV>::construct_row(size_type row)
{
    if (row >= rsize_)
        allocate_rows(row + 8);
    BM_ASSERT(row < rsize_);
    bvector_type_ptr bv = bv_rows_[row];
    if (!bv)
        bv = bv_rows_[row] = construct_bvector(0);
    return bv;
}

//---------------------------------------------------------------------

template<typename BV>
typename basic_bmatrix<BV>::bvector_type_ptr
basic_bmatrix<BV>::construct_row(size_type row, const bvector_type& bv_src)
{
    if (row >= rsize_)
        allocate_rows(row + 8);
    BM_ASSERT(row < rsize_);
    bvector_type_ptr bv = bv_rows_[row];
    if (bv)
        *bv = bv_src;
    else
        bv = bv_rows_[row] = construct_bvector(&bv_src);
    return bv;
}


//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::destruct_row(size_type row)
{
    BM_ASSERT(row < rsize_);
    if (bvector_type_ptr bv = bv_rows_[row])
    {
        destruct_bvector(bv);
        bv_rows_[row] = 0;
    }
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::clear_row(size_type row, bool free_mem)
{
    BM_ASSERT(row < rsize_);
    if (bvector_type_ptr bv = bv_rows_[row])
    {
        if (free_mem)
        {
            destruct_bvector(bv);
            bv_rows_[row] = 0;
        }
        else
            bv->clear(true);
    }
}

//---------------------------------------------------------------------

template<typename BV>
typename basic_bmatrix<BV>::bvector_type*
basic_bmatrix<BV>::construct_bvector(const bvector_type* bv) const
{
    bvector_type* rbv = 0;
#ifdef BM_NO_STL   // C compatibility mode
    void* mem = ::malloc(sizeof(bvector_type));
    if (mem == 0)
    {
        BM_THROW(false, BM_ERR_BADALLOC);
    }
    if (bv)
        rbv = new(mem) bvector_type(*bv);
    else
    {
        rbv = new(mem) bvector_type(ap_.strat, ap_.glevel_len, bv_size_, alloc_);
        rbv->init();
    }
#else
    if (bv)
        rbv = new bvector_type(*bv);
    else
    {
        rbv = new bvector_type(ap_.strat, ap_.glevel_len, bv_size_, alloc_);
        rbv->init();
    }
#endif
    if (pool_)
        rbv->set_allocator_pool(pool_);
    return rbv;
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::destruct_bvector(bvector_type* bv) const
{
#ifdef BM_NO_STL   // C compatibility mode
    bv->~TBM_bvector();
    ::free((void*)bv);
#else
    delete bv;
#endif
}

//---------------------------------------------------------------------

template<typename BV>
const bm::word_t*
basic_bmatrix<BV>::get_block(size_type p,
                             unsigned i, unsigned j) const BMNOEXCEPT
{
    if (bvector_type_const_ptr bv = this->row(p))
        return bv->get_blocks_manager().get_block_ptr(i, j);
    return 0;
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::clear_slices_range(
                        unsigned slice_from, unsigned slice_until,
                        size_type idx)
{
    for (unsigned p = slice_from; p < slice_until; ++p)
        if (bvector_type* bv = this->get_row(p)) // TODO: optimize cleaning
            bv->clear_bit_no_check(idx);
}


//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::set_octet(size_type pos,
                                  size_type octet_idx,
                                  unsigned char octet)
{
    if (7u + octet_idx * 8u > rsize_)
        allocate_rows(rsize_ + 16);

    size_type row = octet_idx * 8;
    size_type row_end = row + 8;
    for (; row < row_end; ++row)
    {
        bvector_type* bv = (row < rsize_) ? this->get_row(row) : 0;
        if (octet & 1u)
        {
            if (!bv)
                bv = this->construct_row(row);
            bv->set_bit_no_check(pos);
        }
        else
        {
            if (bv)
                bv->clear_bit_no_check(pos);
        }
        octet >>= 1;
        if (!octet)
            break;
    } // for
    
    // clear the tail
    if (row_end > rsize_)
        row_end = rsize_;
    for (++row; row < row_end; ++row)
        if (bvector_type* bv = this->get_row(row))
            bv->clear_bit_no_check(pos);
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::insert_octet(size_type pos,
                                     size_type octet_idx,
                                     unsigned char octet)
{
    BM_ASSERT(octet_idx * 8u < rsize_);
    
    size_type oct = octet;
    size_type row = octet_idx * 8;
    size_type row_end = row + 8;
    for (; row < row_end; ++row)
    {
        bvector_type* bv = (row < rsize_) ? this->get_row(row) : 0;
        if (oct & 1u)
        {
            if (!bv)
            {
                bv = this->construct_row(row);
                bv->set_bit_no_check(pos);
            }
            else
            {
                bv->insert(pos, true);
            }
        }
        else
        {
            if (bv)
                bv->insert(pos, false);
        }
        oct >>= 1;
        if (!oct)
            break;
    } // for
    
    // clear the tail
    if (row_end > rsize_)
        row_end = rsize_;
    for (++row; row < row_end; ++row)
        if (bvector_type* bv = this->get_row(row))
            bv->insert(pos, false);
}


//---------------------------------------------------------------------

template<typename BV>
unsigned char
basic_bmatrix<BV>::get_octet(size_type pos, size_type octet_idx) const BMNOEXCEPT
{
    unsigned v = 0;

    block_idx_type nb = (pos >>  bm::set_block_shift);
    unsigned i0 = unsigned(nb >> bm::set_array_shift); // top block address
    unsigned j0 = unsigned(nb &  bm::set_array_mask);  // address in sub-block

    const bm::word_t* blk;
    const bm::word_t* blka[8];
    unsigned nbit = unsigned(pos & bm::set_block_mask);
    unsigned nword  = unsigned(nbit >> bm::set_word_shift);
    unsigned mask0 = 1u << (nbit & bm::set_word_mask);
    
    unsigned row_idx = unsigned(octet_idx * 8);
    if (row_idx + 7 >= rsize_ ||
        (null_idx_ && (row_idx + 7 > null_idx_))) // out of bounds request?
        return (unsigned char)v;

    blka[0] = get_block(row_idx+0, i0, j0);
    blka[1] = get_block(row_idx+1, i0, j0);
    blka[2] = get_block(row_idx+2, i0, j0);
    blka[3] = get_block(row_idx+3, i0, j0);
    blka[4] = get_block(row_idx+4, i0, j0);
    blka[5] = get_block(row_idx+5, i0, j0);
    blka[6] = get_block(row_idx+6, i0, j0);
    blka[7] = get_block(row_idx+7, i0, j0);
    unsigned is_set;
    
    if ((blk = blka[0])!=0)
    {
        if (blk == FULL_BLOCK_FAKE_ADDR)
            is_set = 1;
        else
            is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
        v |= (unsigned)bool(is_set);
    }
    if ((blk = blka[1])!=0)
    {
        if (blk == FULL_BLOCK_FAKE_ADDR)
            is_set = 1;
        else
            is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
        v |= unsigned(bool(is_set)) << 1u;
    }
    if ((blk = blka[2])!=0)
    {
        if (blk == FULL_BLOCK_FAKE_ADDR)
            is_set = 1;
        else
            is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
        v |= unsigned(bool(is_set)) << 2u;
    }
    if ((blk = blka[3])!=0)
    {
        if (blk == FULL_BLOCK_FAKE_ADDR)
            is_set = 1;
        else
            is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
        v |= unsigned(bool(is_set)) << 3u;
    }
    
    
    if ((blk = blka[4])!=0)
    {
        if (blk == FULL_BLOCK_FAKE_ADDR)
            is_set = 1;
        else
            is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
        v |= unsigned(bool(is_set)) << 4u;
    }
    if ((blk = blka[5])!=0)
    {
        if (blk == FULL_BLOCK_FAKE_ADDR)
            is_set = 1;
        else
            is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
        v |= unsigned(bool(is_set)) << 5u;
    }
    if ((blk = blka[6])!=0)
    {
        if (blk == FULL_BLOCK_FAKE_ADDR)
            is_set = 1;
        else
            is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
        v |= unsigned(bool(is_set)) << 6u;
    }
    if ((blk = blka[7])!=0)
    {
        if (blk == FULL_BLOCK_FAKE_ADDR)
            is_set = 1;
        else
            is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
        v |= unsigned(bool(is_set)) << 7u;
    }
   
    return (unsigned char)v;
}

//---------------------------------------------------------------------

template<typename BV>
int basic_bmatrix<BV>::compare_octet(size_type pos,
                                     size_type octet_idx,
                                     char      octet) const BMNOEXCEPT
{
    char value = char(get_octet(pos, octet_idx));
    return (value > octet) - (value < octet);
}

//---------------------------------------------------------------------

/**
    Test 4 pointers are all marked as GAPs
    @internal
 */
inline
bool test_4gaps(const bm::word_t* p0, const bm::word_t* p1,
                const bm::word_t* p2, const bm::word_t* p3) BMNOEXCEPT
{
    uintptr_t p
        = uintptr_t(p0) | uintptr_t(p1) | uintptr_t(p2) | uintptr_t(p3);
    return (p & 1);
}
/**
    Test 4 pointers are not NULL and not marked as FULLBLOCK
    @internal
*/
inline
bool test_4bits(const bm::word_t* p0, const bm::word_t* p1,
                const bm::word_t* p2, const bm::word_t* p3) BMNOEXCEPT
{
    return p0 && p0!=FULL_BLOCK_FAKE_ADDR &&
           p1 && p1!=FULL_BLOCK_FAKE_ADDR &&
           p2 && p2!=FULL_BLOCK_FAKE_ADDR &&
           p3 && p3!=FULL_BLOCK_FAKE_ADDR;
}


template<typename BV>
unsigned
basic_bmatrix<BV>::get_half_octet(size_type pos, size_type row_idx) const BMNOEXCEPT
{
    unsigned v = 0;

    block_idx_type nb = (pos >>  bm::set_block_shift);
    unsigned i0 = unsigned(nb >> bm::set_array_shift); // top block address
    unsigned j0 = unsigned(nb &  bm::set_array_mask);  // address in sub-block

    const bm::word_t* blk;
    const bm::word_t* blka[4];
    unsigned nbit = unsigned(pos & bm::set_block_mask);

    blka[0] = get_block(row_idx+0, i0, j0);
    blka[1] = get_block(row_idx+1, i0, j0);
    blka[2] = get_block(row_idx+2, i0, j0);
    blka[3] = get_block(row_idx+3, i0, j0);
    unsigned is_set;


    unsigned nword  = unsigned(nbit >> bm::set_word_shift);
    unsigned mask0 = 1u << (nbit & bm::set_word_mask);

    // speculative assumption that nibble is often 4 bit-blocks
    // and we will be able to extract it faster with less mispredicts
    //
    if (!test_4gaps(blka[0], blka[1], blka[2], blka[3]))
    {
        if (test_4bits(blka[0], blka[1], blka[2], blka[3]))
        {
            v = unsigned(bool((blka[0][nword] & mask0))) |
                unsigned(bool((blka[1][nword] & mask0)) << 1u) |
                unsigned(bool((blka[2][nword] & mask0)) << 2u) |
                unsigned(bool((blka[3][nword] & mask0)) << 3u);
            return v;
        }
    }
    // hypothesis above didn't work out extract the regular way
    unsigned i = 0;
    do
    {
        if ((blk = blka[i])!=0)
        {
            if (blk == FULL_BLOCK_FAKE_ADDR)
                is_set = 1;
            else
                is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
            v |= unsigned(bool(is_set)) << i;
        }
        if ((blk = blka[++i])!=0)
        {
            if (blk == FULL_BLOCK_FAKE_ADDR)
                is_set = 1;
            else
                is_set = (BM_IS_GAP(blk)) ? bm::gap_test_unr(BMGAP_PTR(blk), nbit) : (blk[nword] & mask0);
            v |= unsigned(bool(is_set)) << i;
        }
    } while(++i < 4);
    return v;
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::optimize(bm::word_t* temp_block,
                  typename bvector_type::optmode opt_mode,
                  typename bvector_type::statistics* st)
{
    if (st)
        st->reset();

    BM_DECLARE_TEMP_BLOCK(tb)
    if (!temp_block)
        temp_block = tb;

    for (unsigned k = 0; k < rsize_; ++k)
    {
        if (bvector_type* bv = get_row(k))
        {
            typename bvector_type::statistics stbv;
            stbv.reset();
            bv->optimize(temp_block, opt_mode, st ? &stbv : 0);
            if (st)
                st->add(stbv);
        }
    } // for k
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::freeze()
{
    for (unsigned k = 0; k < rsize_; ++k)
        if (bvector_type* bv = get_row(k))
            bv->freeze();
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::calc_stat(typename bvector_type::statistics& st,
                                  size_type rsize) const BMNOEXCEPT
{
    for (size_type i = 0; i < rsize; ++i)
        if (const bvector_type* bv = row(i))
        {
            typename bvector_type::statistics stbv;
            bv->calc_stat(&stbv);
            st.add(stbv);
        }
        else
            st.max_serialize_mem += 8;
}

//---------------------------------------------------------------------

template<typename BV>
void basic_bmatrix<BV>::optimize_block(block_idx_type nb,
                                       typename BV::optmode opt_mode)
{
    for (unsigned k = 0; k < rsize_; ++k)
    {
        if (bvector_type* bv = get_row(k))
        {
            unsigned i, j;
            bm::get_block_coord(nb, i, j);
            typename bvector_type::blocks_manager_type& bman =
                                                bv->get_blocks_manager();
            bman.optimize_bit_block(i, j, opt_mode);
        }
    } // for k

}

//---------------------------------------------------------------------
//
//---------------------------------------------------------------------



template<class Val, class BV, unsigned MAX_SIZE>
base_sparse_vector<Val, BV, MAX_SIZE>::base_sparse_vector()
: bmatr_(sv_slices, allocation_policy_type(), bm::id_max, allocator_type())
{}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
base_sparse_vector<Val, BV, MAX_SIZE>::base_sparse_vector(
        bm::null_support        null_able,
        allocation_policy_type  ap,
        size_type               bv_max_size,
        const allocator_type&       alloc)
: bmatr_(sv_slices, ap, bv_max_size, alloc)
{
    if (null_able == bm::use_null)
    {
        size_type null_idx = (MAX_SIZE * sizeof(Val) * 8);
        bmatr_.construct_row(null_idx)->init();
        bmatr_.set_null_idx(null_idx);
        slice_mask_ |= unsigned_value_type(1) << null_idx;
    }
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
base_sparse_vector<Val, BV, MAX_SIZE>::base_sparse_vector(
                        const base_sparse_vector<Val, BV, MAX_SIZE>& bsv)
: bmatr_(bsv.bmatr_),
  slice_mask_(bsv.slice_mask_),
  size_(bsv.size_),
  effective_slices_(bsv.effective_slices_)
{}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::copy_from(
                const base_sparse_vector<Val, BV, MAX_SIZE>& bsv)
{
    resize(bsv.size(), true);
    effective_slices_ = bsv.effective_slices_;

    size_type arg_null_idx = bsv.bmatr_.get_null_idx();
    if (arg_null_idx)
        bmatr_.null_idx_ = arg_null_idx;

    size_type slices = bsv.get_bmatrix().rows(); //stored_slices();
    bmatr_.allocate_rows(slices);
    for (size_type i = 0; i < slices; ++i)
    {
        bvector_type* bv = bmatr_.get_row(i);
        const bvector_type* bv_src = bsv.bmatr_.row(i);
        
        if (i && (i == arg_null_idx)) // NULL plane copy
        {
            if (bv && !bv_src) // special case (copy from not NULL)
            {
                if (size_)
                    bv->set_range(0, size_-1);
                continue;
            }
        }
        if (bv)
        {
            bmatr_.destruct_row(i);
            slice_mask_ &= ~(unsigned_value_type(1) << i);
        }
        if (bv_src)
        {
            bmatr_.construct_row(i, *bv_src);
            slice_mask_ |= (unsigned_value_type(1) << i);
        }
    } // for i
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::merge_matr(
                            bmatrix_type& bmatr)
{
    size_type rows = this->bmatr_.rows();
    const size_type arg_rows = bmatr.rows();
    if (rows < arg_rows)
    {
        rows = arg_rows;
        bmatr_.allocate_rows(rows);
        BM_ASSERT(this->bmatr_.rows() == arg_rows);
    }

    bvector_type* bv_null_arg = 0;
    size_type null_idx = bmatr.get_null_idx();
    if (null_idx)
        bv_null_arg = bmatr.get_row(null_idx);

    if (bvector_type* bv_null = get_null_bvect())
    {
        BM_ASSERT(bv_null_arg);
        bv_null->merge(*bv_null_arg);
    }
    if (rows > arg_rows)
        rows = arg_rows; // min
    for (unsigned j = 0; j < rows; ++j)
    {
        bvector_type* arg_bv = bmatr.get_row(j);
        if (arg_bv && arg_bv != bv_null_arg)
        {
            bvector_type* bv = this->get_create_slice(j);
            slice_mask_ |= (unsigned_value_type(1) << j);
            bv->merge(*arg_bv);
        }
    } // for j
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::swap(
                 base_sparse_vector<Val, BV, MAX_SIZE>& bsv) BMNOEXCEPT
{
    if (this != &bsv)
    {
        bmatr_.swap(bsv.bmatr_);
        bm::xor_swap(slice_mask_, bsv.slice_mask_);
        bm::xor_swap(size_, bsv.size_);
        bm::xor_swap(effective_slices_, bsv.effective_slices_);
    }
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::clear_all(bool free_mem) BMNOEXCEPT
{
    auto slices = bmatr_.rows();
    bvector_type* bv_null = this->get_null_bvect();
    for (size_type i = 0; i < slices; ++i)
        if (bvector_type* bv = this->bmatr_.get_row(i))
            if (bv != bv_null)
                bmatr_.clear_row(i, free_mem);
    slice_mask_ = 0; size_ = 0;
    if (bv_null)
        bv_null->clear(true);
    is_ro_ = false;
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::clear_range(
        typename base_sparse_vector<Val, BV, MAX_SIZE>::size_type left,
        typename base_sparse_vector<Val, BV, MAX_SIZE>::size_type right,
        bool set_null)
{
    if (right < left)
        return clear_range(right, left, set_null);
    auto planes = bmatr_.rows();
    bvector_type* bv_null = this->get_null_bvect();
    for (unsigned i = 0; i < planes; ++i)
    {
        if (bvector_type* bv = this->bmatr_.get_row(i))
            if (bv != bv_null)
                bv->clear_range_no_check(left, right);
    }
    if (set_null && bv_null)
        bv_null->clear_range_no_check(left, right);
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::keep_range_no_check(
                        size_type left, size_type right,
                        bm::null_support slice_null)
{
    if (left)
        this->clear_range(0, left-1, (slice_null == bm::use_null));
    size_type sz = size();
    if (right < sz)
        this->clear_range(right + 1, sz, (slice_null == bm::use_null));
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::resize(size_type sz, bool set_null)
{
    if (sz == size())  // nothing to do
        return;
    if (!sz) // resize to zero is an equivalent of non-destructive deallocation
    {
        clear_all();
        return;
    }
    if (sz < size()) // vector shrink
        clear_range(sz, this->size_, set_null); // clear the tails and NULL
    size_ = sz;
}


//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
bool base_sparse_vector<Val, BV, MAX_SIZE>::is_null(
                                    size_type idx) const BMNOEXCEPT
{
    const bvector_type* bv_null = get_null_bvector();
    return (bv_null) ? (!bv_null->test(idx)) : false;
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::insert_null(size_type idx,
                                                        bool      not_null)
{
    if (bvector_type* bv_null = this->get_null_bvect())
        bv_null->insert(idx, not_null);
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
typename base_sparse_vector<Val, BV, MAX_SIZE>::bvector_type_ptr
base_sparse_vector<Val, BV, MAX_SIZE>::get_create_slice(unsigned i)
{
    bvector_type_ptr bv = bmatr_.construct_row(i);

    // slice mask or efective_slices does not extend beyond the natural size
    // (in bits)    
    if (i < (sizeof(value_type) * 8))
    {
        // note: unsigned int shift by << 32 is UNDEFINED behav.
        slice_mask_ |= (unsigned_value_type(1) << i);
        if (i > effective_slices_)
            effective_slices_ = i;
    }
    return bv;
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
bm::id64_t base_sparse_vector<Val, BV, MAX_SIZE>::get_slice_mask(
                                        unsigned element_idx) const BMNOEXCEPT
{
    bm::id64_t mask = 0;
    bm::id64_t mask1 = 1;
    const unsigned planes = sizeof(value_type) * 8;
    unsigned bidx = 0;
    unsigned slice_size = (element_idx+1) * planes;
    if (slice_size > this->bmatr_.rows())
        slice_size = (unsigned) this->bmatr_.rows();
    for (unsigned i = element_idx * planes; i < slice_size; ++i, ++bidx)
        mask |= get_slice(i) ? (mask1 << bidx) : 0ull;
    return mask;
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::optimize(bm::word_t* temp_block,
                                    typename bvector_type::optmode opt_mode,
                                    typename bvector_type::statistics* st)
{
    typename bvector_type::statistics stbv;
    bmatr_.optimize(temp_block, opt_mode, &stbv);
    if (st)
        st->add(stbv);
    
    bvector_type* bv_null = this->get_null_bvect();
    unsigned slices = (unsigned)this->bmatr_.rows();
    for (unsigned j = 0; j < slices; ++j)
    {
        bvector_type* bv = this->bmatr_.get_row(j);
        if (bv && (bv != bv_null)) // protect the NULL vector from de-allocation
        {
            // TODO: check if this can be done within optimize loop
            if (!bv->any())  // empty vector?
            {
                this->bmatr_.destruct_row(j);
                slice_mask_ &= ~(unsigned_value_type(1) << j);
                continue;
            }
        }
    } // for j
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::calc_stat(
                    typename bvector_type::statistics* st) const BMNOEXCEPT
{
    BM_ASSERT(st);
    st->reset();
    size_type slices = this->get_bmatrix().rows();//stored_slices();
    bmatr_.calc_stat(*st, slices);

    // header accounting
    st->max_serialize_mem += 1 + 1 + 1 + 1 + 8 + (8 * slices);
    st->max_serialize_mem += 1 + 8; // extra header fields for large bit-matrixes
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::clear_value_planes_from(
                                    unsigned plane_idx, size_type idx)
{
    bmatr_.clear_column(idx, plane_idx);
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::insert_clear_value_planes_from(
                                        unsigned plane_idx, size_type idx)
{
    bmatr_.insert_column(idx, plane_idx);
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::erase_column(
                                size_type idx, bool erase_null)
{
    bmatr_.erase_column(idx, erase_null);
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
bool base_sparse_vector<Val, BV, MAX_SIZE>::equal(
            const base_sparse_vector<Val, BV, MAX_SIZE>& sv,
             bm::null_support null_able) const BMNOEXCEPT
{
    if (size_type arg_size = sv.size(); this->size_ != arg_size)
        return false;

    unsigned slices = (unsigned) this->bmatr_.rows();
    unsigned arg_slices = (unsigned) sv.bmatr_.rows();
    unsigned max_slices(slices);
    if (max_slices < arg_slices)
        max_slices = arg_slices;

    const bvector_type* bv_null = this->get_null_bvector();
    const bvector_type* bv_null_arg = sv.get_null_bvector();

    for (unsigned j = 0; j < max_slices; ++j)
    {
        const bvector_type* bv;
        const bvector_type* arg_bv;

        bv = (j < slices) ? this->bmatr_.get_row(j) : 0;
        arg_bv = (j < arg_slices) ? sv.bmatr_.get_row(j) : 0;
        if (bv == bv_null)
            bv = 0; // NULL vector compare postponed for later
        if (arg_bv == bv_null_arg)
            arg_bv = 0;
        if (bv == arg_bv) // same NULL
            continue;

        // check if any not NULL and not empty
        if (!bv && arg_bv)
        {
            if (arg_bv->any())
                return false;
            continue;
        }
        if (bv && !arg_bv)
        {
            if (bv->any())
                return false;
            continue;
        }
        // both not NULL
        bool eq = bv->equal(*arg_bv);
        if (!eq)
            return false;
    } // for j
    
    if (null_able == bm::use_null)
    {
        // check the NULL vectors
        if (bv_null == bv_null_arg)
            return true;
        if (!bv_null || !bv_null_arg)
            return false; // TODO: this may need an improvement when one is null, others not null

        BM_ASSERT(bv_null);
        BM_ASSERT(bv_null_arg);
        bool eq = bv_null->equal(*bv_null_arg);
        if (!eq)
            return false;
    }
    return true;
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::sync_ro() BMNOEXCEPT
{
    unsigned slices = (unsigned) this->bmatr_.rows();
    for (unsigned j = 0; j < slices; ++j)
    {
        if (const bvector_type* bv = this->bmatr_.get_row(j))
        {
            if (bv->is_ro())
            {
                is_ro_ = true;
                break;
            }
        }
    } // for j
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
void base_sparse_vector<Val, BV, MAX_SIZE>::copy_range_slices(
        const base_sparse_vector<Val, BV, MAX_SIZE>& bsv,
        typename base_sparse_vector<Val, BV, MAX_SIZE>::size_type left,
        typename base_sparse_vector<Val, BV, MAX_SIZE>::size_type right,
        bm::null_support slice_null)
{
    if (bmatr_.rows() < bsv.bmatr_.rows())
        bmatr_.allocate_rows(bsv.bmatr_.rows());

    size_type spli;
    const bvector_type* bv_null_arg = bsv.get_null_bvector();
    if (bvector_type* bv_null = get_null_bvect())
    {
        spli = this->bmatr_.rows(); //stored_slices();
        if (bv_null_arg && (slice_null == bm::use_null))
            bv_null->copy_range(*bv_null_arg, left, right);
        --spli;
    }
    else
        spli = this->bmatr_.rows();

    for (size_type j = 0; j < spli; ++j)
    {
        if (const bvector_type* arg_bv = bsv.bmatr_.row(j))
        {
            if (arg_bv == bv_null_arg)
                continue;
            bvector_type* bv = this->get_create_slice(unsigned(j));
            bv->copy_range(*arg_bv, left, right);
        }
    } // for j

}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
typename base_sparse_vector<Val, BV, MAX_SIZE>::unsigned_value_type
base_sparse_vector<Val, BV, MAX_SIZE>::s2u(value_type v) BMNOEXCEPT
{
    if constexpr (is_signed())
    {
        if (v < 0)
        {
            // the +1 trick is to get abs of INT_MIN without overflowing
            // https://stackoverflow.com/questions/22268815/absolute-value-of-int-min
            value_type uv = -(v+1);
            return 1u | unsigned_value_type((unsigned_value_type(uv) << 1u));
        }
        return unsigned_value_type(unsigned_value_type(v) << 1u);
    }
    else
        return v;
}

//---------------------------------------------------------------------

template<class Val, class BV, unsigned MAX_SIZE>
typename base_sparse_vector<Val, BV, MAX_SIZE>::value_type
base_sparse_vector<Val, BV, MAX_SIZE>::u2s(unsigned_value_type uv) BMNOEXCEPT
{
    if constexpr (is_signed())
    {
        if (uv & 1u) // signed
        {
            value_type s = (-(uv >> 1u)) - 1;
            return s;
        }
        return value_type(uv >> 1u);
    }
    else
        return uv;
}
#ifdef _MSC_VER
#pragma warning( pop )
#endif


} // namespace

#endif
