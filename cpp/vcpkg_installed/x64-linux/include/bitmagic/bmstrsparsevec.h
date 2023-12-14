#ifndef BMSTRSPARSEVEC__H__INCLUDED__
#define BMSTRSPARSEVEC__H__INCLUDED__
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

/*! \file bmstrsparsevec.h
    \brief string sparse vector based on bit-transposed matrix
*/

#include <stddef.h>
#include "bmconst.h"

#ifndef BM_NO_STL
#include <stdexcept>
#include <string_view>
#endif

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration 
// #include "bm.h" or "bm64.h" explicitly 
# error missing include (bm.h or bm64.h)
#endif

#include "bmtrans.h"
#include "bmalgo.h"
#include "bmbuffer.h"
#include "bmbmatrix.h"
#include "bmdef.h"

namespace bm
{

enum class remap_setup
{
    COPY_RTABLES,   //!<  copy remap tables only (without data)
};


/*!
   \brief succinct sparse vector for strings with compression using bit-slicing ( transposition) method
 
   Initial string is bit-transposed into bit-slices so collection may use less
   memory due to prefix sum (GAP) compression in bit-slices. In addition, the container
   can use chracter re-mapping using char freaquencies to compute the minimal codes.
   Re-mapping can reduce memory footprint, get better search performance and improve storage
   compression.

   Template parameters:
           CharType - type of character (char or unsigned char) (wchar not tested)
           BV - bit-vector for bit-slicing
           STR_SIZE - initial string size (can dynamically increase on usage)
 
   @ingroup sv
*/
template<typename CharType, typename BV, unsigned STR_SIZE>
class str_sparse_vector : public base_sparse_vector<CharType, BV, STR_SIZE>
{
public:
    typedef BV                                       bvector_type;
    typedef bvector_type*                            bvector_type_ptr;
    typedef const bvector_type*                      bvector_type_const_ptr;
    typedef CharType                                 value_type;
    typedef CharType*                                value_type_prt;
    typedef typename bvector_type::size_type         size_type;
    typedef typename BV::allocator_type              allocator_type;
    typedef typename bvector_type::allocation_policy allocation_policy_type;
    typedef typename bvector_type::enumerator        bvector_enumerator_type;
    typedef typename allocator_type::allocator_pool_type allocator_pool_type;
    typedef bm::basic_bmatrix<BV>                    bmatrix_type;
    typedef base_sparse_vector<CharType, BV, STR_SIZE> parent_type;
    typedef typename parent_type::unsigned_value_type unsigned_value_type;

    /*! Statistical information about  memory allocation details. */
    struct statistics : public bv_statistics
    {};
    
    enum octet_slices
    {
        sv_octet_slices = STR_SIZE
    };

    /** Matrix of character remappings
        @internal
     */
    typedef bm::dynamic_heap_matrix<unsigned char, allocator_type>
                                            slice_octet_matrix_type;
    typedef slice_octet_matrix_type remap_matrix_type;

    /** Matrix of character frequencies (for optimal code remap)
        @internal
     */
    typedef
    bm::dynamic_heap_matrix<size_t, allocator_type> octet_freq_matrix_type;

    struct is_remap_support { enum trait { value = true }; };
    struct is_rsc_support { enum trait { value = false }; };
    struct is_dynamic_splices { enum trait { value = true }; };

    class reference_base
    {
    public:
        typedef
        bm::heap_vector<CharType, typename bvector_type::allocator_type, true>
                                                                 bufffer_type;
    protected:
        mutable bufffer_type buf_;
    };

    /**
         Reference class to access elements via common [] operator
         @ingroup sv
    */
    class const_reference : protected reference_base
    {
    public:
        const_reference(
                const str_sparse_vector<CharType, BV, STR_SIZE>& str_sv,
                size_type idx)
        : str_sv_(str_sv), idx_(idx)
        {
            this->buf_.resize(str_sv.effective_max_str());
        }
        
        operator const value_type*() const BMNOEXCEPT
        {
            return get();
        }

        const value_type* get() const BMNOEXCEPT
        {
            str_sv_.get(idx_, this->buf_.data(), str_sv_.effective_max_str());
            return this->buf_.data();
        }

        bool operator==(const const_reference& ref) const BMNOEXCEPT
                                { return bool(*this) == bool(ref); }
        bool is_null() const BMNOEXCEPT { return str_sv_.is_null(idx_); }
    private:
        const str_sparse_vector<CharType, BV, STR_SIZE>& str_sv_;
        size_type                                            idx_;
    };

    /**
         Reference class to access elements via common [] operator
         @ingroup sv
    */
    class reference : protected reference_base
    {
    public:
        reference(str_sparse_vector<CharType, BV, STR_SIZE>& str_sv,
                  size_type idx)
        : str_sv_(str_sv), idx_(idx)
        {
            this->buf_.resize(str_sv.effective_max_str());
        }

        operator const value_type*() const BMNOEXCEPT
        {
            return get();
        }

        const value_type* get() const BMNOEXCEPT
        {
            str_sv_.get(idx_, this->buf_.data(), str_sv_.effective_max_str());
            return this->buf_.data();
        }

        reference& operator=(const reference& ref)
        {
            // TO DO: implement element copy bit by bit
            str_sv_.set(idx_, (const value_type*)ref);
            return *this;
        }

        reference& operator=(const value_type* str)
        {
            str_sv_.set(idx_, str);
            return *this;
        }
        bool operator==(const reference& ref) const BMNOEXCEPT
                                { return bool(*this) == bool(ref); }
        bool is_null() const BMNOEXCEPT { return str_sv_.is_null(idx_); }
    private:
        str_sparse_vector<CharType, BV, STR_SIZE>& str_sv_;
        size_type                                  idx_;
    };

    /**
        Const iterator to do quick traverse of the sparse vector.
     
        Implementation uses buffer for decoding so, competing changes
        to the original vector may not match the iterator returned values.
     
        This iterator keeps an operational buffer of decoded elements, so memory footprint is NOT negligable.
     
        @ingroup sv
    */
    class const_iterator
    {
    public:
    friend class str_sparse_vector;
#ifndef BM_NO_STL
        typedef std::input_iterator_tag            iterator_category;
        typedef std::basic_string_view<CharType>   string_view_type;
#endif
        typedef str_sparse_vector<CharType, BV, STR_SIZE>     str_sparse_vector_type;
        typedef str_sparse_vector_type*                       str_sparse_vector_type_ptr;
        typedef typename str_sparse_vector_type::value_type   value_type;
        typedef typename str_sparse_vector_type::size_type    size_type;
        typedef typename str_sparse_vector_type::bvector_type bvector_type;
        typedef typename bvector_type::allocator_type         allocator_type;
        typedef typename allocator_type::allocator_pool_type  allocator_pool_type;
        
        typedef long long                   difference_type;
        typedef CharType*                   pointer;
        typedef CharType*&                  reference;
    public:
        /**
            Construct iterator (not attached to any particular vector)
         */
        const_iterator() BMNOEXCEPT;
        /**
            Construct iterator (attached to sparse vector)
            @param sv - pointer to sparse vector
         */
        const_iterator(const str_sparse_vector_type* sv) BMNOEXCEPT;
        /**
            Construct iterator (attached to sparse vector) and positioned
            @param sv - reference to sparse vector
            @param pos - position in the vector to start
         */
        const_iterator(const str_sparse_vector_type* sv, size_type pos) BMNOEXCEPT;

        const_iterator(const const_iterator& it) BMNOEXCEPT;

        /**
            setup iterator to retrieve a sub-string of a string
            @param from - Position of the first character to be copied
            @param len - length of a substring (defult: 0 read to the available end)
         */
        void set_substr(unsigned from, unsigned len = 0) BMNOEXCEPT;

        
        bool operator==(const const_iterator& it) const BMNOEXCEPT
                                { return (pos_ == it.pos_) && (sv_ == it.sv_); }
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
        const value_type* operator*() const
            { return this->value(); }

        /// \brief Advance to the next available value
        const_iterator& operator++() BMNOEXCEPT
            { this->advance(); return *this; }

        /// \brief Advance to the next available value
        const_iterator& operator++(int) BMNOEXCEPT
            { const_iterator tmp(*this);this->advance(); return tmp; }


        /// \brief Get zero terminated string value at the current position
        const value_type* value() const;

        /// \brief Get current string as string_view
        string_view_type get_string_view() const;

        /// \brief Get NULL status
        bool is_null() const BMNOEXCEPT { return sv_->is_null(this->pos_); }

        /// Returns true if iterator is at a valid position
        bool valid() const BMNOEXCEPT { return pos_ != bm::id_max; }

        /// Invalidate current iterator
        void invalidate() BMNOEXCEPT { pos_ = bm::id_max; }

        /// Current position (index) in the vector
        size_type pos() const BMNOEXCEPT { return pos_; }

        /// re-position to a specified position
        void go_to(size_type pos) BMNOEXCEPT;

        /// advance iterator forward by one
        void advance() BMNOEXCEPT;

    protected:
        enum buf_size_e
        {
            n_rows = 1024
        };
        typedef dynamic_heap_matrix<CharType, allocator_type> buffer_matrix_type;

    private:
        const str_sparse_vector_type*     sv_;      ///!< ptr to parent
        unsigned                          substr_from_; ///!< substring from
        unsigned                          substr_to_;   ///!< substring to
        mutable size_type                 pos_;         ///!< Position
        mutable buffer_matrix_type        buf_matrix_;  ///!< decode value buffer
        mutable size_type                 pos_in_buf_;  ///!< buffer position
    };


    /**
        Back insert iterator implements buffered insert, faster than generic
        access assignment.
     
        Limitations for buffered inserter:
        1. Do not use more than one inserter (into one vector) at the same time
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
        typedef str_sparse_vector<CharType, BV, STR_SIZE>      str_sparse_vector_type;
        typedef str_sparse_vector_type*                        str_sparse_vector_type_ptr;
        typedef typename str_sparse_vector_type::value_type    value_type;
        typedef typename str_sparse_vector_type::size_type     size_type;
        typedef typename str_sparse_vector_type::bvector_type  bvector_type;
        typedef typename bvector_type::allocator_type          allocator_type;
        typedef typename allocator_type::allocator_pool_type   allocator_pool_type;

        typedef void difference_type;
        typedef void pointer;
        typedef void reference;
        
    public:
        back_insert_iterator() BMNOEXCEPT;
        back_insert_iterator(str_sparse_vector_type* sv) BMNOEXCEPT;
        back_insert_iterator(const back_insert_iterator& bi) BMNOEXCEPT;
        
        back_insert_iterator& operator=(const back_insert_iterator& bi)
        {
            BM_ASSERT(bi.empty());
            buf_matrix_.init_resize(
                bi.buf_matrix_.rows(), bi.buf_matrix_.cols());
            this->flush_impl(); sv_ = bi.sv_;
            return *this;
        }

        ~back_insert_iterator();

        /**
            Set optimization on load option (deafult: false)
         */
        void set_optimize(typename bvector_type::optmode opt_mode) BMNOEXCEPT
            { opt_mode_ = opt_mode; }

        /**
            Method to configure back inserter to collect statistics on optimal character codes.
            This methos makes back inserter slower, but can be used to accelerate later remap() of
            the sparse vector. Use flush at the end to apply the remapping.
            By default inserter does not collect additional statistics.

            Important! You should NOT use intermediate flush if you set remapping!

            @sa flush
         */
        void set_remap(bool flag) BMNOEXCEPT { remap_flags_ = flag; }

        /// Get curent remap state flags
        unsigned get_remap() const BMNOEXCEPT { return remap_flags_; }
        
        /** push value to the vector */
        back_insert_iterator& operator=(const value_type* v)
            { this->add(v); return *this; }


        /** push value to the vector */
        template<typename StrType>
        back_insert_iterator& operator=(const StrType& v)
        {
            this->add(v.c_str()); return *this; // TODO: avoid c_str()
        }

        /** noop */
        back_insert_iterator& operator*() { return *this; }
        /** noop */
        back_insert_iterator& operator++() { return *this; }
        /** noop */
        back_insert_iterator& operator++( int ) { return *this; }
        
        /** add value to the container*/
        void add(const value_type* v);
        
        /** add NULL (no-value) to the container */
        void add_null();
        
        /** add a series of consequitve NULLs (no-value) to the container */
        void add_null(size_type count);

        /** flush the accumulated buffer. It is important to call flush at the end, before destruction of the
           inserter. Flush may throw exceptions, which will be possible to intercept.
           Otherwise inserter is flushed in the destructor.
        */
        void flush();

        // access to internals
        //

        /// Get octet frequence matrix
        ///  @internal
        const octet_freq_matrix_type& get_octet_matrix() const noexcept
            { return omatrix_; }

    protected:
        /** return true if insertion buffer is empty */
        bool empty() const BMNOEXCEPT;

        typedef typename bvector_type::block_idx_type     block_idx_type;

        /** add value to the buffer without changing the NULL vector
            @param v - value to push back
            @internal
        */
        void add_value(const value_type* v);

        /**
            account new value as remap statistics
         */
        void add_remap_stat(const value_type* v);

        void flush_impl();

    private:
        enum buf_size_e
        {
            n_buf_size = str_sparse_vector_type::ins_buf_size // 1024 * 8
        };
        typedef bm::dynamic_heap_matrix<CharType, allocator_type> buffer_matrix_type;
        friend class str_sparse_vector;

    protected:
        str_sparse_vector_type*  sv_;         ///!< pointer on the parent vector
        bvector_type*            bv_null_;    ///!< not NULL vector pointer
        buffer_matrix_type       buf_matrix_; ///!< value buffer
        size_type                pos_in_buf_; ///!< buffer position
        block_idx_type           prev_nb_ = 0;///!< previous block added
        typename
        bvector_type::optmode    opt_mode_ = bvector_type::opt_compress;
        ///
        unsigned                 remap_flags_ = 0; ///< target remapping
        octet_freq_matrix_type   omatrix_; ///< octet frequency matrix
    };


public:

    /*!
        \brief Sparse vector constructor
     
        \param null_able - defines if vector supports NULL values flag
            by default it is OFF, use bm::use_null to enable it
        \param ap - allocation strategy for underlying bit-vectors
        Default allocation policy uses BM_BIT setting (fastest access)
        \param bv_max_size - maximum possible size of underlying bit-vectors
        Please note, this is NOT size of svector itself, it is dynamic upper limit
        which should be used very carefully if we surely know the ultimate size
        \param alloc - allocator for bit-vectors
     
        \sa bvector<>
        \sa bm::bvector<>::allocation_policy
        \sa bm::startegy
    */
    str_sparse_vector(bm::null_support null_able = bm::no_null,
                      allocation_policy_type ap = allocation_policy_type(),
                      size_type bv_max_size = bm::id_max,
                      const allocator_type&   alloc  = allocator_type());

    /*! copy-ctor */
    str_sparse_vector(const str_sparse_vector& str_sv);

    /*! construct empty sparse vector, copying the remap tables from another vector
        \param str_sv - source vector to take the remap tables from (assumed to be remaped)
        \param remap_mode - remap table copy param
    */
    str_sparse_vector(const str_sparse_vector& str_sv, bm::remap_setup remap_mode);


    /*! copy assignmment operator */
    str_sparse_vector<CharType, BV, STR_SIZE>& operator = (
                const str_sparse_vector<CharType, BV, STR_SIZE>& str_sv)
    {
        if (this != &str_sv)
            parent_type::copy_from(str_sv);
        remap_flags_ = str_sv.remap_flags_;
        remap_matrix1_ = str_sv.remap_matrix1_;
        remap_matrix2_ = str_sv.remap_matrix2_;
        return *this;
    }
#ifndef BM_NO_CXX11
    /*! move-ctor */
    str_sparse_vector(str_sparse_vector<CharType, BV, STR_SIZE>&& str_sv) BMNOEXCEPT
    {
        parent_type::swap(str_sv);
        remap_flags_ = str_sv.remap_flags_;
        remap_matrix1_.swap(str_sv.remap_matrix1_);
        remap_matrix2_.swap(str_sv.remap_matrix2_);
    }

    /*! move assignmment operator */
    str_sparse_vector<CharType, BV, STR_SIZE>& operator =
            (str_sparse_vector<CharType, BV, STR_SIZE>&& str_sv) BMNOEXCEPT
    {
        if (this != &str_sv)
            this->swap(str_sv);
        return *this;
    }
#endif

public:

    // ------------------------------------------------------------
    /*! @name String element access */
    ///@{

    /** \brief Operator to get read access to an element  */
    const const_reference operator[](size_type idx) const
                                { return const_reference(*this, idx); }

    /** \brief Operator to get write access to an element  */
    reference operator[](size_type idx) { return reference(*this, idx); }

    /*!
        \brief set specified element with bounds checking and automatic resize
        \param idx  - element index (vector auto-resized if needs to)
        \param str  - string to set (zero terminated)
    */
    void set(size_type idx, const value_type* str);

    /*!
        \brief set NULL status for the specified element
        Vector is resized automatically
        \param idx  - element index (vector auto-resized if needs to)
    */
    void set_null(size_type idx);

    /**
        Set NULL all elements set as 1 in the argument vector
        \param bv_idx - index bit-vector for elements which needs to be turned to NULL
     */
    void set_null(const bvector_type& bv_idx)
        { this->bit_sub_rows(bv_idx, true); }

    /**
        Set vector elements spcified by argument bit-vector to empty
        Note that set to empty elements are NOT going to tuned to NULL (NULL qualifier is preserved)
        \param bv_idx - index bit-vector for elements which  to be set to 0
     */
    void clear(const bvector_type& bv_idx)
        { this->bit_sub_rows(bv_idx, false); }


    /**
        Set NULL all elements NOT set as 1 in the argument vector
        \param bv_idx - index bit-vector for elements which needs to be kept
     */
    void keep(const bvector_type& bv_idx) { this->bit_and_rows(bv_idx); }

    
    /*!
        \brief insert the specified element
        \param idx  - element index (vector auto-resized if needs to)
        \param str  - string to set (zero terminated)
    */
    void insert(size_type idx, const value_type* str);


    /*!
        \brief insert STL string
        \param idx  - element index (vector auto-resized if needs to)
        \param str  - STL string to set
    */
    template<typename StrType>
    void insert(size_type idx, const StrType& str)
    {
        this->insert(idx, str.c_str()); // TODO: avoid c_str()
    }

    /*!
        \brief erase the specified element
        \param idx  - element index
    */
    void erase(size_type idx);

    /*!
        \brief get specified element
     
        \param idx  - element index
        \param str  - string buffer
        \param buf_size - string buffer size
     
        @return string length
    */
    size_type get(size_type idx,
                 value_type* str, size_type buf_size) const BMNOEXCEPT;
    
    /*!
        \brief set specified element with bounds checking and automatic resize
     
        This is an equivalent of set() method, but templetized to be
        more compatible with the STL std::string and the likes
     
        \param idx  - element index (vector auto-resized if needs to)
        \param str  - input string
                      expected an STL class with size() support,
                      like basic_string<> or vector<char>
    */
    template<typename StrType>
    void assign(size_type idx, const StrType& str)
    {
        if (idx >= this->size())
            this->size_ = idx+1;

        size_type str_size = size_type(str.size());
        if (!str_size)
        {
            this->clear_value_planes_from(0, idx);
            return;
        }
        for (size_type i=0; i < str_size; ++i)
        {
            CharType ch = str[i];
            if (remap_flags_) // compressional re-mapping is in effect
            {
                unsigned char remap_value = remap_matrix2_.get(i, unsigned(ch));
                BM_ASSERT(remap_value);
                ch = CharType(remap_value);
            }
            this->bmatr_.set_octet(idx, i, (unsigned char)ch);
            if (!ch)
                break;
        } // for i
        this->clear_value_planes_from(unsigned(str_size*8), idx);
        if (bvector_type* bv_null = this->get_null_bvect())
            bv_null->set_bit_no_check(idx);
    }
    
    /*!
        \brief push back a string
        \param str  - string to set
                    (STL class with size() support, like basic_string)
    */
    template<typename StrType>
    void push_back(const StrType& str) { assign(this->size_, str); }
    
    /*!
        \brief push back a string (zero terminated)
        \param str  - string to set
    */
    void push_back(const value_type* str) { set(this->size_, str); }

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
        \brief get specified string element if NOT NULL
        Template method expects an STL-compatible type basic_string<>
        \param idx  - element index (vector auto-resized if needs to)
        \param str  - string to get [out]
        \return true if element is not null and try-get successfull
    */
    template<typename StrType>
    bool try_get(size_type idx, StrType& str) const
    {
        if (this->is_null(idx))
            return false;
        get(idx, str);
        return true;
    }


    /*!
        \brief get specified string element
        Template method expects an STL-compatible type basic_string<>
        \param idx  - element index (vector auto-resized if needs to)
        \param str  - string to get [out]
    */
    template<typename StrType>
    void get(size_type idx, StrType& str) const
    {
        str.clear();
        for (unsigned i = 0; true; ++i)
        {
            CharType ch = CharType(this->bmatr_.get_octet(idx, i));
            if (!ch)
                break;
            if (remap_flags_)
            {
                const unsigned char* remap_row = remap_matrix1_.row(i);
                unsigned char remap_value = remap_row[unsigned(ch)];
                BM_ASSERT(remap_value);
                if (!remap_value) // unknown dictionary element
                {
                    throw_bad_value(0);
                    break;
                }
                ch = CharType(remap_value);
            }
            str.push_back(ch);
        } // for i
    }

    /*! Swap content */
    void swap(str_sparse_vector& str_sv) BMNOEXCEPT;

    ///@}
    
    // ------------------------------------------------------------
    /*! @name Element comparison functions       */
    ///@{

    /**
        \brief Compare vector element with argument lexicographically

        The function does not account for NULL values, NULL element is treated as an empty string
     
        NOTE: for a re-mapped container, input string may have no correct
        remapping, in this case we have an ambiguity
        (we know it is not equal (0) but LT or GT?).
        Behavior is undefined.
     
        \param idx - vactor element index
        \param str - argument to compare with
     
        \return 0 - equal,  < 0 - vect[idx] < str,  >0 otherwise
    */
    int compare(size_type idx, const value_type* str) const BMNOEXCEPT;

    static
    int compare_str(const value_type* str1, const value_type* str2) BMNOEXCEPT;

    static
    int compare_str(const value_type* str1, const value_type* str2, size_t min_len) BMNOEXCEPT;

    /**
        \brief Compare two vector elements

        The function does not account for NULL values, NULL element is treated as an empty string
        \param idx1 - vactor element index 1
        \param idx2 - vactor element index 2

        \return 0 - equal,  < 0 - vect[idx1] < vect[idx2],  >0 otherwise
    */
    int compare(size_type idx1, size_type idx2) const BMNOEXCEPT;

    
    /**
        \brief Find size of common prefix between two vector elements in octets
        @param prefix_buf - optional param for keeping the common prefix string (without remap decode)
        \return size of common prefix
    */
    template<bool USE_PREFIX_BUF = false>
    unsigned common_prefix_length(size_type idx1, size_type idx2,
                                  value_type* prefix_buf=0) const BMNOEXCEPT;

    /**
        Variant of compare for remapped vectors. Caller MUST guarantee vector is remapped.
     */
    int compare_remap(size_type idx, const value_type* str) const BMNOEXCEPT;

    /**
        Variant of compare for non-mapped vectors. Caller MUST guarantee vector is not remapped.
     */
    int compare_nomap(size_type idx, const value_type* str) const BMNOEXCEPT;

    ///@}


    // ------------------------------------------------------------
    /*! @name Clear                                              */
    ///@{

    /*! \brief resize to zero, free memory */
    void clear_all(bool free_mem) BMNOEXCEPT;

    /*! \brief resize to zero, free memory */
    void clear() BMNOEXCEPT { clear_all(true); }

    /*!
        \brief clear range (assign bit 0 for all planes)
        \param left  - interval start
        \param right - interval end (closed interval)
        \param set_null - set cleared values to unassigned (NULL)
    */
    str_sparse_vector<CharType, BV, STR_SIZE>&
        clear_range(size_type left, size_type right, bool set_null = false)
    {
        parent_type::clear_range(left, right, set_null);
        return *this;
    }


    ///@}

    
    // ------------------------------------------------------------
    /*! @name Size, etc       */
    ///@{

    /*! \brief return size of the vector
        \return size of sparse vector
    */
    size_type size() const { return this->size_; }
    
    /*! \brief return true if vector is empty
        \return true if empty
    */
    bool empty() const { return (size() == 0); }
    
    /*! \brief resize vector
        \param sz - new size
    */
    void resize(size_type sz) { parent_type::resize(sz, true); }
    
    /*! \brief get maximum string length capacity
        \return maximum string length sparse vector can take
    */
    static size_type max_str() { return sv_octet_slices; }
    
    /*! \brief get effective string length used in vector
        Calculate and returns efficiency, how close are we
        to the reserved maximum.
        \return current string length maximum
    */
    size_type effective_max_str() const BMNOEXCEPT;
    
    /*! \brief get effective string length used in vector
        \return current string length maximum
    */
    size_type effective_vector_max() const { return effective_max_str(); }

    /**
        \brief recalculate size to exclude tail NULL elements
        After this call size() will return the true size of the vector
     */
    void sync_size() BMNOEXCEPT;

    ///@}


    // ------------------------------------------------------------
    /*! @name Memory optimization/compression                    */
    ///@{

    /*!
        \brief run memory optimization for all vector planes
        \param temp_block - pre-allocated memory block to avoid unnecessary re-allocs
        \param opt_mode - requested compression depth
        \param stat - memory allocation statistics after optimization
    */
    void optimize(
       bm::word_t* temp_block = 0,
       typename bvector_type::optmode opt_mode = bvector_type::opt_compress,
       typename str_sparse_vector<CharType, BV, STR_SIZE>::statistics* stat = 0);

    /*!
        @brief Calculates memory statistics.

        Function fills statistics structure containing information about how
        this vector uses memory and estimation of max. amount of memory
        bvector needs to serialize itself.

        @param st - pointer on statistics structure to be filled in.

        @sa statistics
    */
    void calc_stat(
        struct str_sparse_vector<CharType, BV, STR_SIZE>::statistics* st
        ) const BMNOEXCEPT;
    
    /**
        @brief Turn sparse vector into immutable mode
        Read-only (immutable) vector uses less memory and allows faster searches.
        Before freezing it is recommenede to call optimize() to get full memory saving effect
        @sa optimize, remap
     */
    void freeze() { this->freeze_matr(); }

    /** Returns true if vector is read-only */
    bool is_ro() const BMNOEXCEPT { return this->is_ro_; }

    ///@}

    // ------------------------------------------------------------
    /*! @name Iterator access */
    ///@{

    /** Provide const iterator access to container content  */
    const_iterator begin() const BMNOEXCEPT;

    /** Provide const iterator access to the end    */
    const_iterator end() const BMNOEXCEPT { return const_iterator(this, bm::id_max); }

    /** Get const_itertor re-positioned to specific element
    @param idx - position in the sparse vector
    */
    const_iterator get_const_iterator(size_type idx) const BMNOEXCEPT
        { return const_iterator(this, idx); }
    
     /** Provide back insert iterator
    Back insert iterator implements buffered insertion, which is faster, than random access
    or push_back
    */
    back_insert_iterator get_back_inserter()
        { return back_insert_iterator(this); }

    ///@}



    // ------------------------------------------------------------
    /*! @name Various traits                                     */
    ///@{
    
    /** \brief various type traits
    */
    static constexpr
    bool is_compressed() BMNOEXCEPT { return false; }

    static constexpr
    bool is_str() BMNOEXCEPT { return true; }

    ///@}

    // ------------------------------------------------------------
    /*! @name Char remapping, succinct utilities

        Remapping runs character usage analysis (frequency analysis)
        based on that implements reduction of dit-depth thus improves
        search performance and memory usage (both RAM and serialized).

        Remapping limits farther modifications of sparse vector.
        (Use remapped vector as read-only).
    */

    ///@{
    
    /**
        Get character remapping status (true | false)
    */
    bool is_remap() const BMNOEXCEPT { return remap_flags_ != 0; }
    
    /**
        Build remapping profile and load content from another sparse vector
        Remapped vector likely saves memory (both RAM and disk) but
        should not be modified (should be read-only).

        \param str_sv - source sparse vector (assumed it is not remapped)
        \param omatrix - pointer to externall computed char freaquency matrix (optional)
        \so remap, freeze
    */
    void remap_from(const str_sparse_vector& str_sv,
                    octet_freq_matrix_type* omatrix = 0);

    /**
        Build remapping profile and re-load content to save memory
    */
    void remap();

    /*!
        Calculate flags which octets are present on each byte-plane.
        @internal
    */
    void calc_octet_stat(octet_freq_matrix_type& octet_matrix) const;

    /*!
        Compute optimal remap codes
        @internal
    */
    void build_octet_remap(
                slice_octet_matrix_type& octet_remap_matrix1,
                slice_octet_matrix_type& octet_remap_matrix2,
                octet_freq_matrix_type& octet_occupancy_matrix) const;
    /*!
        remap string from external (ASCII) system to matrix internal code
        @return true if remapping was ok, false if found incorrect value
                for the plane
        @internal
    */
    static
    bool remap_tosv(value_type*  BMRESTRICT      sv_str,
                    size_type                    buf_size,
                    const value_type* BMRESTRICT str,
                    const slice_octet_matrix_type& BMRESTRICT octet_remap_matrix2
                    ) BMNOEXCEPT;
    /*!
        remap string from external (ASCII) system to matrix internal code
        also creates a zero terminated copy string
        @return true if remapping was ok, false if found incorrect value
                for the plane
        @internal
    */
    static
    bool remap_n_tosv_2way(
       value_type*   BMRESTRICT     sv_str,
       value_type*   BMRESTRICT     str_cp,
       size_type                    buf_size,
       const value_type* BMRESTRICT str,
       size_t                       in_len,
       const slice_octet_matrix_type& BMRESTRICT octet_remap_matrix2) BMNOEXCEPT;

    /*!
        remap string from external (ASCII) system to matrix internal code
        @internal
    */
    bool remap_tosv(value_type*       sv_str,
                    size_type         buf_size,
                    const value_type* str) const BMNOEXCEPT
    {
        return remap_tosv(sv_str, buf_size, str, remap_matrix2_);
    }

    /*!
        remap string from external (ASCII) system to matrix internal code
        @internal
    */
    bool remap_n_tosv_2way(
                   value_type*   BMRESTRICT     sv_str,
                   value_type*   BMRESTRICT     str_cp,
                   size_type                    buf_size,
                   const value_type* BMRESTRICT str,
                   size_t                       in_len) const BMNOEXCEPT
    {
        return remap_n_tosv_2way(
                    sv_str, str_cp, buf_size, str, in_len, remap_matrix2_);
    }

    /*!
        remap string from internal code to external (ASCII) system
        @return true if remapping was ok, false if found incorrect value
                for the plane
        @internal
    */
    static
    bool remap_fromsv(
            value_type*   BMRESTRICT     str,
            size_type                    buf_size,
            const value_type* BMRESTRICT sv_str,
            const slice_octet_matrix_type& BMRESTRICT octet_remap_matrix1
            ) BMNOEXCEPT;
    /*!
        re-calculate remap matrix2 based on matrix1
        @internal
    */
    void recalc_remap_matrix2();

    ///@}
    
    // ------------------------------------------------------------
    /*! @name Export content to C-style                          */
    ///@{

    /**
        \brief Bulk export strings to a C-style matrix of chars

        \param cmatr  - dest matrix (bm::heap_matrix)
        \param idx_from - index in the sparse vector to export from
        \param dec_size - decoding size (matrix column allocation should match)
        \param zero_mem - set to false if target array is pre-initialized
                          with 0s to avoid performance penalty

        \return number of actually exported elements (can be less than requested)
    */
    template<typename CharMatrix>
    size_type decode(CharMatrix& cmatr,
                     size_type   idx_from,
                     size_type   dec_size,
                     bool        zero_mem = true) const
    {
        size_type str_len = effective_max_str();
        return decode_substr(cmatr, idx_from, dec_size,
                             0, unsigned(str_len-1), zero_mem);
    }

    /**
        \brief Bulk export strings to a C-style matrix of chars
     
        \param cmatr  - dest matrix (bm::heap_matrix)
        \param idx_from - index in the sparse vector to export from
        \param dec_size - decoding size (matrix column allocation should match)
        \param substr_from - sub-string position from
        \param substr_to - sub-string position to
        \param zero_mem - set to false if target array is pre-initialized
                          with 0s to avoid performance penalty
     
        \return number of actually exported elements (can be less than requested)
    */
    template<typename CharMatrix>
    size_type decode_substr(CharMatrix& cmatr,
                     size_type   idx_from,
                     size_type   dec_size,
                     unsigned    substr_from,
                     unsigned    substr_to,
                     bool        zero_mem = true) const
    {

    /// Decoder functor
    /// @internal
    ///
    struct sv_decode_visitor_func
    {
        sv_decode_visitor_func(CharMatrix& cmatr) BMNOEXCEPT2
            : cmatr_(cmatr)
        {}

        int add_bits(size_type bv_offset,
                     const unsigned char* BMRESTRICT bits,
                     unsigned bits_size) BMNOEXCEPT
        {
            BM_ASSERT(bits_size);

            // can be negative (-1) when bv base offset = 0 and sv = 1,2..
            size_type base = bv_offset - sv_off_;
            const unsigned_value_type m = mask_;
            const unsigned i = substr_i_;
            unsigned j = 0;
            do
            {
                size_type idx = bits[j] + base;
                value_type* BMRESTRICT str = cmatr_.row(idx);
                str[i] |= m;
            } while (++j < bits_size);
/*
            for (unsigned j = 0; j < bits_size; ++j)
            {
                size_type idx = bits[j] + base;
                value_type* BMRESTRICT str = cmatr_.row(idx);
                str[i] |= m;
            } // for j
*/
            return 0;
        }

        int add_range(size_type bv_offset, size_type sz) BMNOEXCEPT
        {
            BM_ASSERT(sz);

            auto base = bv_offset - sv_off_;
            const unsigned_value_type m = mask_;
            const unsigned i = substr_i_;
            size_type j = 0;
            do
            {
                size_type idx = j + base;
                value_type* BMRESTRICT str = cmatr_.row(idx);
                str[i] |= m;
            } while(++j < sz);
/*
            for (size_type j = 0; j < sz; ++j)
            {
                size_type idx = j + base;
                value_type* BMRESTRICT str = cmatr_.row(idx);
                str[i] |= m;
            } // for j
*/
            return 0;
        }

        CharMatrix&            cmatr_;        ///< target array for reverse transpose
        unsigned_value_type    mask_ = 0;     ///< bit-plane mask
        unsigned               substr_i_= 0;  ///< i
        size_type              sv_off_ = 0;   ///< SV read offset
    };


        BM_ASSERT(substr_from <= substr_to);
        BM_ASSERT(cmatr.is_init());

        if (zero_mem)
            cmatr.set_zero(); // TODO: set zero based on requested capacity
        
        size_type rows = size_type(cmatr.rows());
        size_type max_sz = this->size() - idx_from;
        if (max_sz < dec_size)
            dec_size = max_sz;
        if (rows < dec_size)
            dec_size = rows;
        if (!dec_size)
            return dec_size;

        sv_decode_visitor_func func(cmatr);

        for (unsigned i = substr_from; i <= substr_to; ++i)
        {
            unsigned bi = 0;
            func.substr_i_ = i - substr_from; // to put substr at the str[0]

            auto rsize = this->bmatr_.rows_not_null();
            for (unsigned k = i * 8; k < (i * 8) + 8; ++k, ++bi)
            {
                if (k >= rsize)
                    goto break2;
                const bvector_type* bv = this->bmatr_.get_row(k);
                if (!bv)
                    continue;
                BM_ASSERT (bv != this->get_null_bvector());

                func.mask_ = unsigned_value_type(1u << bi);
                func.sv_off_ = idx_from;

                size_type end = idx_from + dec_size;
                bm::for_each_bit_range_no_check(*bv, idx_from, end-1, func);

            } // for k
        } // for i
        break2:
        
        if (remap_flags_)
        {
            for (unsigned i = 0; i < dec_size; ++i)
            {
                typename CharMatrix::value_type* str = cmatr.row(i);
                remap_matrix1_.remapz(str);
            } // for i
        }
        return dec_size;
    }

    /**
        \brief Bulk import of strings from a C-style matrix of chars

        \param cmatr  - source matrix (bm::heap_matrix)
                        [in/out] parameter gets modified(corrupted)
                        in the process
        \param idx_from - destination index in the sparse vector
        \param imp_size - import size (number or rows to import)
    */
    template<typename CharMatrix>
    void import(CharMatrix& cmatr, size_type idx_from, size_type imp_size)
    {
        if (!imp_size)
            return;
        if (idx_from < this->size_) // in case it touches existing elements
        {
            // clear all planes in the range to provide corrrect import of 0 values
            this->clear_range(idx_from, idx_from + imp_size - 1);
        }
        import_no_check(cmatr, idx_from, imp_size);
    }

    /**
        \brief Bulk push-back import of strings from a C-style matrix of chars

        \param cmatr  - source matrix (bm::heap_matrix)
                        [in/out] parameter gets modified(corrupted)
                        in the process
        \param imp_size - import size (number or rows to import)
    */
    template<typename CharMatrix>
    void import_back(CharMatrix& cmatr, size_type imp_size)
    {
        if (!imp_size)
            return;
        import_no_check(cmatr, this->size(), imp_size);
    }


    ///@}

    // ------------------------------------------------------------
    /*! @name Merge, split, partition data                        */
    ///@{

    /**
        @brief copy range of values from another sparse vector

        Copy [left..right] values from the source vector,
        clear everything outside the range.

        \param sv - source vector
        \param left  - index from in losed diapason of [left..right]
        \param right - index to in losed diapason of [left..right]
        \param slice_null - "use_null" copy range for NULL vector or
                             do not copy it
    */
    void copy_range(const str_sparse_vector<CharType, BV, STR_SIZE>& sv,
                    size_type left, size_type right,
                    bm::null_support slice_null = bm::use_null);

    /**
        \brief merge with another sparse vector using OR operation
        Merge is different from join(), because it borrows data from the source
        vector, so it gets modified (destructive join)

        \param tr_sv - [in, out]argument vector to join with (vector mutates)

        \return self reference
     */
     str_sparse_vector<CharType, BV, STR_SIZE>&
     merge(str_sparse_vector<CharType, BV, STR_SIZE>& str_sv);

    /**
        Keep only specified interval in the sparse vector, clear all other
        elements.

        \param left  - interval start
        \param right - interval end (closed interval)
        \param slice_null - "use_null" copy range for NULL vector or not
     */
     void keep_range(size_type left, size_type right,
                    bm::null_support slice_null = bm::use_null);

    ///@}

    // ------------------------------------------------------------

    /*! \brief syncronize internal structures */
    void sync(bool force);

    /*!
        \brief check if another sparse vector has the same content and size
     
        \param sv        - sparse vector for comparison
        \param null_able - flag to consider NULL vector in comparison (default)
                           or compare only value content planes
     
        \return true, if it is the same
    */
    bool equal(const str_sparse_vector<CharType, BV, STR_SIZE>& sv,
               bm::null_support null_able = bm::use_null) const BMNOEXCEPT;

    /**
        \brief find position of compressed element by its rank
    */
    static
    bool find_rank(size_type rank, size_type& pos) BMNOEXCEPT;
    
    /**
        \brief size of sparse vector (may be different for RSC)
    */
    size_type effective_size() const BMNOEXCEPT { return size(); }

protected:
    enum insert_buf_size_e
    {
        ins_buf_size = bm::gap_max_bits // 1024 * 8
    };

    /// @internal
    template<typename CharMatrix, size_t BufSize = ins_buf_size>
    void import_no_check(CharMatrix& cmatr,
                         size_type idx_from, size_type imp_size,
                         bool set_not_null = true)
    {
        BM_ASSERT (cmatr.is_init());

        unsigned max_str_size = 0;
        {
            for (unsigned j = 0; j < imp_size; ++j)
            {
                typename CharMatrix::value_type* str = cmatr.row(j);
                typename CharMatrix::size_type i;
                typename CharMatrix::size_type cols = cmatr.cols();
                for (i = 0; i < cols; ++i)
                {
                    value_type ch = str[i];
                    if (!ch)
                    {
                        max_str_size =
                            (unsigned)((i > max_str_size) ? i : max_str_size);
                        break;
                    }
                    if (remap_flags_) // re-mapping is in effect
                    {
                        unsigned char remap_value =
                            remap_matrix2_.get(i, (unsigned char)(ch));
                        BM_ASSERT(remap_value); // unknown ?!
                        /*
                        if (!remap_value) // unknown dictionary element
                            throw_bad_value(0); */
                        str[i] = CharType(remap_value);
                    }
                } // for i
            } // for j
        }

        this->bmatr_.allocate_rows((1+max_str_size) * 8 + this->is_nullable());

        unsigned_value_type ch_slice[BufSize];
        for (unsigned i = 0; i < max_str_size; ++i)
        {
            unsigned ch_acc = 0;
#if defined(BMVECTOPT) || defined(BM_USE_GCC_BUILD)
            if (imp_size == ins_buf_size) /// full buffer import can use loop unrolling
            {
                for (size_type j = 0; j < imp_size; j+=4)
                {
                    unsigned_value_type ch0 = (unsigned_value_type)cmatr.row(j)[i];
                    unsigned_value_type ch1 = (unsigned_value_type)cmatr.row(j+1)[i];
                    unsigned_value_type ch2 = (unsigned_value_type)cmatr.row(j+2)[i];
                    unsigned_value_type ch3 = (unsigned_value_type)cmatr.row(j+3)[i];

                    ch_acc |= ch0 | ch1 | ch2 | ch3;
                    ch_slice[j] = ch0; ch_slice[j+1] = ch1;
                    ch_slice[j+2] = ch2; ch_slice[j+3] = ch3;
                }
            }
            else
#endif
            {
                for (size_type j = 0; j < imp_size; ++j)
                {
                    unsigned_value_type ch = (unsigned_value_type)cmatr.row(j)[i];
                    ch_acc |= ch;
                    ch_slice[j] = ch;
                }
            }
            import_char_slice(ch_slice, ch_acc, i, idx_from, imp_size);
        }

        size_type idx_to = idx_from + imp_size - 1;
        if (set_not_null)
        {
            if (bvector_type* bv_null = this->get_null_bvect())
                bv_null->set_range(idx_from, idx_to);
        }
        if (idx_to >= this->size())
            this->size_ = idx_to+1;

    }
#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4146 )
#endif
    /// @internal
    template<size_t BufSize = ins_buf_size>
    void import_char_slice(const unsigned_value_type* ch_slice,
                           unsigned ch_acc,
                           size_type char_slice_idx,
                           size_type idx_from, size_type imp_size)
    {
        size_type bit_list[BufSize];
        for ( ;ch_acc; ch_acc &= ch_acc - 1) // bit-scan
        {
            unsigned n_bits = 0;
            const unsigned bi = (bm::word_bitcount((ch_acc & -ch_acc) - 1));
            unsigned mask = 1u << bi;
#if defined(BMVECTOPT) || defined(BM_USE_GCC_BUILD)
            if (imp_size == ins_buf_size) /// full buffer import can use loop unrolling
            {
                mask |= (mask << 8) | (mask << 16) | (mask << 24);
                for (size_type j = 0; j < imp_size; j+=4)
                {
                    unsigned ch0 = ((unsigned)ch_slice[j+0]) |
                                   ((unsigned)ch_slice[j+1] << 8)  |
                                   ((unsigned)ch_slice[j+2] << 16) |
                                   ((unsigned)ch_slice[j+3] << 24);
                    ch0 &= mask;
                    ch0 = (ch0 >> bi) | (ch0 >> (bi+7)) |
                          (ch0 >> (bi+14)) | (ch0 >> (bi+21));
                    ch0 &= 15u;
                    BM_ASSERT(bm::word_bitcount(ch0) <= 4);
                    for (size_type base_idx = idx_from + j ;ch0; ch0 &= ch0 - 1) // bit-scan
                    {
                        const unsigned bit_idx =
                                (bm::word_bitcount((ch0 & -ch0) - 1));
                        bit_list[n_bits++] = base_idx + bit_idx;
                    } // for ch0
                } // for j
            }
            else
#endif
            {
                for (size_type j = 0; j < imp_size; ++j)
                {
                    unsigned ch = unsigned(ch_slice[j]);
                    if (ch & mask)
                        bit_list[n_bits++] = idx_from + j;
                } // for j
            }

            if (n_bits) // set transposed bits to the target plane
            {
                bvector_type* bv =
                    this->get_create_slice((unsigned)(char_slice_idx * 8) + bi);
                bv->import_sorted(&bit_list[0], n_bits, false);
            }
        } // for ch_acc
    }
#ifdef _MSC_VER
#pragma warning( pop )
#endif
    // ------------------------------------------------------------
    /*! @name Errors and exceptions                              */
    ///@{

    /**
        \brief throw range error
        \internal
    */
    static
    void throw_range_error(const char* err_msg);

    /**
        \brief throw domain error
        \internal
    */
    static
    void throw_bad_value(const char* err_msg);

    ///@}

    /*! \brief set value without checking boundaries */
    void set_value(size_type idx, const value_type* str);

    /*! \brief set value without checking boundaries or support of NULL */
    void set_value_no_null(size_type idx, const value_type* str);

    /*! \brief insert value without checking boundaries */
    void insert_value(size_type idx, const value_type* str);

    /*! \brief insert value without checking boundaries or support of NULL */
    void insert_value_no_null(size_type idx, const value_type* str);


    size_type size_internal() const { return size(); }
    void resize_internal(size_type sz) { resize(sz); }

    size_t remap_size() const { return remap_matrix1_.get_buffer().size(); }
    const unsigned char* get_remap_buffer() const
                { return remap_matrix1_.get_buffer().buf(); }
    unsigned char* init_remap_buffer()
    {
        remap_matrix1_.init(true);
        return remap_matrix1_.get_buffer().data();
    }
    void set_remap() { remap_flags_ = 1; }

protected:

    bool resolve_range(size_type from, size_type to,
                       size_type* idx_from, size_type* idx_to) const
    {
        *idx_from = from; *idx_to = to; return true;
    }

    const remap_matrix_type* get_remap_matrix() const
        { return &remap_matrix1_; }
    remap_matrix_type* get_remap_matrix()
        { return &remap_matrix1_; }

    /**
        reamp using statistics table from inserter
    */
    void remap(back_insert_iterator& iit);

    /**
        Remap from implementation, please note that move_data flag can violate cosnt-ness
     */
    void remap_from_impl(const str_sparse_vector& str_sv,
                         octet_freq_matrix_type* omatrix,
                         bool move_data);

protected:
    template<class SVect> friend class sparse_vector_serializer;
    template<class SVect> friend class sparse_vector_deserializer;

protected:
    unsigned                  remap_flags_;  ///< remapping status
    slice_octet_matrix_type  remap_matrix1_; ///< octet remap table 1
    slice_octet_matrix_type  remap_matrix2_; ///< octet remap table 2
};

//---------------------------------------------------------------------
//---------------------------------------------------------------------


template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::str_sparse_vector(
        bm::null_support null_able,
        allocation_policy_type  ap,
        size_type               bv_max_size,
        const allocator_type&   alloc)
: parent_type(null_able, ap, bv_max_size, alloc),
  remap_flags_(0)
{
    static_assert(STR_SIZE > 1,
        "BM:: String vector size must be > 1 (to accomodate 0 terminator)");
}


//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::str_sparse_vector(
                                        const str_sparse_vector& str_sv)
: parent_type(str_sv),
  remap_flags_(str_sv.remap_flags_),
  remap_matrix1_(str_sv.remap_matrix1_),
  remap_matrix2_(str_sv.remap_matrix2_)
{
    static_assert(STR_SIZE > 1,
        "BM:: String vector size must be > 1 (to accomodate 0 terminator)");
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::str_sparse_vector(
              const str_sparse_vector& str_sv, bm::remap_setup remap_mode)
: parent_type(str_sv.get_null_support()),
  remap_flags_(str_sv.remap_flags_),
  remap_matrix1_(str_sv.remap_matrix1_),
  remap_matrix2_(str_sv.remap_matrix2_)
{
    BM_ASSERT(str_sv.remap_flags_); // source vector should be remapped
    BM_ASSERT(remap_mode == bm::remap_setup::COPY_RTABLES);
    static_assert(STR_SIZE > 1,
        "BM:: String vector size must be > 1 (to accomodate 0 terminator)");
    (void) remap_mode;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::swap(
                                str_sparse_vector& str_sv) BMNOEXCEPT
{
    parent_type::swap(str_sv);
    bm::xor_swap(remap_flags_, str_sv.remap_flags_);
    remap_matrix1_.swap(str_sv.remap_matrix1_);
    remap_matrix2_.swap(str_sv.remap_matrix2_);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::set(
                                size_type idx, const value_type* str)
{
    if (idx >= this->size())
        this->size_ = idx+1;
    set_value(idx, str);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::insert(
                                size_type idx, const value_type* str)
{
    if (idx >= this->size())
    {
        this->size_ = idx+1;
        set_value(idx, str);
        return;
    }
    insert_value(idx, str);
    this->size_++;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::erase(size_type idx)
{
    BM_ASSERT(idx < this->size_);
    if (idx >= this->size_)
        return;
    this->erase_column(idx, true);
    this->size_--;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::set_null(size_type idx)
{
    if (idx >= this->size_)
        this->size_ = idx + 1; // assumed nothing todo outside current size
    else
        this->bmatr_.clear_column(idx, 0);
}
//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::push_back_null(size_type count)
{
    BM_ASSERT(count);
    BM_ASSERT(bm::id_max - count > this->size_);
    BM_ASSERT(this->is_nullable());

    this->size_ += count;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::set_value(
                                size_type idx, const value_type* str)
{
    set_value_no_null(idx, str);
    if (bvector_type* bv_null = this->get_null_bvect())
        bv_null->set_bit_no_check(idx);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::set_value_no_null(
                                size_type idx, const value_type* str)
{
    for (unsigned i = 0; true; ++i)
    {
        CharType ch = str[i];
        if (!ch)
        {
            this->clear_value_planes_from(i*8, idx);
            return;
        }
        if (remap_flags_) // compressional re-mapping is in effect
        {
            auto r = remap_matrix2_.rows();
            if (i >= r)
            {
                remap_matrix1_.resize(i + 1, remap_matrix1_.cols(), true);
                remap_matrix2_.resize(i + 1, remap_matrix2_.cols(), true);
            }
            unsigned char remap_value = remap_matrix2_.get(i, unsigned(ch));
            BM_ASSERT(remap_value);
            if (!remap_value) // unknown dictionary element
            {
                this->clear_value_planes_from(i*8, idx);
                return;
            }
            ch = CharType(remap_value);
        }
        this->bmatr_.set_octet(idx, i, (unsigned char)ch);
    } // for i
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::insert_value(
                                    size_type idx, const value_type* str)
{
    insert_value_no_null(idx, str);
    this->insert_null(idx, true);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::insert_value_no_null(
                                        size_type idx, const value_type* str)
{
    for (unsigned i = 0; true; ++i)
    {
        CharType ch = str[i];
        if (!ch)
        {
            this->insert_clear_value_planes_from(i*8, idx);
            return;
        }
        
        if (remap_flags_) // compressional re-mapping is in effect
        {
            unsigned char remap_value = remap_matrix2_.get(i, unsigned(ch));
            BM_ASSERT(remap_value);
            if (!remap_value) // unknown dictionary element
            {
                this->insert_clear_value_planes_from(i*8, idx);
                return;
            }
            ch = CharType(remap_value);
        }
        this->bmatr_.insert_octet(idx, i, (unsigned char)ch);
    } // for i
}


//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
typename str_sparse_vector<CharType, BV, STR_SIZE>::size_type
str_sparse_vector<CharType, BV, STR_SIZE>::get(
            size_type idx, value_type* str, size_type buf_size) const BMNOEXCEPT
{
    size_type i = 0;
    for (; true; ++i)
    {
        if (i >= buf_size)
            break;
        CharType ch = CharType(this->bmatr_.get_octet(idx, i));
        str[i] = ch;
        if (!ch)
            break;
    }
    if (remap_flags_)
        remap_matrix1_.remap(str, i);
    return i;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::optimize(
      bm::word_t* temp_block,
      typename bvector_type::optmode opt_mode,
      typename str_sparse_vector<CharType, BV, STR_SIZE>::statistics* st)
{
    typename bvector_type::statistics stbv;
    parent_type::optimize(temp_block, opt_mode, &stbv);
    
    if (st)
        st->add(stbv);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::calc_stat(
    struct str_sparse_vector<CharType, BV, STR_SIZE>::statistics* st
    ) const BMNOEXCEPT
{
    BM_ASSERT(st);
    typename bvector_type::statistics stbv;
    parent_type::calc_stat(&stbv);
    
    st->reset();
    
    st->bit_blocks += stbv.bit_blocks;
    st->gap_blocks += stbv.gap_blocks;
    st->ptr_sub_blocks += stbv.ptr_sub_blocks;
    st->bv_count += stbv.bv_count;
    st->max_serialize_mem += stbv.max_serialize_mem + 8;
    st->memory_used += stbv.memory_used;
    st->gap_cap_overhead += stbv.gap_cap_overhead;
    
    size_t remap_mem_usage = sizeof(remap_flags_);
    remap_mem_usage += remap_matrix1_.get_buffer().mem_usage();
    remap_mem_usage += remap_matrix2_.get_buffer().mem_usage();

    st->memory_used += remap_mem_usage;
    if (remap_flags_) // use of remapping requires some extra storage
    {
        st->max_serialize_mem += (remap_mem_usage * 2);
    }
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
int str_sparse_vector<CharType, BV, STR_SIZE>::compare_str(
        const value_type* str1, const value_type* str2)  BMNOEXCEPT
{
    BM_ASSERT(str1 && str2);
    int res = 0;
    for (unsigned i = 0; true; ++i)
    {
        CharType octet2 = str2[i];
        CharType octet1 = str1[i];
        if (!octet1)
        {
            res = -octet2; // -1 || 0
            break;
        }
        res = (octet1 > octet2) - (octet1 < octet2);
        if (res || !octet2)
            break;
    } // for i
    return res;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
int str_sparse_vector<CharType, BV, STR_SIZE>::compare_str(
    const value_type* str1, const value_type* str2, size_t min_len) BMNOEXCEPT
{
    BM_ASSERT(str1 && str2);

    int res = 0;
    size_t i = 0;

    CharType octet2, octet1;
    if (min_len >= 4)
    {
        for (; i < min_len-3; i+=4)
        {
            unsigned i2, i1;
            ::memcpy(&i2, &str2[i], sizeof(i2));
            ::memcpy(&i1, &str1[i], sizeof(i1));
            BM_ASSERT(!bm::has_zero_byte_u64(bm::id64_t(i2) | (bm::id64_t(i1) << 32)));
            if (i1 != i2)
            {
                octet2 = str2[i];
                octet1 = str1[i];
                res = (octet1 > octet2) - (octet1 < octet2);
                if (res)
                    return res;
                octet2 = str2[i+1];
                octet1 = str1[i+1];
                res = (octet1 > octet2) - (octet1 < octet2);
                if (res)
                    return res;
                octet2 = str2[i+2];
                octet1 = str1[i+2];
                res = (octet1 > octet2) - (octet1 < octet2);
                if (res)
                    return res;
                octet2 = str2[i+3];
                octet1 = str1[i+3];
                res = (octet1 > octet2) - (octet1 < octet2);
                if (res)
                    return res;
                BM_ASSERT(0);
                break;
            }
        } // for i
    }


    for (; i < min_len; ++i)
    {
        octet2 = str2[i];
        octet1 = str1[i];
        BM_ASSERT(octet1 && octet2);
        res = (octet1 > octet2) - (octet1 < octet2);
        if (res)
            return res;
    } // for i

    for (; true; ++i)
    {
        octet2 = str2[i];
        octet1 = str1[i];
        if (!octet1)
        {
            res = -octet2; // -1 || 0
            break;
        }
        res = (octet1 > octet2) - (octet1 < octet2);
        if (res || !octet2)
            break;
    } // for i
    return res;
}


//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
int str_sparse_vector<CharType, BV, STR_SIZE>::compare_remap(
                size_type idx, const value_type* str) const BMNOEXCEPT
{
    BM_ASSERT(str);
    BM_ASSERT(is_remap()); // MUST guarantee remapping

    int res = 0;
    for (unsigned i = 0; true; ++i)
    {
        CharType octet2 = str[i];
        CharType octet1 = (CharType)this->bmatr_.get_octet(idx, i);
        if (!octet1)
        {
            res = -octet2; // -1 || 0
            break;
        }
        const unsigned char* remap_row = remap_matrix1_.row(i);
        CharType remap_value1 = (CharType)remap_row[unsigned(octet1)];
        BM_ASSERT(remap_value1);
        res = (remap_value1 > octet2) - (remap_value1 < octet2);
        if (res || !octet2)
            break;
    } // for i
    return res;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
int str_sparse_vector<CharType, BV, STR_SIZE>::compare_nomap(size_type idx,
                                    const value_type* str) const BMNOEXCEPT
{
    BM_ASSERT(str);
    BM_ASSERT(!is_remap()); // MUST guarantee remapping

    int res = 0;
    for (unsigned i = 0; true; ++i)
    {
        CharType octet2 = str[i];
        CharType octet1 = (CharType)this->bmatr_.get_octet(idx, i);
        if (!octet1)
        {
            res = -octet2; // -1 || 0
            break;
        }
        res = (octet1 > octet2) - (octet1 < octet2);
        if (res || !octet2)
            break;
    } // for i
    return res;
}


//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
int str_sparse_vector<CharType, BV, STR_SIZE>::compare(
                     size_type idx,
                     const value_type* str) const BMNOEXCEPT
{
    BM_ASSERT(str);
    int res = remap_flags_ ? compare_remap(idx, str)
                           : compare_nomap(idx, str);
    return res;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
int str_sparse_vector<CharType, BV, STR_SIZE>::compare(
                                              size_type idx1,
                                              size_type idx2) const BMNOEXCEPT
{
    int res = 0;
    if (idx1 == idx2)
        return 0;
    if (remap_flags_)
    {
        for (unsigned i = 0; true; ++i)
        {
            // TODO: implement function to return two octets at once
            CharType octet2 = (CharType)this->bmatr_.get_octet(idx2, i);
            CharType octet1 = (CharType)this->bmatr_.get_octet(idx1, i);
            if (!octet1)
            {
                res = -octet2; // -1 || 0
                break;
            }
            const unsigned char* remap_row = remap_matrix1_.row(i);
            unsigned char remap_value1 = remap_row[unsigned(octet1)];
            BM_ASSERT(remap_value1);
            unsigned char remap_value2 = remap_row[unsigned(octet2)];
            BM_ASSERT(remap_value2);
            res = (remap_value1 > remap_value2) - (remap_value1 < remap_value2);
            if (res || !octet2)
                break;
        } // for i
    }
    else
    {
        for (unsigned i = 0; true; ++i)
        {
            CharType octet2 = (CharType)this->bmatr_.get_octet(idx2, i);
            CharType octet1 = (CharType)this->bmatr_.get_octet(idx1, i);
            if (!octet1)
            {
                res = -octet2; // -1 || 0
                break;
            }
            res = (octet1 > octet2) - (octet1 < octet2);
            if (res || !octet2)
                break;
        } // for i
    }
    return res;

}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
template<bool USE_PREFIX_BUF>
unsigned str_sparse_vector<CharType, BV, STR_SIZE>::common_prefix_length(
                                size_type idx1, size_type idx2,
                                value_type* prefix_buf) const BMNOEXCEPT
{
    BM_ASSERT (!(prefix_buf && !USE_PREFIX_BUF));
    unsigned i = 0;
    CharType ch1 = CharType(this->bmatr_.get_octet(idx1, i));
    CharType ch2 = CharType(this->bmatr_.get_octet(idx2, i));
    if (ch1 == ch2 && (ch1|ch2))
    {
        if constexpr(USE_PREFIX_BUF)
        {
            BM_ASSERT(prefix_buf);
            *prefix_buf++ = ch1;
        }
        for (++i; true; ++i)
        {
            ch1 = CharType(this->bmatr_.get_octet(idx1, i));
            ch2 = CharType(this->bmatr_.get_octet(idx2, i));
            if (ch1 != ch2 || (!(ch1|ch2))) // chs not the same or both zero
                return i;
            if constexpr(USE_PREFIX_BUF)
                *prefix_buf++ = ch1;
        } // for i
    }
    return i;
}


//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
bool 
str_sparse_vector<CharType, BV, STR_SIZE>::find_rank(
                                                size_type rank,
                                                size_type& pos) BMNOEXCEPT
{
    BM_ASSERT(rank);
    pos = rank - 1;
    return true;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
typename str_sparse_vector<CharType, BV, STR_SIZE>::size_type
str_sparse_vector<CharType, BV, STR_SIZE>::effective_max_str()
                                                        const BMNOEXCEPT
{
    return this->bmatr_.octet_size();
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::calc_octet_stat(
                    octet_freq_matrix_type& octet_matrix) const
{
    size_type max_str_len = effective_max_str();
    octet_matrix.resize(max_str_len, 256, false);
    octet_matrix.set_zero(); //init(true);
    {
    const_iterator it(this);
    for(; it.valid(); ++it)
    {
        const value_type* s = *it; // get asciiz char*
        if(!s)
            continue;
        for (unsigned i = 0; true; ++i) // for each char in str
        {
            value_type ch = s[i];
            if (!ch)
                break;
            typename
            octet_freq_matrix_type::value_type* row = octet_matrix.row(i);
            unsigned ch_idx = (unsigned char)ch;
            row[ch_idx] += 1;
        } // for i
    } // for it
    }
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::build_octet_remap(
            slice_octet_matrix_type& octet_remap_matrix1,
            slice_octet_matrix_type& octet_remap_matrix2,
            octet_freq_matrix_type& octet_occupancy_matrix) const
{
    size_type max_str_len = effective_max_str();
    octet_remap_matrix1.resize(max_str_len, 256, false);
    octet_remap_matrix1.set_zero();
    octet_remap_matrix2.resize(max_str_len, 256, false);
    octet_remap_matrix2.set_zero();

    for (unsigned i = 0; i < octet_occupancy_matrix.rows(); ++i)
    {
        typename octet_freq_matrix_type::value_type* frq_row =
                                                octet_occupancy_matrix.row(i);

        unsigned char* remap_row1 = octet_remap_matrix1.row(i);
        unsigned char* remap_row2 = octet_remap_matrix2.row(i);

        const typename slice_octet_matrix_type::size_type row_size =
                                             octet_occupancy_matrix.cols();
        for (unsigned remap_code = 1; true; ++remap_code)
        {
            typename octet_freq_matrix_type::size_type char_idx;
            bool found = bm::find_max_nz(frq_row, row_size, &char_idx);
            #if 0
            bool found = bm::find_first_nz(frq_row, row_size, &char_idx);
            #endif
            if (!found)
                break;
            BM_ASSERT(char_idx);
            unsigned char ch = (unsigned char)char_idx;
            remap_row1[remap_code] = ch;
            remap_row2[ch] = (unsigned char)remap_code;
            frq_row[char_idx] = 0; // clear the picked char
        } // for
    } // for i
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::recalc_remap_matrix2()
{
    BM_ASSERT(remap_flags_);

    auto rows = remap_matrix1_.rows();
    remap_matrix2_.resize(rows, remap_matrix1_.cols(), false);
    if (rows)
    {
        remap_matrix2_.set_zero();
        //remap_matrix2_.init(true);
        for (unsigned i = 0; i < remap_matrix1_.rows(); ++i)
        {
            const unsigned char* remap_row1 = remap_matrix1_.row(i);
                  unsigned char* remap_row2 = remap_matrix2_.row(i);
            for (unsigned j = 1; j < remap_matrix1_.cols(); ++j)
            {
                if (remap_row1[j])
                {
                    unsigned ch_code = remap_row1[j];
                    remap_row2[ch_code] = (unsigned char)j;
                    BM_ASSERT(ch_code < 256);
                }
            } // for j
        } // for i
    } // if rows
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
bool str_sparse_vector<CharType, BV, STR_SIZE>::remap_tosv(
       value_type*   BMRESTRICT     sv_str,
       size_type                    buf_size,
       const value_type* BMRESTRICT str,
       const slice_octet_matrix_type& BMRESTRICT octet_remap_matrix2) BMNOEXCEPT
{
    const unsigned char* remap_row = octet_remap_matrix2.row(0);
    for (unsigned i = 0; i < buf_size; ++i, remap_row += 256)
    {
        CharType ch = str[i];
        if (!ch)
        {
            sv_str[i] = ch;
            break;
        }
//        const unsigned char* remap_row = octet_remap_matrix2.row(i);
        unsigned char remap_value = remap_row[unsigned(ch)];
        sv_str[i] = CharType(remap_value);
        if (!remap_value) // unknown dictionary element
            return false;
    } // for i
    return true;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
bool str_sparse_vector<CharType, BV, STR_SIZE>::remap_n_tosv_2way(
       value_type*   BMRESTRICT     sv_str,
       value_type*   BMRESTRICT     str_cp,
       size_type                    buf_size,
       const value_type* BMRESTRICT str,
       size_t                       in_len,
       const slice_octet_matrix_type& BMRESTRICT octet_remap_matrix2) BMNOEXCEPT
{
    BM_ASSERT(in_len <= buf_size); (void) buf_size;

    const unsigned char* remap_row = octet_remap_matrix2.row(0);
    for (unsigned i = 0; i < in_len; ++i, remap_row += 256)
    {
        CharType ch = str[i];
        str_cp[i] = value_type(ch);
        BM_ASSERT(ch);
        unsigned char remap_value = remap_row[unsigned(ch)];
        sv_str[i] = CharType(remap_value);
        if (!remap_value) // unknown dictionary element
            return false;
    } // for i
    sv_str[in_len] = str_cp[in_len] = 0; // gurantee zero termination
    return true;
}


//---------------------------------------------------------------------

template<class CharType, class BV, unsigned MAX_STR_SIZE>
bool str_sparse_vector<CharType, BV, MAX_STR_SIZE>::remap_fromsv(
         value_type* BMRESTRICT str,
         size_type         buf_size,
         const value_type* BMRESTRICT sv_str,
         const slice_octet_matrix_type& BMRESTRICT octet_remap_matrix1
         ) BMNOEXCEPT
{
    const unsigned char* remap_row = octet_remap_matrix1.row(0);
    for (unsigned i = 0; i < buf_size; ++i, remap_row += 256)
    {
        CharType ch = sv_str[i];
        if (!ch)
        {
            str[i] = ch;
            break;
        }
        unsigned char remap_value = remap_row[unsigned(ch)];
        str[i] = CharType(remap_value);
        if (!remap_value) // unknown dictionary element
            return false;
    } // for i
    return true;
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned MAX_STR_SIZE>
void str_sparse_vector<CharType, BV, MAX_STR_SIZE>::remap()
{
    str_sparse_vector<CharType, BV, MAX_STR_SIZE>
                                sv_tmp(this->get_null_support());
    sv_tmp.remap_from_impl(*this, 0, true /*move data*/);
    sv_tmp.swap(*this);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned MAX_STR_SIZE>
void str_sparse_vector<CharType, BV, MAX_STR_SIZE>::remap(
                                    back_insert_iterator& iit)
{
    if (iit.remap_flags_ && iit.omatrix_.rows())
    {
        str_sparse_vector<CharType, BV, MAX_STR_SIZE>
                                    sv_tmp(this->get_null_support());
        sv_tmp.remap_from_impl(*this, &iit.omatrix_, true /*move data*/);
        sv_tmp.swap(*this);
    }
    else
        remap();
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void
str_sparse_vector<CharType, BV, STR_SIZE>::remap_from(
                                    const str_sparse_vector& str_sv,
                                    octet_freq_matrix_type* omatrix)
{
    remap_from_impl(str_sv, omatrix, false);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void
str_sparse_vector<CharType, BV, STR_SIZE>::remap_from_impl(
                                    const str_sparse_vector& str_sv,
                                    octet_freq_matrix_type*  omatrix,
                                    bool                     move_data)
{
    const unsigned buffer_size = ins_buf_size; // bm::gap_max_bits; // 65536;

    if (str_sv.is_remap())
    {
        *this = str_sv;
        return;
    }

    typename bvector_type::allocator_pool_type pool;
    typename
    bm::alloc_pool_guard<typename bvector_type::allocator_pool_type, str_sparse_vector> g1, g2;
    if (move_data)
    {
        str_sparse_vector& sv = const_cast<str_sparse_vector&>(str_sv);
        g1.assign_if_not_set(pool, *this);
        g2.assign_if_not_set(pool, sv);

        auto r = sv.get_bmatrix().rows();
        pool.set_block_limit(r + 10);
    }

    this->clear_all(true);
    if (str_sv.empty()) // no content to remap
        return;

    octet_freq_matrix_type occ_matrix; // occupancy map
    if (!omatrix)
    {
        str_sv.calc_octet_stat(occ_matrix);
        omatrix = &occ_matrix;
    }
    str_sv.build_octet_remap(remap_matrix1_, remap_matrix2_, *omatrix);
    remap_flags_ = 1; // turn ON remapped mode

    typedef bm::dynamic_heap_matrix<CharType, allocator_type> buffer_matrix_type;

    size_type str_len = str_sv.effective_max_str()+1;
    buffer_matrix_type cmatr(buffer_size, str_len);
    cmatr.init(true); // init and set zero

    for (size_type i{0}, dsize; true; i += dsize)
    {
        dsize = str_sv.decode(cmatr, i, buffer_size, true);
        if (!dsize)
            break;
        if (move_data && (dsize == ins_buf_size)) // free the src.vect blocks
        {
            // here const_cast is OK, because we violate cosnt-ness only
            // in internal safe cases controlled by the upper level call
            //
            str_sparse_vector& sv = const_cast<str_sparse_vector&>(str_sv);
            sv.clear_range(i, i+dsize-1, false);
        }

        this->import(cmatr, i, dsize);
    } // for i

    if (bvector_type* bv_null = this->get_null_bvect())
    {
        if (const bvector_type* bv_null_arg = str_sv.get_null_bvector())
            if (move_data)
            {
                bvector_type* bv = const_cast<bvector_type*>(bv_null_arg);
                bv_null->swap(*bv);
            }
            else
                *bv_null = *bv_null_arg;
        else
        {
            // TODO: exception? assert? maybe it is OK...
        }
    }

}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::sync(bool /*force*/)
{
    if (remap_flags_)
        recalc_remap_matrix2();
    this->sync_ro();
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
bool str_sparse_vector<CharType, BV, STR_SIZE>::equal(
                const str_sparse_vector<CharType, BV, STR_SIZE>& sv,
                bm::null_support null_able) const BMNOEXCEPT
{
    // at this point both vectors should have the same remap settings
    // to be considered "equal".
    // Strictly speaking this is incorrect, because re-map and non-remap
    // vectors may have the same content

    if (remap_flags_ != sv.remap_flags_)
        return false;
    if (remap_flags_)
    {
        // TODO: equal matrix dimention overlap may be not enough
        // (check the non-overlap to be zero)
        // dimentionality shrink is a result of de-serialization
        bool b;
        b = remap_matrix1_.equal_overlap(sv.remap_matrix1_);
        if (!b)
            return b;
        b = remap_matrix2_.equal_overlap(sv.remap_matrix2_);
        if (!b)
            return b;
    }
    return parent_type::equal(sv, null_able);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::copy_range(
                const str_sparse_vector<CharType, BV, STR_SIZE>& sv,
                size_type left, size_type right,
                bm::null_support slice_null)
{
    if (left > right)
        bm::xor_swap(left, right);
    this->clear_all(true);

    remap_flags_ = sv.remap_flags_;
    remap_matrix1_ = sv.remap_matrix1_;
    remap_matrix2_ = sv.remap_matrix2_;

    this->copy_range_slices(sv, left, right, slice_null);
    this->resize(sv.size());
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>&
str_sparse_vector<CharType, BV, STR_SIZE>::merge(
                    str_sparse_vector<CharType, BV, STR_SIZE>& str_sv)
{
    size_type arg_size = str_sv.size();
    if (this->size_ < arg_size)
        resize(arg_size);

    // there is an assumption here that we only need to copy remap flags once
    // because we merge matrices with the same remaps
    // otherwise - undefined behavior
    //
    if (remap_flags_ != str_sv.remap_flags_)
    {
        remap_flags_ = str_sv.remap_flags_;
        remap_matrix1_ = str_sv.remap_matrix1_;
        remap_matrix2_ = str_sv.remap_matrix2_;
    }
    bvector_type* bv_null = this->get_null_bvect();

    this->merge_matr(str_sv.bmatr_);

    // our vector is NULL-able but argument is not (assumed all values are real)
    if (bv_null && !str_sv.is_nullable())
        bv_null->set_range(0, arg_size-1);

    return *this;

}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::keep_range(
                size_type left, size_type right,
                bm::null_support slice_null)
{
    if (right < left)
        bm::xor_swap(left, right);
    this->keep_range_no_check(left, right, slice_null);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
typename str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator
str_sparse_vector<CharType, BV, STR_SIZE>::begin() const BMNOEXCEPT
{
    typedef typename
        str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator it_type;
    return it_type(this);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::clear_all(
                                        bool free_mem) BMNOEXCEPT
{
    parent_type::clear_all(free_mem);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::throw_range_error(
                                                           const char* err_msg)
{
#ifndef BM_NO_STL
    throw std::range_error(err_msg);
#else
    BM_ASSERT_THROW(false, BM_ERR_RANGE);
#endif
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::throw_bad_value(
                                                           const char* err_msg)
{
#ifndef BM_NO_STL
    if (!err_msg)
        err_msg = "Unknown/incomparable dictionary character";
    throw std::domain_error(err_msg);
#else
    BM_ASSERT_THROW(false, BM_BAD_VALUE);
#endif
}


//---------------------------------------------------------------------
//
//---------------------------------------------------------------------


template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::const_iterator() BMNOEXCEPT
: sv_(0), substr_from_(0), substr_to_(STR_SIZE),
  pos_(bm::id_max), pos_in_buf_(~size_type(0))
{
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::const_iterator(
   const str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator& it) BMNOEXCEPT
: sv_(it.sv_),
  substr_from_(it.substr_from_), substr_to_(it.substr_to_),
  pos_(it.pos_), pos_in_buf_(~size_type(0))
{
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::const_iterator(
    const str_sparse_vector<CharType, BV, STR_SIZE>* sv) BMNOEXCEPT
: sv_(sv), pos_(sv->empty() ? bm::id_max : 0), pos_in_buf_(~size_type(0))
{
    substr_from_ = 0;
    substr_to_ = (unsigned) sv_->effective_max_str();
    buf_matrix_.resize(n_rows, substr_to_+1);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::const_iterator(
    const str_sparse_vector<CharType, BV, STR_SIZE>* sv,
    typename str_sparse_vector<CharType, BV, STR_SIZE>::size_type pos) BMNOEXCEPT
: sv_(sv), pos_(pos >= sv->size() ? bm::id_max : pos), pos_in_buf_(~size_type(0))
{
    substr_from_ = 0;
    substr_to_ = (unsigned) sv_->effective_max_str();
    buf_matrix_.resize(n_rows, substr_to_+1);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::set_substr(
                    unsigned from, unsigned len) BMNOEXCEPT
{
    unsigned max_str = sv_->effective_max_str();
    substr_from_ = from;
    if (!len)
    {
        len = 1 + max_str - from;
        substr_to_ = from + len;
    }
    else
    {
        // TODO: check for overflow
        substr_to_ = substr_from_ + (len - 1);
    }
    if (max_str < substr_to_)
        substr_to_ = max_str;
    buf_matrix_.resize(n_rows, len+1, false/*no content copy*/);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
const typename str_sparse_vector<CharType, BV, STR_SIZE>::value_type*
str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::value() const
{
    BM_ASSERT(sv_);
    BM_ASSERT(this->valid());

    if (pos_in_buf_ == ~size_type(0))
    {
        if (!buf_matrix_.is_init())
            buf_matrix_.init();
        pos_in_buf_ = 0;
        size_type d = sv_->decode_substr(buf_matrix_, pos_, n_rows,
                                         substr_from_, substr_to_);
        if (!d)
        {
            pos_ = bm::id_max;
            return 0;
        }
    }
    if (is_null())
        return 0;
    return buf_matrix_.row(pos_in_buf_);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
typename str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::string_view_type
str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::get_string_view() const
{
    BM_ASSERT(sv_);
    BM_ASSERT(this->valid());

    if (pos_in_buf_ == ~size_type(0))
    {
        if (!buf_matrix_.is_init())
            buf_matrix_.init();
        pos_in_buf_ = 0;
        size_type d = sv_->decode_substr(buf_matrix_, pos_, n_rows,
                                         substr_from_, substr_to_);
        if (!d)
        {
            pos_ = bm::id_max;
            return string_view_type();
        }
    }
    if (is_null())
        return string_view_type();
    return string_view_type(buf_matrix_.row(pos_in_buf_));
}


//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void
str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::go_to(
   typename str_sparse_vector<CharType, BV, STR_SIZE>::size_type pos
   ) BMNOEXCEPT
{
    pos_ = (!sv_ || pos >= sv_->size()) ? bm::id_max : pos;
    pos_in_buf_ = ~size_type(0);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void
str_sparse_vector<CharType, BV, STR_SIZE>::const_iterator::advance() BMNOEXCEPT
{
    if (pos_ == bm::id_max) // nothing to do, we are at the end
        return;
    ++pos_;
    
    if (pos_ >= sv_->size())
        this->invalidate();
    else
    {
        if (pos_in_buf_ != ~size_type(0))
        {
            ++pos_in_buf_;
            if (pos_in_buf_ >= n_rows)
                pos_in_buf_ = ~size_type(0);
        }
    }
}

//---------------------------------------------------------------------
//
//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::back_insert_iterator() BMNOEXCEPT
: sv_(0), bv_null_(0), pos_in_buf_(~size_type(0))
{}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::back_insert_iterator(
           str_sparse_vector<CharType, BV, STR_SIZE>* sv) BMNOEXCEPT
: sv_(sv), pos_in_buf_(~size_type(0))
{
    if (sv)
    {
        prev_nb_ = sv_->size() >> bm::set_block_shift;
        bv_null_ = sv_->get_null_bvect();
        unsigned esize = (unsigned) sv_->effective_max_str();
        if (esize < STR_SIZE)
            esize = STR_SIZE;
        buf_matrix_.init_resize(n_buf_size, esize);
    }
    else
    {
        bv_null_ = 0; prev_nb_ = 0;
    }
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::back_insert_iterator(
const str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator& bi) BMNOEXCEPT
: sv_(bi.sv_), bv_null_(bi.bv_null_), buf_matrix_(bi.buf_matrix_.rows(), bi.buf_matrix_.cols()),
  pos_in_buf_(~size_type(0)), prev_nb_(bi.prev_nb_), opt_mode_(bi.opt_mode_),
  remap_flags_(bi.remap_flags_), omatrix_(bi.omatrix_)
{
    BM_ASSERT(bi.empty());
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::~back_insert_iterator()
{
    this->flush();
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
bool
str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::empty()
                                                             const BMNOEXCEPT
{
    return (pos_in_buf_ == ~size_type(0) || !sv_);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::flush()
{
    flush_impl();
    if (remap_flags_)
    {
        buf_matrix_.free();
        sv_->remap(*this);
        remap_flags_ = 0;
    }
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::flush_impl()
{
    if (this->empty())
        return;

    size_type imp_idx = sv_->size();
    sv_->import_no_check(buf_matrix_, imp_idx, pos_in_buf_+1, false);
    pos_in_buf_ = ~size_type(0);
    block_idx_type nb = sv_->size() >> bm::set_block_shift;
    if (nb != prev_nb_)
    {
        sv_->optimize_block(prev_nb_, opt_mode_);
        prev_nb_ = nb;
    }
}


//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::add(
const typename str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::value_type* v)
{
    if (!v)
    {
        this->add_null();
        return;
    }
    size_type buf_idx = this->pos_in_buf_; // offset in
    size_type sz = sv_->size();

    this->add_value(v);
    if (bv_null_)
        bv_null_->set_bit_no_check(sz + buf_idx + 1);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::add_null()
{
    /*size_type buf_idx = */this->add_value("");
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::add_null(
typename str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::size_type count)
{
    for (size_type i = 0; i < count; ++i) // TODO: optimization
        this->add_value("");
}

//---------------------------------------------------------------------


template<class CharType, class BV, unsigned STR_SIZE>
void
str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::add_remap_stat(
const str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::value_type* v)
{
    BM_ASSERT(remap_flags_);

    size_t slen = ::strlen(v);

    auto orows = omatrix_.rows();
    if (slen > orows)
    {
        if (!orows)
        {
            omatrix_.resize(slen, 256, false);
            omatrix_.set_zero();
        }
        else
        {
            omatrix_.resize(slen, 256, true);
            for (; orows < omatrix_.rows(); ++orows)
            {
                typename
                octet_freq_matrix_type::value_type* r = omatrix_.row(orows);
                ::memset(r, 0, 256 * sizeof(r[0]));
            } // for orows
        }
    }
    for (size_t i = 0; i < slen; ++i)
    {
        value_type ch = v[i];
        typename
        octet_freq_matrix_type::value_type* row = omatrix_.row(i);
        unsigned ch_idx = (unsigned char)ch;
        row[ch_idx] += 1;
    } // for i
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void
str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::add_value(
const str_sparse_vector<CharType, BV, STR_SIZE>::back_insert_iterator::value_type* v)
{
    BM_ASSERT(sv_);
    BM_ASSERT(v);
    BM_ASSERT(buf_matrix_.rows()>0);

    if (pos_in_buf_ >= buf_matrix_.rows()-1)
    {
        if (pos_in_buf_ == ~size_type(0) && (!buf_matrix_.is_init()))
            buf_matrix_.init();
        else
            this->flush_impl();
        pos_in_buf_ = 0; buf_matrix_.set_zero();
    }
    else
    {
        ++pos_in_buf_;
    }

    if (remap_flags_)
        add_remap_stat(v);

    value_type* r = buf_matrix_.row(pos_in_buf_);

    typename buffer_matrix_type::size_type i;
    typename buffer_matrix_type::size_type cols = buf_matrix_.cols();
    for (i = 0; i < cols; ++i)
    {
        r[i] = v[i];
        if (!r[i])
            return;
    } // for i

    // string is longer than the initial size, matrix resize is needed
    for (cols = i; true; ++cols) // find the new length
    {
        if (!v[cols])
            break;
    } // for cols

    // cols is now string length and the new mattrix size parameter
    buf_matrix_.resize(buf_matrix_.rows(), cols + 1);

    r = buf_matrix_.row(pos_in_buf_);
    cols = buf_matrix_.cols();
    for (; i < cols; ++i)
    {
        r[i] = v[i];
        if (!r[i])
            return;
    } // for i
    BM_ASSERT(0);
}

//---------------------------------------------------------------------

template<class CharType, class BV, unsigned STR_SIZE>
void str_sparse_vector<CharType, BV, STR_SIZE>::sync_size() BMNOEXCEPT
{
    const bvector_type* bv_null = this->get_null_bvector();
    if (!bv_null)
        return;
    bool found = bv_null->find_reverse(this->size_);
    this->size_ += found;
}

//---------------------------------------------------------------------

} // namespace

#endif
