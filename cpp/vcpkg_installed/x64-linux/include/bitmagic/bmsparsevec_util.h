#ifndef BMSPARSEVEC_UTIL_H__INCLUDED__
#define BMSPARSEVEC_UTIL_H__INCLUDED__
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

#include <memory.h>
#include <stdexcept>

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration 
// #include "bm.h" or "bm64.h" explicitly 
# error missing include (bm.h or bm64.h)
#endif

#include "bmserial.h"
#include "bmdef.h"

namespace bm
{



/*!
    \brief Bit-bector prefix sum address resolver using bit-vector prefix sum
    as a space compactor.
 
    @internal
*/
template<class BV>
class bvps_addr_resolver
{
public:
    typedef BV                                       bvector_type;
    typedef typename BV::size_type                   size_type;
    typedef typename bvector_type::rs_index_type     rs_index_type;
public:
    bvps_addr_resolver();
    ~bvps_addr_resolver();
    bvps_addr_resolver(const bvps_addr_resolver& addr_res);
    
    bvps_addr_resolver& operator=(const bvps_addr_resolver& addr_res)
    {
        if (this != &addr_res)
        {
            addr_bv_ = addr_res.addr_bv_;
            in_sync_ = addr_res.in_sync_;
            if (in_sync_)
            {
                rs_index_->copy_from(*addr_res.rs_index_);
            }
        }
        return *this;
    }
    
    /*!
        \brief Move content from the argument address resolver
    */
    void move_from(bvps_addr_resolver& addr_res) BMNOEXCEPT;
    
    /*!
        \brief Resolve id to integer id (address)
     
        Resolve id to address with full sync and range checking
     
        \param id_from - input id to resolve
        \param id_to   - output id
     
        \return true if id is known and resolved successfully
    */
    bool resolve(size_type id_from, size_type* id_to) const BMNOEXCEPT;
    
    /*!
        \brief Resolve id to integer id (address) without sync check
     
        If prefix sum table is not sync - unexpected behavior
     
        \param id_from - input id to resolve
        \param id_to   - output id
     
        \return true if id is known and resolved successfully
    */
    bool get(size_type id_from, size_type* id_to) const BMNOEXCEPT;
    
    /*!
        \brief Set id (bit) to address resolver
    */
    void set(size_type id_from);
    
    /*!
        \brief Re-calculate prefix sum table
        \param force - force recalculation even if it is already recalculated
    */
    void calc_prefix_sum(bool force = false);
    
    /*!
        \brief Re-calculate prefix sum table (same as calc_prefix_sum)
        \param force - force recalculation even if it is already recalculated
        @sa calc_prefix_sum
    */
    void sync(bool force = false) { calc_prefix_sum(force); }
    
    /*!
        \brief returns true if prefix sum table is in sync with the vector
    */
    bool in_sync() const { return in_sync_; }
    
    /*!
        \brief Unsync the prefix sum table
    */
    void unsync() { in_sync_ = false; }
    
    /*!
        \brief Get const reference to the underlying bit-vector
    */
    const bvector_type& get_bvector() const { return addr_bv_; }

    /*!
        \brief Get writable reference to the underlying bit-vector
     
        Access to underlying vector assumes modification and
        loss of prefix sum acceleration structures.
        Need to call sync() at the end of transaction.
    */
    bvector_type& get_bvector()  { unsync(); return addr_bv_; }
    
    /*!
        \brief optimize underlying bit-vector
    */
    void optimize(bm::word_t* temp_block = 0);
    
    /*!
        \brief equality comparison
    */
    bool equal(const bvps_addr_resolver& addr_res) const BMNOEXCEPT;
    
protected:
    void construct_rs_index();
    void free_rs_index();
    
private:
    bvector_type               addr_bv_;   ///< bit-vector for id translation
    rs_index_type*             rs_index_;  ///< rank translation index
    bool                       in_sync_;   ///< flag if prefix sum is in-sync with vector
};


/*!
    \brief sparse vector based address resolver
    (no space compactor, just bit-plane compressors provided by sparse_vector)
 
    @internal
*/
template<class SV>
class sv_addr_resolver
{
public:
    typedef SV                                    sparse_vector_type;
    typedef typename SV::bvector_type             bvector_type;
    typedef typename bvector_type::size_type      size_type;
public:
    sv_addr_resolver();
    sv_addr_resolver(const sv_addr_resolver& addr_res);
    
    /*!
        \brief Resolve id to integer id (address)
     
        \param id_from - input id to resolve
        \param id_to   - output id
     
        \return true if id is known and resolved successfully
    */
    bool resolve(size_type id_from, size_type* id_to) const;
    
    /*!
        \brief Resolve id to integer id (address)
     
        \param id_from - input id to resolve
        \param id_to   - output id
     
        \return true if id is known and resolved successfully
    */
    bool get(size_type id_from, size_type* id_to) const;
    
    /*!
        \brief Set id (bit) to address resolver
    */
    void set(size_type id_from);
    
    /*!
        \brief optimize underlying sparse vectors
    */
    void optimize(bm::word_t* temp_block = 0);

    /*!
    \brief Get const reference to the underlying bit-vector of set values
    */
    const bvector_type& get_bvector() const { return set_flags_bv_; }

private:
    bvector_type              set_flags_bv_;   ///< bit-vector of set flags
    sparse_vector_type        addr_sv_;     ///< sparse vector for address map
    size_type                 max_addr_;    ///< maximum allocated address/index
};


/**
    \brief Compressed (sparse collection of objects)
    @internal
*/
template<class Value, class BV>
class compressed_collection
{
public:
    typedef BV                                   bvector_type;
    typedef typename BV::size_type               size_type;
    typedef size_type                            key_type;
    typedef size_type                            address_type;
    typedef Value                                value_type;
    typedef Value                                mapped_type;
    typedef std::vector<value_type>              container_type;
    typedef bm::bvps_addr_resolver<bvector_type> address_resolver_type;
    
public:
    compressed_collection();
    
    /**
        Add new value to compressed collection.
        Must be added in sorted key order (growing).
     
        Unsorted will not be added!
     
        \return true if value was added (does not violate sorted key order)
    */
    bool push_back(key_type key, const value_type& val);
    
    /**
        find and return const associated value (with bounds/presense checking)
    */
    const value_type& at(key_type key) const;

    /**
        find and return associated value (with bounds/presense checking)
    */
    value_type& at(key_type key);

    /** Checkpoint method to prepare collection for reading
    */
    void sync();
    
    /** perform memory optimizations/compression
    */
    void optimize(bm::word_t* temp_block=0);

    /** Resolve key address (index) in the dense vector
    */
    bool resolve(key_type key, address_type* addr) const;
    
    /** Get access to associated value by resolved address
    */
    const value_type& get(address_type addr) const;
    
    /** Get address resolver
    */
    const address_resolver_type& resolver() const { return addr_res_; }

    /** Get address resolver
    */
    address_resolver_type& resolver() { return addr_res_; }

    /** size of collection
    */
    size_t size() const { return dense_vect_.size(); }
    
    /** perform equality comparison with another collection
    */
    bool equal(const compressed_collection<Value, BV>& ccoll) const;
    
    /** return dense container for direct access
        (this should be treated as an internal function designed for deserialization)
    */
    container_type& container() { return dense_vect_; }
    
protected:
    void throw_range_error(const char* err_msg) const;
    
protected:
    address_resolver_type             addr_res_;    ///< address resolver
    container_type                    dense_vect_;  ///< compressed space container
    key_type                          last_add_;    ///< last added element
};

/**
    \brief Compressed (sparse collection of objects)
    @internal
*/
template<class BV>
class compressed_buffer_collection :
               public compressed_collection<typename serializer<BV>::buffer, BV>
{
public:
    typedef BV                                  bvector_type;
    typedef typename serializer<BV>::buffer     buffer_type;
    typedef compressed_collection<typename serializer<BV>::buffer, BV> parent_type;
    
    /// collection statistics
    struct statistics
    {
        size_t memory_used;       ///< total capacity
        size_t max_serialize_mem; ///< memory needed for serialization
    };
public:

    /// move external buffer into collection
    ///
    bool move_buffer(typename parent_type::key_type key, buffer_type& buffer)
    {
        bool added = this->push_back(key, buffer_type());
        if (!added)
            return added;
        buffer_type& buf = this->at(key);
        buf.swap(buffer);
        return added;
    }
    
    /// compute statistics on memory consumption
    ///
    void calc_stat(statistics* st) const
    {
        BM_ASSERT(st);
        
        // evaluate address resolver consumption
        //
        typename BV::statistics  bv_st;
        const BV& addr_bv = this->addr_res_.get_bvector();
        addr_bv.calc_stat(&bv_st);
        st->memory_used = bv_st.memory_used;
        st->max_serialize_mem = bv_st.max_serialize_mem;
        
        // sum-up all buffers
        for (size_t i = 0; i < this->dense_vect_.size(); ++i)
        {
            const buffer_type& buf = this->dense_vect_.at(i);
            st->memory_used += buf.capacity();
            st->max_serialize_mem += buf.size();
        } // for i
        
        // header needs
        size_t h_size = 2 + 1 + ((this->dense_vect_.size()+1) * 8);
        st->max_serialize_mem += h_size;
        
        // 10% extra on top (safety) for serialization
        size_t extra_mem = (st->max_serialize_mem / 10);
        if (!extra_mem)
            extra_mem = 4096;
        st->max_serialize_mem += extra_mem;
    }
};



//---------------------------------------------------------------------

template<class BV>
bvps_addr_resolver<BV>::bvps_addr_resolver()
: rs_index_(0), in_sync_(false)
{
    addr_bv_.init();
    construct_rs_index();
}

//---------------------------------------------------------------------

template<class BV>
bvps_addr_resolver<BV>::~bvps_addr_resolver()
{
    free_rs_index();
}

//---------------------------------------------------------------------

template<class BV>
void bvps_addr_resolver<BV>::construct_rs_index()
{
    if (rs_index_)
        return;
    rs_index_ =
        (rs_index_type*) bm::aligned_new_malloc(sizeof(rs_index_type));
    rs_index_ = new(rs_index_) rs_index_type(); // placement new
}

//---------------------------------------------------------------------

template<class BV>
void bvps_addr_resolver<BV>::free_rs_index()
{
    if (rs_index_)
    {
        rs_index_->~rs_index();
        bm::aligned_free(rs_index_);
        rs_index_ = 0;
    }
}

//---------------------------------------------------------------------

template<class BV>
bvps_addr_resolver<BV>::bvps_addr_resolver(const bvps_addr_resolver& addr_res)
: addr_bv_(addr_res.addr_bv_),
  in_sync_(addr_res.in_sync_)
{
    rs_index_ = 0;
    construct_rs_index();

    if (in_sync_)
    {
        rs_index_->copy_from(*addr_res.rs_index_);
    }
    addr_bv_.init();
}

//---------------------------------------------------------------------


template<class BV>
void bvps_addr_resolver<BV>::move_from(bvps_addr_resolver& addr_res) BMNOEXCEPT
{
    if (this != &addr_res)
    {
        addr_bv_.move_from(addr_res.addr_bv_);
        in_sync_ = addr_res.in_sync_;
        if (in_sync_)
        {
            free_rs_index();
            rs_index_ = addr_res.rs_index_;
            addr_res.rs_index_ = 0;
        }
    }
    else
    {
        rs_index_ = 0; in_sync_ = false;
    }
}

//---------------------------------------------------------------------

template<class BV>
bool bvps_addr_resolver<BV>::resolve(size_type id_from,
                                    size_type* id_to) const BMNOEXCEPT
{
    BM_ASSERT(id_to);
    if (in_sync_)
    {
        *id_to = addr_bv_.count_to_test(id_from, *rs_index_);
    }
    else  // slow access
    {
        bool found = addr_bv_.test(id_from);
        if (!found)
        {
            *id_to = 0;
        }
        else
        {
            *id_to = addr_bv_.count_range(0, id_from);
        }
    }
    return (bool) *id_to;
}

//---------------------------------------------------------------------

template<class BV>
bool bvps_addr_resolver<BV>::get(size_type id_from,
                                 size_type* id_to) const BMNOEXCEPT
{
    BM_ASSERT(id_to);
    BM_ASSERT(in_sync_);

    *id_to = addr_bv_.count_to_test(id_from, *rs_index_);
    return (bool)*id_to;
}

//---------------------------------------------------------------------

template<class BV>
void bvps_addr_resolver<BV>::set(size_type id_from)
{
    BM_ASSERT(!addr_bv_.test(id_from));
    
    in_sync_ = false;
    addr_bv_.set(id_from);
}


//---------------------------------------------------------------------


template<class BV>
void bvps_addr_resolver<BV>::calc_prefix_sum(bool force)
{
    if (in_sync_ && force == false)
        return;  // nothing to do
    
    addr_bv_.build_rs_index(rs_index_); // compute popcount prefix list
    in_sync_ = true;
}

//---------------------------------------------------------------------

template<class BV>
void bvps_addr_resolver<BV>::optimize(bm::word_t* temp_block)
{
    addr_bv_.optimize(temp_block);
}

//---------------------------------------------------------------------

template<class BV>
bool bvps_addr_resolver<BV>::equal(
                    const bvps_addr_resolver& addr_res) const BMNOEXCEPT
{
    return addr_bv_.equal(addr_res.addr_bv_);
}

//---------------------------------------------------------------------
//
//---------------------------------------------------------------------



template<class SV>
sv_addr_resolver<SV>::sv_addr_resolver()
: max_addr_(0)
{
    set_flags_bv_.init();
}

//---------------------------------------------------------------------

template<class SV>
sv_addr_resolver<SV>::sv_addr_resolver(const sv_addr_resolver& addr_res)
: set_flags_bv_(addr_res.set_flags_bv_),
  addr_sv_(addr_res.addr_sv_),
  max_addr_(addr_res.max_addr_)
{
}

//---------------------------------------------------------------------

template<class SV>
bool sv_addr_resolver<SV>::resolve(size_type id_from, size_type* id_to) const
{
    BM_ASSERT(id_to);
    
    bool found = set_flags_bv_.test(id_from);
    *id_to = found ? addr_sv_.at(id_from) : 0;
    return found;
}

//---------------------------------------------------------------------

template<class SV>
void sv_addr_resolver<SV>::set(size_type id_from)
{
    bool found = set_flags_bv_.test(id_from);
    if (!found)
    {
        set_flags_bv_.set(id_from);
        ++max_addr_;
        addr_sv_.set(id_from, max_addr_);
    }
}

//---------------------------------------------------------------------

template<class SV>
void sv_addr_resolver<SV>::optimize(bm::word_t* temp_block)
{
    set_flags_bv_.optimize(temp_block);
    addr_sv_.optimize(temp_block);
}

//---------------------------------------------------------------------


template<class Value, class BV>
compressed_collection<Value, BV>::compressed_collection()
: last_add_(0)
{
}

//---------------------------------------------------------------------

template<class Value, class BV>
bool compressed_collection<Value, BV>::push_back(key_type key, const value_type& val)
{
    if (dense_vect_.empty()) // adding first one
    {
    }
    else
    if (key <= last_add_)
    {
        BM_ASSERT(0);
        return false;
    }
    
    addr_res_.set(key);
    dense_vect_.push_back(val);
    last_add_ = key;
    return true;
}

//---------------------------------------------------------------------

template<class Value, class BV>
void compressed_collection<Value, BV>::sync()
{
    addr_res_.sync();
}

//---------------------------------------------------------------------

template<class Value, class BV>
void compressed_collection<Value, BV>::optimize(bm::word_t* temp_block)
{
    addr_res_.optimize(temp_block);
}

//---------------------------------------------------------------------

template<class Value, class BV>
bool compressed_collection<Value, BV>::resolve(key_type key,
                                               address_type* addr) const
{
    bool found = addr_res_.resolve(key, addr);
    *addr -= found;
    return found;
}

//---------------------------------------------------------------------


template<class Value, class BV>
const typename compressed_collection<Value, BV>::value_type&
compressed_collection<Value, BV>::get(address_type addr) const
{
    return dense_vect_.at(addr);
}

//---------------------------------------------------------------------

template<class Value, class BV>
const typename compressed_collection<Value, BV>::value_type&
compressed_collection<Value, BV>::at(key_type key) const
{
    address_type idx;
    bool found = addr_res_.resolve(key, &idx);
    if (!found)
    {
        throw_range_error("compressed collection item not found");
    }
    return get(idx-1);
}

//---------------------------------------------------------------------

template<class Value, class BV>
typename compressed_collection<Value, BV>::value_type&
compressed_collection<Value, BV>::at(key_type key)
{
    address_type idx;
    bool found = addr_res_.resolve(key, &idx);
    if (!found)
    {
        throw_range_error("compressed collection item not found");
    }
    return dense_vect_.at(idx-1);
}


//---------------------------------------------------------------------

template<class Value, class BV>
void compressed_collection<Value, BV>::throw_range_error(const char* err_msg) const
{
#ifndef BM_NO_STL
    throw std::range_error(err_msg);
#else
    BM_ASSERT_THROW(false, BM_ERR_RANGE);
#endif
}

//---------------------------------------------------------------------

template<class Value, class BV>
bool compressed_collection<Value, BV>::equal(
                     const compressed_collection<Value, BV>& ccoll) const
{
    const bvector_type& bv = addr_res_.get_bvector();
    const bvector_type& bva = ccoll.addr_res_.get_bvector();
    
    int cmp = bv.compare(bva);
    if (cmp != 0)
        return false;
    BM_ASSERT(dense_vect_.size() == ccoll.dense_vect_.size());
    for (size_t i = 0; i < dense_vect_.size(); ++i)
    {
        const value_type& v = dense_vect_[i];
        const value_type& va = ccoll.dense_vect_[i];
        if (!(v == va))
            return false;
    }
    return true;
}

//---------------------------------------------------------------------


} // namespace bm



#endif
