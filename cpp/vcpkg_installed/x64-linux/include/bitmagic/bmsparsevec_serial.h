#ifndef BMSPARSEVEC_SERIAL__H__INCLUDED__
#define BMSPARSEVEC_SERIAL__H__INCLUDED__
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

/*! \file bmsparsevec_serial.h
    \brief Serialization for sparse_vector<>
*/


#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration 
// #include "bm.h" or "bm64.h" explicitly 
# error missing include (bm.h or bm64.h)
#endif

#include "bmsparsevec.h"
#include "bmserial.h"
#include "bmbuffer.h"
#include "bmdef.h"

namespace bm
{

/** \defgroup svserial Sparse vector serialization
    Sparse vector serialization
    \ingroup svector
 */


/*!
    \brief layout class for serialization buffer structure
    
    Class keeps a memory block sized for the target sparse vector BLOB.
    This class also provides acess to bit-plane memory, so it becomes possible
    to use parallel storage methods to save bit-planes into
    different storage shards.
    
    \ingroup svserial
*/
template<class SV>
struct sparse_vector_serial_layout
{
    typedef typename SV::value_type   value_type;
    typedef typename SV::bvector_type bvector_type;
    typedef typename bvector_type::allocator_type allocator_type;
    typedef typename serializer<bvector_type>::buffer buffer_type;

    sparse_vector_serial_layout() BMNOEXCEPT {}
    
    ~sparse_vector_serial_layout() {}
    
    /*!
        \brief resize capacity
        \param capacity - new capacity
        \return new buffer or 0 if failed
    */
    unsigned char* reserve(size_t capacity)
    {
        if (capacity == 0)
        {
            freemem();
            return 0;
        }
        buf_.reinit(capacity);
        return buf_.data();
    }
    
    /// return current serialized size
    size_t  size() const BMNOEXCEPT { return buf_.size();  }
    
    /// Set new serialized size
    void resize(size_t ssize) { buf_.resize(ssize);  }
    
    /// return serialization buffer capacity
    size_t  capacity() const BMNOEXCEPT { return buf_.capacity(); }
    
    /// free memory
    void freemem() BMNOEXCEPT { buf_.release(); }
    
    /// Set plane output pointer and size
    void set_plane(unsigned i, unsigned char* ptr, size_t buf_size) BMNOEXCEPT
    {
        plane_ptrs_[i] = ptr;
        plane_size_[i] = buf_size;
    }
    
    /// Get plane pointer
    const unsigned char* get_plane(unsigned i) const BMNOEXCEPT
        { return plane_ptrs_[i]; }
    
    /// Return serialization buffer pointer
    const unsigned char* buf() const BMNOEXCEPT { return buf_.buf();  }
    /// Return serialization buffer pointer
    const unsigned char* data() const BMNOEXCEPT { return buf_.buf();  }

    /// Resize for the target number of plains / bit-slices
    void resize_slices(unsigned new_slices_size)
    {
        plane_ptrs_.resize(new_slices_size);
        plane_size_.resize(new_slices_size);
    }

private:
    sparse_vector_serial_layout(const sparse_vector_serial_layout&);
    void operator=(const sparse_vector_serial_layout&);
protected:
    typedef bm::heap_vector<unsigned char*, allocator_type, true> ptr_vector_type;
    typedef bm::heap_vector<size_t, allocator_type, true> sizet_vector_type;


    buffer_type       buf_;                       ///< serialization buffer
    ptr_vector_type   plane_ptrs_; ///< pointers on serialized bit-planes
    sizet_vector_type plane_size_; ///< serialized plane size
//    unsigned char* plane_ptrs_[SV::sv_slices]; ///< pointers on serialized bit-planes
//    size_t         plane_size_[SV::sv_slices]; ///< serialized plane size
};

// -------------------------------------------------------------------------

/*!
    \brief Serialize sparse vector into a memory buffer(s) structure
 
 Serialization format:

 | HEADER | BIT-VECTORS ... | REMAP_MATRIX

 Header structure:
 -----------------
   BYTE+BYTE: Magic-signature 'BM' or 'BC' (c-compressed)
   BYTE : Byte order ( 0 - Big Endian, 1 - Little Endian)
   {
        BYTE : Number of Bit-vector planes (total) (non-zero when < 255 planes)
        |
        BYTE: zero - flag of large plane matrix
        INT64: Nnmber of bit-vector planes
   }
   INT64: Vector size
   INT64: Offset of plane 0 from the header start (value 0 means plane is empty)
   INT64: Offset of plane 1 from
   ...
   INT32: reserved
 
Bit-vectors:
------------
   Based on current bit-vector serialization

Remap Matrix:
   SubHeader | Matrix BLOB
 
   sub-header:
     BYTE:  'R' (remapping) or 'N' (no remapping)
             N - means no other info is saved on the stream
     INT64:  remap matrix size
 
    \ingroup svector
    \ingroup svserial
*/
template<typename SV>
class sparse_vector_serializer
{
public:
    typedef typename SV::bvector_type       bvector_type;
    typedef const bvector_type*             bvector_type_const_ptr;
    typedef bvector_type*                   bvector_type_ptr;
    typedef typename SV::value_type         value_type;
    typedef typename SV::size_type          size_type;
    typedef typename bvector_type::allocator_type       alloc_type;
    typedef typename alloc_type::allocator_pool_type    allocator_pool_type;
    typedef typename
    bm::serializer<bvector_type>::bv_ref_vector_type bv_ref_vector_type;
    typedef typename
    bm::serializer<bvector_type>::xor_sim_model_type xor_sim_model_type;


public:
    sparse_vector_serializer();


    /*! @name Compression settings                               */
    ///@{

    /**
        Add skip-markers for faster range deserialization

        @param enable - TRUE searilization will add bookmark codes
        @param bm_interval - bookmark interval in (number of blocks)
                            (suggested between 4 and 512)
        smaller interval means more bookmarks added to the skip list thus
        more increasing the BLOB size
    */
    void set_bookmarks(bool enable, unsigned bm_interval = 256) BMNOEXCEPT
        { bvs_.set_bookmarks(enable, bm_interval); }

    /**
        Enable XOR compression on vector serialization
        @sa set_xor_ref
        @sa disable_xor_compression
     */
    void enable_xor_compression() BMNOEXCEPT
        { set_xor_ref(true); }

    /**
        Disable XOR compression on serialization
     */
    void disable_xor_compression() BMNOEXCEPT
        { set_xor_ref((const bv_ref_vector_type*)0); }

    /** Turn ON and OFF XOR compression of sparse vectors
        Enables XOR reference compression for the sparse vector.
        Default: disabled
        Reference bit-vectors from the sparse vector itself
    */
    void set_xor_ref(bool is_enabled) BMNOEXCEPT;

    /** Enable external XOR serialization via external reference vectors
       (data frame ref. vector).
       This method is useful when we serialize a group of related
       sparse vectors which benefits from the XOR referencial compression

       @param bv_ref_ptr - external reference vector
       if NULL - resets the use of reference vector
    */
    void set_xor_ref(const bv_ref_vector_type* bv_ref_ptr) BMNOEXCEPT;

    /**
        Calculate XOR similarity model for ref_vector
        refernece vector must be associated before
        @sa set_ref_vectors, set_sim_model
        @internal
     */
    void compute_sim_model(xor_sim_model_type& sim_model,
                           const bv_ref_vector_type& ref_vect,
                           const bm::xor_sim_params& params);

    /**
        Attach serizalizer to a pre-computed similarity model
        @param sim_model - pointer to external computed model
     */
    void set_sim_model(const xor_sim_model_type* sim_model) BMNOEXCEPT;

    /**
        Returns the XOR reference compression status (enabled/disabled)
    */
    bool is_xor_ref() const BMNOEXCEPT { return is_xor_ref_; }

    ///@}

    /*! @name Serialization                                     */
    ///@{

    /*!
        \brief Serialize sparse vector into a memory buffer(s) structure
     
        \param sv                 - sparse vector to serialize
        \param sv_layout  - buffer structure to keep the result
        as defined in bm::serialization_flags
    */
    void serialize(const SV&                        sv,
                   sparse_vector_serial_layout<SV>& sv_layout);

    /** Get access to the underlying bit-vector serializer
        This access can be used to fine tune compression settings
        @sa bm::serializer::set_compression_level
    */
    bm::serializer<bvector_type>& get_bv_serializer() BMNOEXCEPT
        { return bvs_; }
        
    ///@}


protected:
    void build_xor_ref_vector(const SV& sv);

    static
    void build_plane_digest(bvector_type& digest_bv, const SV& sv);

    typedef typename SV::remap_matrix_type  remap_matrix_type;

    /// serialize the remap matrix used for SV encoding
    void encode_remap_matrix(bm::encoder& enc, const SV& sv);

    typedef bm::heap_vector<unsigned, alloc_type, true> u32_vector_type;
    typedef bm::serializer<bvector_type>                serializer_type;
    typedef typename serializer_type::buffer            buffer_type;
private:
    sparse_vector_serializer(const sparse_vector_serializer&) = delete;
    sparse_vector_serializer& operator=(const sparse_vector_serializer&) = delete;

protected:
    bm::serializer<bvector_type>     bvs_;

    bvector_type                     plane_digest_bv_; ///< bv.digest of bit-planes
    buffer_type                      plane_digest_buf_; ///< serialization buf
    u32_vector_type                  plane_off_vect_;

    u32_vector_type                  remap_rlen_vect_;
    // XOR compression member vars
    bool                             is_xor_ref_;
    bv_ref_vector_type               bv_ref_;
    xor_sim_model_type               sim_model_;
    const bv_ref_vector_type*        bv_ref_ptr_;
    const xor_sim_model_type*        sim_model_ptr_;
};

/**
    sparse vector de-serializer

*/
template<typename SV>
class sparse_vector_deserializer
{
public:
    typedef typename SV::bvector_type       bvector_type;
    typedef const bvector_type*             bvector_type_const_ptr;
    typedef bvector_type*                   bvector_type_ptr;
    typedef typename SV::value_type         value_type;
    typedef typename SV::size_type          size_type;
    typedef typename bvector_type::allocator_type::allocator_pool_type allocator_pool_type;
    typedef typename bm::serializer<bvector_type>::bv_ref_vector_type bv_ref_vector_type;

public:
    sparse_vector_deserializer();
    ~sparse_vector_deserializer();

    /**
        Set deserialization finalization to force deserialized vectors into READONLY (or READWRITE) mode.
        Performance impact: Turning ON finalization will make deserialization a lit slower,
        because each bit-vector will be re-converted into new mode (READONLY).
        Following (search) operations may perform a bit faster.

        @param is_final - finalization code
                        (use bm::finalization::READONLY to produce an immutable vector)
     */
    void set_finalization(bm::finalization is_final);


    /** Set external XOR reference vectors
        (data frame referenece vectors)

        @param bv_ref_ptr - external reference vector
        if NULL - resets the use of reference
    */
    void set_xor_ref(bv_ref_vector_type* bv_ref_ptr);

    /*!
        Deserialize sparse vector

        @param sv - [out] target sparse vector to populate
        @param buf - input BLOB source memory pointer
        @param clear_sv - if true clears the target vector (sv)

        @sa deserialize_range
    */
    void deserialize(SV& sv,
                     const unsigned char* buf,
                     bool clear_sv = true);

    /*!
        Deserialize sparse vector for the range [from, to]

        @param sv - [out] target sparse vector to populate
        @param buf - input BLOB  source memory pointer
        @param from - start vector index for deserialization range
        @param to - end vector index for deserialization range
        @param clear_sv - if true clears the target vector

    */
    void deserialize_range(SV& sv, const unsigned char* buf,
                           size_type from, size_type to,
                           bool clear_sv = true);

    /*!
        Better use deserialize_range()
        @sa deserialize_range
    */
    void deserialize(SV& sv, const unsigned char* buf,
                     size_type from, size_type to)
    {
        deserialize_range(sv, buf, from, to);
    }



    /*!
        Deserialize sparse vector using address mask vector
        Address mask defines (by set bits) which vector elements to be extracted
        from the compressed BLOB

        @param sv - [out] target sparse vector to populate
        @param buf - source memory pointer
        @param mask_bv - AND mask bit-vector (address gather vector)
    */
    void deserialize(SV& sv,
                     const unsigned char* buf,
                     const bvector_type& mask_bv)
    { idx_range_set_ = false;
      deserialize_sv(sv, buf, &mask_bv, true);
    }


    /*!
        Load serialization descriptor, create vectors but DO NOT perform full deserialization
        @param sv - [out] target sparse vector to populate
        @param buf - source memory pointer
    */
    void deserialize_structure(SV& sv,
                               const unsigned char* buf);


protected:
    typedef typename bvector_type::allocator_type      alloc_type;


    /// Deserialize header/version and other common info
    ///
    /// @return number of bit-planes
    ///
    unsigned load_header(bm::decoder& dec, SV& sv, unsigned char& matr_s_ser);

    void deserialize_sv(SV& sv, const unsigned char* buf,
                        const bvector_type* mask_bv,
                        bool clear_sv);


    /// deserialize bit-vector planes
    void deserialize_planes(SV& sv, unsigned planes,
                            const unsigned char* buf,
                            const bvector_type* mask_bv = 0);

    /// load offset table
    void load_planes_off_table(const unsigned char* buf, bm::decoder& dec, unsigned planes);

    /// load NULL bit-plane (returns new planes count)
    int load_null_plane(SV& sv,
                        int planes,
                        const unsigned char* buf,
                        const bvector_type* mask_bv);

    /// load string remap dict
    void load_remap(SV& sv, const unsigned char* remap_buf_ptr);

    /// throw error on incorrect deserialization
    static void raise_invalid_header();
    /// throw error on incorrect deserialization
    static void raise_invalid_64bit();
    /// throw error on incorrect deserialization
    static void raise_invalid_bitdepth();
    /// throw error on incorrect deserialization
    static void raise_invalid_format();
    /// throw error on incorrect deserialization
    static void raise_missing_remap_matrix();
    /// setup deserializers
    void setup_xor_compression();

    /// unset XOR compression vectors
    void clear_xor_compression();

private:
    sparse_vector_deserializer(const sparse_vector_deserializer&) = delete;
    sparse_vector_deserializer& operator=(const sparse_vector_deserializer&) = delete;

    typedef bm::heap_vector<unsigned, alloc_type, true> rlen_vector_type;

protected:
    bm::finalization                is_final_ = bm::finalization::UNDEFINED;
    const unsigned char*            remap_buf_ptr_ = 0;
    alloc_type                      alloc_;
    bm::word_t*                     temp_block_ = 0;
    allocator_pool_type             pool_;

    bvector_type                     plane_digest_bv_; // digest of bit-planes
    bm::id64_t                       sv_size_;
    bm::id64_t                       digest_offset_;

    bm::deserializer<bvector_type, bm::decoder> deserial_;
    bm::operation_deserializer<bvector_type>    op_deserial_;
    bm::rank_compressor<bvector_type>           rsc_compressor_;
    bvector_type                                not_null_mask_bv_;
    bvector_type                                rsc_mask_bv_;
    bm::heap_vector<size_t, alloc_type, true>   off_vect_;
    bm::heap_vector<unsigned, alloc_type, true>  off32_vect_;
    rlen_vector_type                            remap_rlen_vect_;

    // XOR compression variables
    bv_ref_vector_type              bv_ref_;         ///< reference vector
    bv_ref_vector_type*             bv_ref_ptr_ = 0; ///< external ref bit-vect

    // Range deserialization parameters
    bool                                        idx_range_set_ = false;
    size_type                                   idx_range_from_;
    size_type                                   idx_range_to_;
};



/*!
    \brief Serialize sparse vector into a memory buffer(s) structure
 
    \param sv         - sparse vector to serialize
    \param sv_layout  - buffer structure to keep the result
    \param temp_block - temporary buffer 
                        (allocate with BM_DECLARE_TEMP_BLOCK(x) for speed)
    
    \ingroup svserial
    
    @sa serialization_flags
    @sa sparse_vector_deserializer
*/
template<class SV>
void sparse_vector_serialize(
                const SV&                        sv,
                sparse_vector_serial_layout<SV>& sv_layout,
                bm::word_t*                      temp_block = 0)
{
    (void)temp_block;
    bm::sparse_vector_serializer<SV> sv_serializer;
//    sv_serializer.enable_xor_compression();
    sv_serializer.serialize(sv, sv_layout);
}

// -------------------------------------------------------------------------

/*!
    \brief Deserialize sparse vector
    \param sv         - target sparse vector
    \param buf        - source memory buffer
    \param temp_block - temporary block buffer to avoid re-allocations
 
    \return 0  (error processing via std::logic_error)
 
    \ingroup svserial
    @sa sparse_vector_deserializer
*/
template<class SV>
int sparse_vector_deserialize(SV& sv,
                              const unsigned char* buf,
                              bm::word_t* temp_block=0)
{
    (void)temp_block;
    bm::sparse_vector_deserializer<SV> sv_deserializer;
    sv_deserializer.deserialize(sv, buf);
    return 0;
}

// -------------------------------------------------------------------------

/**
    Seriaizer for compressed collections
*/
template<class CBC>
class compressed_collection_serializer
{
public:
    typedef CBC                                  compressed_collection_type;
    typedef typename CBC::bvector_type           bvector_type;
    typedef typename CBC::buffer_type            buffer_type;
    typedef typename CBC::statistics             statistics_type;
    typedef typename CBC::address_resolver_type  address_resolver_type;
    
public:
    void serialize(const CBC&    buffer_coll,
                   buffer_type&  buf,
                   bm::word_t*   temp_block = 0);
};

/**
    Deseriaizer for compressed collections
*/
template<class CBC>
class compressed_collection_deserializer
{
public:
    typedef CBC                                  compressed_collection_type;
    typedef typename CBC::bvector_type           bvector_type;
    typedef typename bvector_type::allocator_type allocator_type;
    typedef typename CBC::buffer_type            buffer_type;
    typedef typename CBC::statistics             statistics_type;
    typedef typename CBC::address_resolver_type  address_resolver_type;
    typedef typename CBC::container_type         container_type;

public:
    int deserialize(CBC&                 buffer_coll,
                    const unsigned char* buf,
                    bm::word_t*          temp_block=0);
};


// -------------------------------------------------------------------------

/**
    \brief Serialize compressed collection into memory buffer

Serialization format:


<pre>
 | MAGIC_HEADER | ADDRESS_BITVECTROR | LIST_OF_BUFFER_SIZES | BUFFER(s)
 
   MAGIC_HEADER:
   BYTE+BYTE: Magic-signature 'BM' or 'BC'
   BYTE : Byte order ( 0 - Big Endian, 1 - Little Endian)
 
   ADDRESS_BITVECTROR:
   INT64: address bit-vector size
   [memblock]: serialized address bit-vector
 
   LIST_OF_BUFFER_SIZES:
   INT64 - buffer sizes count
   INT32 - buffer size 0
   INT32 - buffer size 1
   ...
 
   BUFFERS:
   [memblock]: block0
   [memblock]: block1
   ...
 
</pre>
*/

template<class CBC>
void compressed_collection_serializer<CBC>::serialize(const CBC&    buffer_coll,
                                                      buffer_type&  buf,
                                                      bm::word_t*   temp_block)
{
    statistics_type st;
    buffer_coll.calc_stat(&st);

    buf.resize(st.max_serialize_mem);
    
    // ptr where bit-planes start
    unsigned char* buf_ptr = buf.data();
    
    bm::encoder enc(buf.data(), buf.capacity());
    ByteOrder bo = globals<true>::byte_order();
    enc.put_8('B');
    enc.put_8('C');
    enc.put_8((unsigned char)bo);
    
    unsigned char* mbuf1 = enc.get_pos(); // bookmark position
    enc.put_64(0);  // address vector size (reservation)

    buf_ptr = enc.get_pos();

    const address_resolver_type& addr_res = buffer_coll.resolver();
    const bvector_type& bv = addr_res.get_bvector();
    {
        bm::serializer<bvector_type > bvs(temp_block);
        bvs.gap_length_serialization(false);

        size_t addr_bv_size = bvs.serialize(bv, buf_ptr, buf.size());
        buf_ptr += addr_bv_size;

        enc.set_pos(mbuf1); // rewind to bookmark
        enc.put_64(addr_bv_size); // save the address vector size
    }
    enc.set_pos(buf_ptr); // restore stream position
    size_t coll_size = buffer_coll.size();
    
    enc.put_64(coll_size);
    
    // pass 1 (save buffer sizes)
    {
        for (unsigned i = 0; i < buffer_coll.size(); ++i)
        {
            const buffer_type& cbuf = buffer_coll.get(i);
            size_t sz = cbuf.size();
            enc.put_64(sz);
        } // for i
    }
    // pass 2 (save buffers)
    {
        for (unsigned i = 0; i < buffer_coll.size(); ++i)
        {
            const buffer_type& cbuf = buffer_coll.get(i);
            size_t sz = cbuf.size();
            enc.memcpy(cbuf.buf(), sz);
        } // for i
    }
    buf.resize(enc.size());
}

// -------------------------------------------------------------------------
template<class CBC>
int compressed_collection_deserializer<CBC>::deserialize(
                                CBC&                 buffer_coll,
                                const unsigned char* buf,
                                bm::word_t*          temp_block)
{
    // TODO: implement correct processing of byte-order corect deserialization
    //    ByteOrder bo_current = globals<true>::byte_order();
    
    bm::decoder dec(buf);
    unsigned char h1 = dec.get_8();
    unsigned char h2 = dec.get_8();

    BM_ASSERT(h1 == 'B' && h2 == 'C');
    if (h1 != 'B' && h2 != 'C')  // no magic header? issue...
    {
        return -1;
    }
    //unsigned char bv_bo =
        dec.get_8();

    // -----------------------------------------
    // restore address resolver
    //
    bm::id64_t addr_bv_size = dec.get_64();
    
    const unsigned char* bv_buf_ptr = dec.get_pos();
    
    address_resolver_type& addr_res = buffer_coll.resolver();
    bvector_type& bv = addr_res.get_bvector();
    bv.clear();
    
    bm::deserialize(bv, bv_buf_ptr, temp_block);
    addr_res.sync();
    
    typename bvector_type::size_type addr_cnt = bv.count();
    dec.seek((int)addr_bv_size);
    
    // -----------------------------------------
    // read buffer sizes
    //
    bm::id64_t coll_size = dec.get_64();
    if (coll_size != addr_cnt)
    {
        return -2; // buffer size collection does not match address vector
    }
    
    typedef size_t vect_size_type;
    bm::heap_vector<bm::id64_t, allocator_type, true> buf_size_vec;

	buf_size_vec.resize(vect_size_type(coll_size));
    {
        for (unsigned i = 0; i < coll_size; ++i)
        {
            bm::id64_t sz = dec.get_64();
            buf_size_vec[i] = sz;
        } // for i
    }

    {
        container_type& buf_vect = buffer_coll.container();
        buf_vect.resize(vect_size_type(coll_size));
        for (unsigned i = 0; i < coll_size; ++i)
        {
            bm::id64_t sz = buf_size_vec[i];
            buffer_type& b = buf_vect.at(i);
            b.resize(sz);
            dec.memcpy(b.data(), size_t(sz));
        } // for i
    }
    buffer_coll.sync();
    return 0;
}

// -------------------------------------------------------------------------
//
// -------------------------------------------------------------------------

template<typename SV>
sparse_vector_serializer<SV>::sparse_vector_serializer()
: bv_ref_ptr_(0)
{
    bvs_.gap_length_serialization(false);
    #ifdef BMXORCOMP
        is_xor_ref_ = true;
    #else
        is_xor_ref_ = false;
    #endif
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_serializer<SV>::set_xor_ref(
                          const bv_ref_vector_type* bv_ref_ptr) BMNOEXCEPT
{
    bv_ref_ptr_ = bv_ref_ptr;
    is_xor_ref_ = bool(bv_ref_ptr);
    sim_model_ptr_ = 0;
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_serializer<SV>::set_xor_ref(bool is_enabled) BMNOEXCEPT
{
    bv_ref_ptr_ = 0; // reset external ref.vector
    is_xor_ref_ = is_enabled;
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_serializer<SV>::compute_sim_model(
                                xor_sim_model_type& sim_model,
                                const bv_ref_vector_type& ref_vect,
                                const xor_sim_params& params)
{
    bvs_.compute_sim_model(sim_model, ref_vect, params);
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_serializer<SV>::set_sim_model(
                const xor_sim_model_type* sim_model) BMNOEXCEPT
{
    sim_model_ptr_ = sim_model;
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_serializer<SV>::build_xor_ref_vector(const SV& sv)
{
    //bv_ref_.reset();
    bv_ref_.build(sv.get_bmatrix());
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_serializer<SV>::encode_remap_matrix(bm::encoder& enc,
                                             const SV& sv)
{
    const typename SV::remap_matrix_type* rm = sv.get_remap_matrix();
    BM_ASSERT(rm);

    const remap_matrix_type& rmatr = *rm;

    size_t rows = rmatr.rows();
    size_t cols = rmatr.cols();

    BM_ASSERT(cols <= 256);
    BM_ASSERT(rows <= ~0u);

    // compute CSR capacity vector
    remap_rlen_vect_.resize(0);
    for (size_t r = 0; r < rows; ++r)
    {
        const unsigned char* BMRESTRICT remap_row = rmatr.row(r);
        size_t cnt = bm::count_nz(remap_row, cols);
        if (!cnt)
            break;
        remap_rlen_vect_.push_back(unsigned(cnt));
    } // for r

    rows = remap_rlen_vect_.size(); // effective rows in the remap table

    size_t csr_size_max = rows * sizeof(bm::gap_word_t);
    for (size_t r = 0; r < rows; ++r)
    {
        unsigned rl = remap_rlen_vect_[r];
        csr_size_max += rl * 2;
    } // for r

    size_t remap_size = sv.remap_size();

    if (remap_size < csr_size_max)
    {
        const unsigned char* matrix_buf = sv.get_remap_buffer();
        BM_ASSERT(matrix_buf);
        BM_ASSERT(remap_size);

        enc.put_8('R');
        enc.put_64(remap_size);
        enc.memcpy(matrix_buf, size_t(remap_size));
    }
    else
    {
        enc.put_8('C'); // Compressed sparse row (CSR)
        enc.put_32(unsigned(rows));
        enc.put_16(bm::gap_word_t(cols)); // <= 255 chars

        {
            bm::bit_out<bm::encoder> bo(enc);
            for (size_t r = 0; r < rows; ++r)
            {
                unsigned rl = remap_rlen_vect_[r];
                bo.gamma(rl);
            } // for r
        }

        for (size_t r = 0; r < rows; ++r)
        {
            const unsigned char* BMRESTRICT row = rmatr.row(r);
            for (size_t j = 0; j < cols; ++j)
            {
                unsigned char v = row[j];
                if (v)
                {
                    enc.put_8((unsigned char)j);
                    enc.put_8(v);
                }
            } // for j
        } // for r
    }

    enc.put_8('E'); // end of matrix (integrity check token)
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_serializer<SV>::build_plane_digest(bvector_type& digest_bv,
                                                      const SV& sv)
{
    digest_bv.init();
    digest_bv.clear(false);
    unsigned planes = (unsigned)sv.get_bmatrix().rows();
    for (unsigned i = 0; i < planes; ++i)
    {
        typename SV::bvector_type_const_ptr bv = sv.get_slice(i);
        if (bv)
            digest_bv.set_bit_no_check(i);
    } // for i
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_serializer<SV>::serialize(const SV&  sv,
                      sparse_vector_serial_layout<SV>&  sv_layout)
{
    bvs_.allow_stat_reset(false); // stats accumulate mode for all bit-slices
    bvs_.reset_compression_stats();

    if (!sv.size()) // special case of an empty vector
    {
        unsigned char* buf = sv_layout.reserve(4);
        buf[0]='B'; buf[1] = 'Z';
        sv_layout.resize(2);
        return;
    }

    build_plane_digest(plane_digest_bv_, sv);
    bvs_.set_ref_vectors(0); // disable possible XOR compression for offs.bv
    bvs_.serialize(plane_digest_bv_, plane_digest_buf_);

    unsigned planes = (unsigned)sv.get_bmatrix().rows();
    sv_layout.resize_slices(planes);

    // ----------------------------------------------------
    // memory pre-reservation
    //
    typename SV::statistics sv_stat;
    sv.calc_stat(&sv_stat);
    sv_stat.max_serialize_mem += plane_digest_buf_.size() + (8 * planes);
    unsigned char* buf = sv_layout.reserve(sv_stat.max_serialize_mem);

    // ----------------------------------------------------
    //
    bm::encoder enc(buf, sv_layout.capacity());

    // header size in bytes
    unsigned h_size = 1 + 1 +        // "BM" or "BC" (magic header)
                      1 +            // byte-order
                      1 +            // number of bit-planes (for vector)
                      8 +            // size (internal 64-bit)
                      8 +            // offset to digest (64-bit)
                      4; //  reserve
    // for large plane matrixes
    {
        h_size += 1 + // version number
                  8;  // number of planes (64-bit)
    }

    // ----------------------------------------------------
    // Setup XOR reference compression
    //
    if (is_xor_ref())
    {
        if (bv_ref_ptr_) // use external reference
        {
            // ref vector and similarity model, both must(!) be set
            BM_ASSERT(sim_model_ptr_);
            bvs_.set_ref_vectors(bv_ref_ptr_);
            bvs_.set_sim_model(sim_model_ptr_);
        }
        else
        {
            bm::xor_sim_params xs_params;
            build_xor_ref_vector(sv);
            bvs_.set_ref_vectors(&bv_ref_);
            if (bvs_.compute_sim_model(sim_model_, bv_ref_, xs_params))
                bvs_.set_sim_model(&sim_model_);
        }
    }

    // ----------------------------------------------------
    // Serialize all bvector planes
    //

    ::memset(buf, 0, h_size);
    unsigned char* buf_ptr = buf + h_size; // ptr where planes start (start+hdr)

    for (unsigned i = 0; i < planes; ++i)
    {
        typename SV::bvector_type_const_ptr bv = sv.get_slice(i);
        if (!bv)  // empty plane
        {
            sv_layout.set_plane(i, 0, 0);
            continue;
        }
        if (is_xor_ref())
        {
            unsigned idx;
            if (bv_ref_ptr_) // use external reference
                idx = (unsigned)bv_ref_ptr_->find_bv(bv);
            else
                idx = (unsigned)bv_ref_.find_bv(bv);
            BM_ASSERT(idx != bv_ref_.not_found());
            bvs_.set_curr_ref_idx(idx);
        }
        size_t buf_size = (size_t)
            bvs_.serialize(*bv, buf_ptr, sv_stat.max_serialize_mem);
        
        sv_layout.set_plane(i, buf_ptr, buf_size);
        buf_ptr += buf_size;
        if (sv_stat.max_serialize_mem > buf_size)
        {
            sv_stat.max_serialize_mem -= buf_size;
            continue;
        }
        BM_ASSERT(0); // TODO: throw an exception here
    } // for i

    bvs_.set_ref_vectors(0); // dis-engage XOR ref vector

    // -----------------------------------------------------
    // serialize the re-map matrix
    //
    if (bm::conditional<SV::is_remap_support::value>::test()) // test remapping trait
    {
        bm::encoder enc_m(buf_ptr, sv_stat.max_serialize_mem);
        if (sv.is_remap())
            encode_remap_matrix(enc_m, sv);
        else
            enc_m.put_8('N');
        buf_ptr += enc_m.size(); // add encoded data size
    }

    // ------------------------------------------------------
    // save the digest vector
    //
    size_t digest_offset = size_t(buf_ptr - buf); // digest position from the start
    ::memcpy(buf_ptr, plane_digest_buf_.buf(), plane_digest_buf_.size());
    buf_ptr += plane_digest_buf_.size();
    {
        bool use_64bit = false;
        plane_off_vect_.resize(0);
        for (unsigned i = 0; i < planes; ++i)
        {
            const unsigned char* p = sv_layout.get_plane(i);
            if (p)
            {
                size_t offset = size_t(p - buf);
                if (offset > bm::id_max32)
                {
                    use_64bit = true;
                    break;
                }
                plane_off_vect_.push_back(unsigned(offset)); // cast is not a bug
            }
        } // for i
        bm::encoder enc_o(buf_ptr, sv_stat.max_serialize_mem);
        if (use_64bit || (plane_off_vect_.size() < 4))
        {
            enc_o.put_8('6');
            // save the offset table as a list of 64-bit values
            //
            for (unsigned i = 0; i < planes; ++i)
            {
                const unsigned char* p = sv_layout.get_plane(i);
                if (p)
                {
                    size_t offset = size_t(p - buf);
                    enc_o.put_64(offset);
                }
            } // for
        }
        else  // searialize 32-bit offset table using BIC
        {
            BM_ASSERT(plane_off_vect_.size() == plane_digest_bv_.count());
            unsigned min_v = plane_off_vect_[0];
            unsigned max_v = plane_off_vect_[plane_off_vect_.size()-1];

            enc_o.put_8('3');
            enc_o.put_32(min_v);
            enc_o.put_32(max_v);

            bm::bit_out<bm::encoder> bo(enc_o);
            bo.bic_encode_u32_cm(plane_off_vect_.data()+1,
                                 unsigned(plane_off_vect_.size()-2),
                                 min_v, max_v);
        }
        buf_ptr += enc_o.size();
    }



    sv_layout.resize(size_t(buf_ptr - buf)); // set the true occupied size

    // -----------------------------------------------------
    // save the header
    //
    ByteOrder bo = bm::globals<true>::byte_order();
    
    enc.put_8('B');  // magic header 'BM' - bit matrix 'BC' - bit compressed
    if (sv.is_compressed())
        enc.put_8('C');
    else
        enc.put_8('M');
    
    enc.put_8((unsigned char)bo);  // byte order
    
    unsigned char matr_s_ser = 1;
#ifdef BM64ADDR
    matr_s_ser = 2;
#endif
    
    enc.put_8(0);              // number of planes == 0 (legacy magic number)
    enc.put_8(matr_s_ser);     // matrix serialization version
    {
        bm::id64_t planes_code = planes | (1ull << 63);
        enc.put_64(planes_code);        // number of rows in the bit-matrix
    }
    enc.put_64(sv.size_internal());
    enc.put_64(bm::id64_t(digest_offset));
}

// -------------------------------------------------------------------------
//
// -------------------------------------------------------------------------

template<typename SV>
sparse_vector_deserializer<SV>::sparse_vector_deserializer()
{
    temp_block_ = alloc_.alloc_bit_block();
    not_null_mask_bv_.set_allocator_pool(&pool_);
    rsc_mask_bv_.set_allocator_pool(&pool_);
}

// -------------------------------------------------------------------------

template<typename SV>
sparse_vector_deserializer<SV>::~sparse_vector_deserializer()
{
    if (temp_block_)
        alloc_.free_bit_block(temp_block_);
}

// -------------------------------------------------------------------------

template<typename SV>
void
sparse_vector_deserializer<SV>::set_finalization(bm::finalization is_final)
{
    this->is_final_ = is_final;
}

// -------------------------------------------------------------------------

template<typename SV>
void
sparse_vector_deserializer<SV>::set_xor_ref(bv_ref_vector_type* bv_ref_ptr)
{
    bv_ref_ptr_ = bv_ref_ptr;
    if (!bv_ref_ptr_)
        clear_xor_compression();
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::clear_xor_compression()
{
    op_deserial_.set_ref_vectors(0);
    deserial_.set_ref_vectors(0);
    bv_ref_.reset();
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::setup_xor_compression()
{
    if (bv_ref_ptr_)
    {
        op_deserial_.set_ref_vectors(bv_ref_ptr_);
        deserial_.set_ref_vectors(bv_ref_ptr_);
    }
    else
    {
        op_deserial_.set_ref_vectors(&bv_ref_);
        deserial_.set_ref_vectors(&bv_ref_);
    }
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::deserialize(SV& sv,
                                                 const unsigned char* buf,
                                                 bool clear_sv)
{
    idx_range_set_ = false;
    deserialize_sv(sv, buf, 0, clear_sv);
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::deserialize_structure(SV& sv,
                                            const unsigned char* buf)
{
    bm::decoder dec(buf); // TODO: implement correct processing of byte-order

    unsigned char matr_s_ser = 0;
    unsigned planes = load_header(dec, sv, matr_s_ser);
    if (planes == 0)
        return;

    load_planes_off_table(buf, dec, planes); // read the offset vector of bit-planes
    for (unsigned i = 0; i < planes; ++i)
    {
        if (!off_vect_[i]) // empty vector
            continue;
        bvector_type* bv = sv.get_create_slice(i);
        BM_ASSERT(bv); (void)bv;
    } // for i
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::deserialize_range(SV& sv,
                                                 const unsigned char* buf,
                                                 size_type from,
                                                 size_type to,
                                                 bool clear_sv)
{
    if (clear_sv)
        sv.clear_all(true);

    idx_range_set_ = true; idx_range_from_ = from; idx_range_to_ = to;

    remap_buf_ptr_ = 0;
    bm::decoder dec(buf); // TODO: implement correct processing of byte-order

    unsigned char matr_s_ser = 0;
    unsigned planes = load_header(dec, sv, matr_s_ser);

    if (!sv_size_) // empty vector
        return;

    sv.resize_internal(size_type(sv_size_));
    bv_ref_.reset();

    load_planes_off_table(buf, dec, planes); // read the offset vector of bit-planes

    setup_xor_compression();

    sv.get_bmatrix().allocate_rows(planes);

    // TODO: add range for not NULL plane
    planes = (unsigned)load_null_plane(sv, int(planes), buf, 0);

    // check if mask needs to be relaculated using the NULL (index) vector
    if (bm::conditional<SV::is_rsc_support::value>::test())
    {
        // recalculate planes range
        size_type sv_left, sv_right;
        bool range_valid = sv.resolve_range(from, to, &sv_left, &sv_right);
        if (!range_valid)
        {
            sv.clear();
            idx_range_set_ = false;
            return;
        }
        else
        {
            idx_range_set_ = true; idx_range_from_ = sv_left; idx_range_to_ = sv_right;
        }
    }

    deserialize_planes(sv, planes, buf, 0);

    clear_xor_compression();

    // load the remap matrix
    //
    if (bm::conditional<SV::is_remap_support::value>::test()) // test remap trait
    {
        if (matr_s_ser)
            load_remap(sv, remap_buf_ptr_);
    } // if remap traits

    sv.sync(true); // force sync, recalculate RS index, remap tables, etc

    remap_buf_ptr_ = 0;

    idx_range_set_ = false;
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::deserialize_sv(SV& sv,
                                                 const unsigned char* buf,
                                                 const bvector_type* mask_bv,
                                                 bool clear_sv)
{
    if (clear_sv)
        sv.clear_all(true);

    remap_buf_ptr_ = 0;
    bm::decoder dec(buf); // TODO: implement correct processing of byte-order

    unsigned char matr_s_ser = 0;
    unsigned planes = load_header(dec, sv, matr_s_ser);
    if (!sv_size_)
        return;  // empty vector
        
    sv.resize_internal(size_type(sv_size_));
    bv_ref_.reset();

    load_planes_off_table(buf, dec, planes); // read the offset vector of bit-planes

    setup_xor_compression();

    sv.get_bmatrix().allocate_rows(planes);
    planes = (unsigned)load_null_plane(sv, int(planes), buf, mask_bv);


    // check if mask needs to be relaculated using the NULL (index) vector
    if (bm::conditional<SV::is_rsc_support::value>::test())
    {
        if (mask_bv)
        {
            const bvector_type* bv_null = sv.get_null_bvector();
            BM_ASSERT(bv_null);
            rsc_mask_bv_.clear(true);
            not_null_mask_bv_.bit_and(*bv_null, *mask_bv, bvector_type::opt_compress);
            rsc_compressor_.compress(rsc_mask_bv_, *bv_null, not_null_mask_bv_);
            mask_bv = &rsc_mask_bv_;

            // if it needs range recalculation
            if (idx_range_set_) // range setting is in effect
            {
                //bool rf =
                rsc_mask_bv_.find_range(idx_range_from_, idx_range_to_);
            }
        }
    }

    deserialize_planes(sv, planes, buf, mask_bv);

    // restore NULL slice index
#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4127)
#endif
    if (sv.max_vector_size == 1)
    {
        // NULL vector at: (sv.max_vector_size * sizeof(value_type) * 8 + 1)
        const bvector_type* bv_null = sv.get_slice(sv.sv_value_slices);
        if (bv_null)
            sv.mark_null_idx(sv.sv_value_slices); // last slice is NULL
    }
#ifdef _MSC_VER
#pragma warning( pop )
#endif


    clear_xor_compression();

    // load the remap matrix
    //
    if (bm::conditional<SV::is_remap_support::value>::test()) // test remap trait
    {
        if (matr_s_ser)
            load_remap(sv, remap_buf_ptr_);
    } // if remap traits
    
    sv.sync(true); // force sync, recalculate RS index, remap tables, etc
    remap_buf_ptr_ = 0;
}

// -------------------------------------------------------------------------

template<typename SV>
unsigned sparse_vector_deserializer<SV>::load_header(
        bm::decoder& dec, SV& sv, unsigned char& matr_s_ser)
{
    (void)sv;
    bm::id64_t planes_code = 0;
    unsigned char h1 = dec.get_8();
    unsigned char h2 = dec.get_8();

    BM_ASSERT(h1 == 'B' && (h2 == 'M' || h2 == 'C' || h2 == 'Z'));

    bool sig2_ok = (h2 == 'M' || h2 == 'C' || h2 == 'Z');
    if (h1 != 'B' || !sig2_ok) //&& (h2 != 'M' || h2 != 'C'))  // no magic header?
        raise_invalid_header();
    unsigned planes = 0;
    if (h2 == 'Z') // empty serialization package
    {
        sv_size_ = 0;
        return planes;
    }

    unsigned char bv_bo = dec.get_8(); (void) bv_bo;
    planes = dec.get_8();
    if (planes == 0)  // bit-matrix
    {
        matr_s_ser = dec.get_8(); // matrix serialization version
        planes_code = dec.get_64();
        planes = (unsigned) planes_code; // number of rows in the bit-matrix
    }
    #ifdef BM64ADDR
    #else
        if (matr_s_ser == 2) // 64-bit matrix
            raise_invalid_64bit();
    #endif

    if constexpr (SV::is_dynamic_splices::value == false)
    {
        unsigned sv_planes = sv.stored_slices();
        if (!planes || planes > sv_planes)
            raise_invalid_bitdepth();
    }

    sv_size_ = dec.get_64();

    digest_offset_ = 0;
    if (planes_code & (1ull << 63))
    {
        digest_offset_ = dec.get_64();
    }

    return planes;
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::deserialize_planes(
                                                SV& sv,
                                                unsigned planes,
                                                const unsigned char* buf,
                                                const bvector_type* mask_bv)
{
    if (mask_bv && !idx_range_set_)
        idx_range_set_ = mask_bv->find_range(idx_range_from_, idx_range_to_);

    // read-deserialize the planes based on offsets
    //       backward order to bring the NULL vector first
    //
    for (int i = int(planes-1); i >= 0; --i)
    {
        size_t offset = off_vect_[unsigned(i)];
        if (!offset) // empty vector
            continue;
        const unsigned char* bv_buf_ptr = buf + offset; // seek to position
        bvector_type*  bv = sv.get_create_slice(unsigned(i));
        BM_ASSERT(bv);

        // add the vector into the XOR reference list
        if (!bv_ref_ptr_)
            bv_ref_.add(bv, unsigned(i));
        if (mask_bv) // gather mask set, use AND operation deserializer
        {
            typename bvector_type::mem_pool_guard mp_g_z(pool_, *bv);
            if (bm::conditional<SV::is_remap_support::value>::test()
                && !remap_buf_ptr_) // last plane vector (special case)
            {
                size_t read_bytes =
                    deserial_.deserialize(*bv, bv_buf_ptr, temp_block_);
                remap_buf_ptr_ = bv_buf_ptr + read_bytes;
                bv->bit_and(*mask_bv, bvector_type::opt_compress);
            }
            else
            {
                if (idx_range_set_)
                    deserial_.set_range(idx_range_from_, idx_range_to_);
                deserial_.deserialize(*bv, bv_buf_ptr);
                bv->bit_and(*mask_bv, bvector_type::opt_compress);
            }
        }
        else
        {
            if (bm::conditional<SV::is_remap_support::value>::test() &&
                !remap_buf_ptr_)
            {
                size_t read_bytes =
                    deserial_.deserialize(*bv, bv_buf_ptr, temp_block_);
                remap_buf_ptr_ = bv_buf_ptr + read_bytes;
                if (idx_range_set_)
                    bv->keep_range(idx_range_from_, idx_range_to_);
            }
            else
            {
                if (idx_range_set_)
                {
                    deserial_.set_range(idx_range_from_, idx_range_to_);
                    deserial_.deserialize(*bv, bv_buf_ptr);
                    bv->keep_range(idx_range_from_, idx_range_to_);
                }
                else
                {
                    //size_t read_bytes =
                    deserial_.deserialize(*bv, bv_buf_ptr, temp_block_);
                }
            }
        }

        switch (is_final_)
        {
        case bm::finalization::READONLY:
            bv->freeze();
            break;
        default:
            break;
        }
    } // for i

    deserial_.unset_range();

}

// -------------------------------------------------------------------------

template<typename SV>
int sparse_vector_deserializer<SV>::load_null_plane(SV& sv,
                                                    int planes,
                                                    const unsigned char* buf,
                                                    const bvector_type* mask_bv)
{
    BM_ASSERT(planes > 0);
    if (!sv.is_nullable())
        return planes;
    int i = planes - 1;
    size_t offset = off_vect_[unsigned(i)];
    if (offset)
    {
        // TODO: improve serialization format to avoid non-range decode of
        // the NULL vector just to get to the offset of remap table

        const unsigned char* bv_buf_ptr = buf + offset; // seek to position
        bvector_type*  bv = sv.get_create_slice(unsigned(i));

        if (!bv_ref_ptr_)
            bv_ref_.add(bv, unsigned(i));

        if (bm::conditional<SV::is_rsc_support::value>::test())
        {
            // load the whole not-NULL vector regardless of range
            // TODO: load [0, idx_range_to_]
            size_t read_bytes = deserial_.deserialize(*bv, bv_buf_ptr, temp_block_);
            remap_buf_ptr_ = bv_buf_ptr + read_bytes;
        }
        else // non-compressed SV
        {
            // NULL plane in string vector with substitute re-map
            //
            if (bm::conditional<SV::is_remap_support::value>::test())
            {
                BM_ASSERT(!remap_buf_ptr_);
                size_t read_bytes = deserial_.deserialize(*bv, bv_buf_ptr, temp_block_);
                remap_buf_ptr_ = bv_buf_ptr + read_bytes;
                if (idx_range_set_)
                    bv->keep_range(idx_range_from_, idx_range_to_);
            }
            else
            if (idx_range_set_)
            {
                deserial_.set_range(idx_range_from_, idx_range_to_);
                deserial_.deserialize(*bv, bv_buf_ptr, temp_block_);
                bv->keep_range(idx_range_from_, idx_range_to_);
                deserial_.unset_range();
            }
            else
            {
                deserial_.deserialize(*bv, bv_buf_ptr, temp_block_);
            }
            if (mask_bv)
                bv->bit_and(*mask_bv, bvector_type::opt_compress);
        }

        switch (is_final_)
        {
        case bm::finalization::READONLY:
            bv->freeze();
            break;
        default:
            break;
        }

    }
    return planes-1;
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::load_planes_off_table(
            const unsigned char* buf, bm::decoder& dec, unsigned planes)
{
    off_vect_.resize(planes);
    if (digest_offset_)
    {
        plane_digest_bv_.clear(false);
        const unsigned char* buf_ptr = buf + digest_offset_;
        size_t read_bytes =
            deserial_.deserialize(plane_digest_bv_, buf_ptr, temp_block_);
        buf_ptr += read_bytes;

        bm::decoder dec_o(buf_ptr);

        unsigned char dtype = dec_o.get_8();
        switch (dtype)
        {
        case '6':
            for (unsigned i = 0; i < planes; ++i)
            {
                size_t offset = 0;
                if (plane_digest_bv_.test(i))
                    offset = (size_t) dec_o.get_64();
                off_vect_[i] = offset;
            } // for i
            break;
        case '3':
        {
            unsigned osize = (unsigned)plane_digest_bv_.count();
            BM_ASSERT(osize);
            off32_vect_.resize(osize);

            unsigned min_v = dec_o.get_32();
            unsigned max_v = dec_o.get_32();

            off32_vect_[0] = min_v;
            off32_vect_[osize-1] = max_v;

            bm::bit_in<bm::decoder> bi(dec_o);
            bi.bic_decode_u32_cm(off32_vect_.data()+1, osize-2, min_v, max_v);

            unsigned k = 0;
            for (unsigned i = 0; i < planes; ++i)
            {
                if (plane_digest_bv_.test(i))
                {
                    off_vect_[i] = off32_vect_[k];
                    ++k;
                }
                else
                    off_vect_[i] = 0;
            }
        }
        break;
        default:
            // TODO: raise an exception
            BM_ASSERT(0);
        } // switch
    }
    else
    {
        for (unsigned i = 0; i < planes; ++i)
        {
            size_t offset = (size_t) dec.get_64();
            off_vect_[i] = offset;
        } // for i
    }
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::load_remap(SV& sv,
                                         const unsigned char* remap_buf_ptr)
{
    if (!remap_buf_ptr)
        return;

    bm::decoder dec_m(remap_buf_ptr);

    unsigned char rh = dec_m.get_8();
    switch (rh)
    {
    case 'N':
        return;
    case 'R':
        {
            size_t remap_size = (size_t) dec_m.get_64();
            unsigned char* remap_buf = sv.init_remap_buffer();
            BM_ASSERT(remap_buf);
            size_t target_remap_size = sv.remap_size();
            if (!remap_size || !remap_buf || remap_size != target_remap_size)
            {
                raise_invalid_format();
            }
            dec_m.memcpy(remap_buf, remap_size);
        }
        break;

    case 'C': // CSR remap
        {
            //sv.init_remap_buffer();
            typename SV::remap_matrix_type* rmatr = sv.get_remap_matrix();
            if (!rmatr)
            {
                raise_missing_remap_matrix();
            }
            size_t rows = (size_t) dec_m.get_32();
            size_t cols = dec_m.get_16();
            if (cols > 256)
            {
                raise_invalid_format();
            }
            rmatr->resize(rows, cols, false);
            if (rows)
            {
                rmatr->set_zero();

                // read gamma encoded row lens
                remap_rlen_vect_.resize(0);
                {
                    bm::bit_in<bm::decoder> bi(dec_m);
                    for (size_t r = 0; r < rows; ++r)
                    {
                        unsigned rl = bi.gamma();
                        remap_rlen_vect_.push_back(rl);
                    } // for r
                }

                for (size_t r = 0; r < rows; ++r)
                {
                    unsigned char* BMRESTRICT row = rmatr->row(r);
                    size_t cnt = remap_rlen_vect_[r];
                    if (!cnt || cnt > 256)
                    {
                        raise_invalid_format(); // format corruption!
                    }
                    for (size_t j = 0; j < cnt; ++j)
                    {
                        unsigned idx = dec_m.get_8();
                        unsigned char v = dec_m.get_8();
                        row[idx] = v;
                    } // for j
                } // for r
            }
        }
        break;
    default:
        // re-map matrix code error
        raise_invalid_format();
    } // switch

    // finalize the remap matrix read
    //
    unsigned char end_tok = dec_m.get_8();
    if (end_tok != 'E')
    {
        raise_invalid_format();
    }
    sv.set_remap();
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::raise_invalid_header()
{
#ifndef BM_NO_STL
    throw std::logic_error("BitMagic: Invalid serialization signature header");
#else
    BM_THROW(BM_ERR_SERIALFORMAT);
#endif
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::raise_invalid_64bit()
{
#ifndef BM_NO_STL
    throw std::logic_error("BitMagic: Invalid serialization target (64-bit BLOB)");
#else
    BM_THROW(BM_ERR_SERIALFORMAT);
#endif
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::raise_invalid_bitdepth()
{
#ifndef BM_NO_STL
    throw std::logic_error("BitMagic: Invalid serialization target (bit depth)");
#else
    BM_THROW(BM_ERR_SERIALFORMAT);
#endif
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::raise_invalid_format()
{
#ifndef BM_NO_STL
    throw std::logic_error("BitMagic: Invalid serialization fromat (BLOB corruption?)");
#else
    BM_THROW(BM_ERR_SERIALFORMAT);
#endif
}

// -------------------------------------------------------------------------

template<typename SV>
void sparse_vector_deserializer<SV>::raise_missing_remap_matrix()
{
#ifndef BM_NO_STL
    throw std::logic_error("BitMagic: Invalid serialization format (remap matrix)");
#else
    BM_THROW(BM_ERR_SERIALFORMAT);
#endif
}

// -------------------------------------------------------------------------

} // namespace bm

#endif
