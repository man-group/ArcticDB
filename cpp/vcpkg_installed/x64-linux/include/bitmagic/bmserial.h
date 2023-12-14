#ifndef BMSERIAL__H__INCLUDED__
#define BMSERIAL__H__INCLUDED__
/*
Copyright(c) 2002-2019 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

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

/*! \file bmserial.h
    \brief Serialization / compression of bvector<>.
    Set theoretical operations on compressed BLOBs.
*/

/*! 
    \defgroup bvserial bvector<> serialization
    Serialization for bvector<> container
 
    \ingroup bvector
*/

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration 
// #include "bm.h" or "bm64.h" explicitly 
# error missing include (bm.h or bm64.h)
#endif


#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4311 4312 4127)
#endif



#include "encoding.h"
#include "bmfunc.h"
#include "bmtrans.h"
#include "bmalgo.h"
#include "bmutil.h"
#include "bmbuffer.h"
#include "bmdef.h"
#include "bmxor.h"

namespace bm
{

const unsigned set_compression_max = 6;     ///< Maximum supported compression level
const unsigned set_compression_default = 6; ///< Default compression level

/**
    Bit-vector serialization class.
    
    Class designed to convert sparse bit-vectors into a single block of memory
    ready for file or database storage or network transfer.
    
    Reuse of this class for multiple serializations (but not across threads).
    Class resue offers some performance advantage (helps with temp memory
    reallocations).
    
    @ingroup bvserial 
*/
template<class BV>
class serializer
{
public:
    typedef BV                                              bvector_type;
    typedef typename bvector_type::allocator_type           allocator_type;
    typedef typename bvector_type::blocks_manager_type      blocks_manager_type;
    typedef typename bvector_type::statistics               statistics_type;
    typedef typename bvector_type::block_idx_type           block_idx_type;
    typedef typename bvector_type::size_type                size_type;

    typedef byte_buffer<allocator_type>                buffer;
    typedef bm::bv_ref_vector<BV>                      bv_ref_vector_type;
    typedef bm::xor_sim_model<BV>                      xor_sim_model_type;

    typedef
    typename xor_sim_model_type::block_match_chain_type block_match_chain_type;
public:
    /**
        Constructor
        
        \param alloc - memory allocator
        \param temp_block - temporary block for various operations
               (if NULL it will be allocated and managed by serializer class)
        Temp block is used as a scratch memory during serialization,
        use of external temp block allows to avoid unnecessary re-allocations.
     
        Temp block attached is not owned by the class and NOT deallocated on
        destruction.
    */
    serializer(const allocator_type&   alloc  = allocator_type(),
              bm::word_t*  temp_block = 0);
    
    serializer(bm::word_t*  temp_block);

    ~serializer();

    /*! @name Compression level settings                               */
    //@{

    // --------------------------------------------------------------------
    /**
        Set compression level. Higher compression takes more time to process.
        @param clevel - compression level (0-6)
        0 - take as is
        1, 2 - apply light weight RLE/GAP encodings, limited depth hierarchical
               compression, intervals encoding
        3 - variant of 2 with different cut-offs
        4 - delta transforms plus Elias Gamma encoding where possible legacy)
        5 - Binary Interpolative Coding (BIC) - light settings
        6 - Binary Interpolative Coding (BIC) - harder settings

        @sa get_compression_level
    */
    void set_compression_level(unsigned clevel) BMNOEXCEPT;

    /**
        Get current compression level.
    */
    unsigned get_compression_level() const BMNOEXCEPT
        { return compression_level_; }


    //@}

    
    // --------------------------------------------------------------------
    /*! @name Serialization Methods                                      */
    //@{

    /**
        Bitvector serialization into memory block
        
        @param bv - input bitvector
        @param buf - out buffer (pre-allocated)
           No range checking is done in this method. 
           It is responsibility of caller to allocate sufficient 
           amount of memory using information from calc_stat() function.        
        
        @param buf_size - size of the output buffer
        
       @return Size of serialization block.
       @sa calc_stat     
    */
    size_type serialize(const BV& bv,
                        unsigned char* buf, size_t buf_size);
    
    /**
        Bitvector serialization into buffer object (resized automatically)
     
        @param bv       - input bitvector
        @param buf      - output buffer object
        @param bv_stat  - input (optional) bit-vector statistics object
                          if NULL, serialize will compute the statistics
    */
    void serialize(const BV& bv,
                   typename serializer<BV>::buffer& buf,
                   const statistics_type* bv_stat = 0);
    
    /**
        Bitvector serialization into buffer object (resized automatically)
        Input bit-vector gets optimized and then destroyed, content is
        NOT guaranteed after this operation.
        Effectively it moves data into the buffer.

        The reason this operation exsists is because it is faster to do
        all three operations in one single pass.
        This is a destructive serialization!

        @param bv       - input/output bitvector
        @param buf      - output buffer object
    */
    void optimize_serialize_destroy(BV& bv,
                                    typename serializer<BV>::buffer& buf);

    //@}
    // --------------------------------------------------------------------

    /**
        Return serialization counter vector
        @internal
    */
    const size_type* get_compression_stat() const BMNOEXCEPT
                                    { return compression_stat_; }

    /**
        Enable/disable statistics reset on each serilaization
     */
    void allow_stat_reset(bool allow) BMNOEXCEPT
        { allow_stat_reset_ = allow; }

    /// Reset all accumulated compression statistics
    void reset_compression_stats() BMNOEXCEPT;

    /**
        Set GAP length serialization (serializes GAP levels of the original vector)
                
        @param value - when TRUE serialized vector includes GAP levels parameters
    */
    void gap_length_serialization(bool value) BMNOEXCEPT;
    
    /**
        Set byte-order serialization (for cross platform compatibility)
        @param value - TRUE serialization format includes byte-order marker
    */
    void byte_order_serialization(bool value) BMNOEXCEPT;

    /**
        Add skip-markers to serialization BLOB for faster range decode
        at the expense of some BLOB size increase

        @param enable - TRUE searilization will add bookmark codes
        @param bm_interval - bookmark interval in (number of blocks)
        suggested values between 4 and 512 (block size is 64K bits)
        smaller interval means more bookmarks added to the skip list 
        allows faster range deserialization at the expense of  
        somewhat increased BLOB size.
    */
    void set_bookmarks(bool enable, unsigned bm_interval = 256) BMNOEXCEPT;

    /**
        Fine tuning for Binary Interpolative Compression (levels 5+)
        The parameter sets average population count per block (64Kbits) 
        below which block is considered very sparse. 
        If super block (group of 256 blocks) is very sparse it applies 
        block size expansion (for the compression purposes) to 
        improve compression rates.
    */
    void set_sparse_cutoff(unsigned cutoff) BMNOEXCEPT;

    /**
        Attach collection of reference vectors for XOR serialization
        (no transfer of ownership for the pointers)
        @internal
    */
    void set_ref_vectors(const bv_ref_vector_type* ref_vect);

    /**
        Calculate XOR similarity model for ref_vector
        refernece vector must be associated before

        @param sim_model - [out] similarity model to compute
        @param ref_vect - [in] reference vectors
        @param params - parameters to regulate search depth
        @return true - if similarity model created successfully

        @sa set_ref_vectors
        @internal
     */
    bool compute_sim_model(xor_sim_model_type&       sim_model,
                           const bv_ref_vector_type& ref_vect,
                           const bm::xor_sim_params& params);

    /**
        Atach XOR similarity model (must be computed by the same ref vector)
        @internal
     */
    void set_sim_model(const xor_sim_model_type* sim_model) BMNOEXCEPT;

    /**
        Set current index in rer.vector collection
        (not a row idx or plain idx)
    */
    void set_curr_ref_idx(size_type ref_idx) BMNOEXCEPT;


protected:
    /**
        Encode serialization header information
    */
    void encode_header(const BV& bv, bm::encoder& enc) BMNOEXCEPT;
    
    /*! Encode GAP block */
    void encode_gap_block(const bm::gap_word_t* gap_block, bm::encoder& enc);

    /*! Encode GAP block with Elias Gamma coder */
    void gamma_gap_block(const bm::gap_word_t* gap_block,
                         bm::encoder&          enc) BMNOEXCEPT;

    /**
        Encode GAP block as delta-array with Elias Gamma coder
    */
    void gamma_gap_array(const bm::gap_word_t* gap_block, 
                         unsigned              arr_len, 
                         bm::encoder&          enc,
                         bool                  inverted = false) BMNOEXCEPT;
    
    /// Encode bit-block as an array of bits
    void encode_bit_array(const bm::word_t* block,
                          bm::encoder& enc, bool inverted) BMNOEXCEPT;
    
    void gamma_gap_bit_block(const bm::word_t* block,
                             bm::encoder&      enc) BMNOEXCEPT;
    
    void gamma_arr_bit_block(const bm::word_t* block,
                          bm::encoder& enc, bool inverted) BMNOEXCEPT;

    void bienc_arr_bit_block(const bm::word_t* block,
                            bm::encoder& enc, bool inverted) BMNOEXCEPT;

    void bienc_arr_sblock(const BV& bv, unsigned sb,
                                        bm::encoder& enc) BMNOEXCEPT;

    /// encode bit-block as interpolated bit block of gaps
    void bienc_gap_bit_block(const bm::word_t* block,
                             bm::encoder& enc) BMNOEXCEPT;

    void interpolated_arr_bit_block(const bm::word_t* block,
                            bm::encoder& enc, bool inverted) BMNOEXCEPT;
    /// encode bit-block as interpolated gap block
    void interpolated_gap_bit_block(const bm::word_t* block,
                                    bm::encoder&      enc) BMNOEXCEPT;

    /**
        Encode GAP block as an array with binary interpolated coder
    */
    void interpolated_gap_array(const bm::gap_word_t* gap_block,
                                unsigned              arr_len,
                                bm::encoder&          enc,
                                bool                  inverted) BMNOEXCEPT;
    void interpolated_gap_array_v0(const bm::gap_word_t* gap_block,
                                   unsigned              arr_len,
                                   bm::encoder&          enc,
                                   bool                  inverted) BMNOEXCEPT;


    /*! Encode GAP block with using binary interpolated encoder */
    void interpolated_encode_gap_block(
                const bm::gap_word_t* gap_block, bm::encoder& enc) BMNOEXCEPT;

    /**
        Encode BIT block with repeatable runs of zeroes
    */
    void encode_bit_interval(const bm::word_t* blk, 
                             bm::encoder&      enc,
                             unsigned          size_control) BMNOEXCEPT;
    /**
        Encode bit-block using digest (hierarchical compression)
    */
    void encode_bit_digest(const bm::word_t*  blk,
                           bm::encoder&       enc,
                           bm::id64_t         d0) BMNOEXCEPT;
    /**
        Encode XOR match chain
     */
    void encode_xor_match_chain(bm::encoder& enc,
                            const block_match_chain_type& mchain) BMNOEXCEPT;

    /**
        Determine best representation for GAP block based
        on current set compression level
     
        @return  set_block_bit, set_block_bit_1bit, set_block_arrgap
                 set_block_arrgap_egamma, set_block_arrgap_bienc
                 set_block_arrgap_inv, set_block_arrgap_egamma_inv
                 set_block_arrgap_bienc_inv, set_block_gap_egamma
                 set_block_gap_bienc
     
        @internal
    */
    unsigned char
    find_gap_best_encoding(const bm::gap_word_t* gap_block) BMNOEXCEPT;
    
    /// Determine best representation for a bit-block
    unsigned char find_bit_best_encoding(const bm::word_t* block) BMNOEXCEPT;

    /// Determine best representation for a bit-block (level 5)
    unsigned char find_bit_best_encoding_l5(const bm::word_t* block) BMNOEXCEPT;

    void reset_models() BMNOEXCEPT { mod_size_ = 0; }
    void add_model(unsigned char mod, unsigned score) BMNOEXCEPT;
protected:

    /// Bookmark state structure
    struct bookmark_state
    {
        bookmark_state(block_idx_type nb_range) BMNOEXCEPT
            : ptr_(0), nb_(0),
              nb_range_(nb_range), bm_type_(0)
        {
            min_bytes_range_ = nb_range * 8;
            if (min_bytes_range_ < 512)
                min_bytes_range_ = 512;

            if (nb_range < 15)
                bm_type_ = 2;  // 16-bit offset
            else
            if (nb_range < 255)
                bm_type_ = 1; // 24-bit offset
        }

        unsigned char*  ptr_; ///< bookmark pointer
        block_idx_type  nb_;  ///< bookmark block idx
        block_idx_type  nb_range_; ///< target bookmark range in blocks
        unsigned        bm_type_;  ///< 0:32-bit, 1: 24-bit, 2: 16-bit
        size_t          min_bytes_range_; ///< minumal distance (bytes) between marks
    };

    /**
       Check if bookmark needs to be placed and if so, encode it 
       into serialization BLOB

       @param nb - block idx
       @param bookm - bookmark state structure
       @param enc - BLOB encoder
    */
    static
    void process_bookmark(block_idx_type nb, bookmark_state& bookm,
                          bm::encoder&   enc) BMNOEXCEPT;

    /**
        Compute digest based XOR product, place into tmp XOR block
    */
    void xor_tmp_product(
                    const bm::word_t* s_block,
                    const block_match_chain_type& mchain,
                    unsigned i, unsigned j) BMNOEXCEPT;

private:
    serializer(const serializer&);
    serializer& operator=(const serializer&);
    
private:
    typedef bm::bit_out<bm::encoder>                        bit_out_type;
    typedef bm::gamma_encoder<bm::gap_word_t, bit_out_type> gamma_encoder_func;
    typedef bm::heap_vector<bm::gap_word_t, allocator_type, true> block_arridx_type;
    typedef bm::heap_vector<unsigned, allocator_type, true> sblock_arridx_type;
    typedef typename allocator_type::allocator_pool_type    allocator_pool_type;

private:
    bm::id64_t         digest0_;
    unsigned           bit_model_d0_size_; ///< memory (bytes) by d0 method (bytes)
    unsigned           bit_model_0run_size_; ///< memory (bytes) by run-0 method (bytes)
    block_arridx_type  bit_idx_arr_;
    sblock_arridx_type sb_bit_idx_arr_;
    unsigned           scores_[bm::block_waves];
    unsigned char      models_[bm::block_waves];
    unsigned           mod_size_;
    
    allocator_type  alloc_;
    size_type*      compression_stat_;
    bool            allow_stat_reset_ = true; ///< controls zeroing of telemetry
    bool            gap_serial_;
    bool            byte_order_serial_;

    bool            sb_bookmarks_; ///< Bookmarks flag
    unsigned        sb_range_;     ///< Desired bookmarks interval

    bm::word_t*     temp_block_;
    unsigned        compression_level_;
    bool            own_temp_block_;
    
    bool            optimize_; ///< flag to optimize the input vector
    bool            free_;     ///< flag to free the input vector
    allocator_pool_type pool_;


    unsigned char* enc_header_pos_; ///< pos of top level header to roll back
    unsigned char header_flag_;     ///< set of masks used to save

    // XOR compression
    //
    const bv_ref_vector_type* ref_vect_; ///< ref.vector for XOR compression
    const xor_sim_model_type* sim_model_; ///< similarity model matrix
    bm::xor_scanner<BV>       xor_scan_; ///< scanner for XOR similarity
    size_type                 ref_idx_;  ///< current reference index
    bm::word_t*               xor_tmp_block_; ///< tmp area for xor product
    bm::word_t*               xor_tmp1_;
    bm::word_t*               xor_tmp2_;

    unsigned      sparse_cutoff_;   ///< number of bits per blocks to consider sparse 
};

/**
    Base deserialization class
    \ingroup bvserial
    @internal
*/
template<typename DEC, typename BLOCK_IDX>
class deseriaizer_base
{
protected:
    typedef DEC       decoder_type;
    typedef BLOCK_IDX block_idx_type;
    typedef bm::bit_in<DEC> bit_in_type;

protected:
    deseriaizer_base()
        : id_array_(0), sb_id_array_(0), bookmark_idx_(0), skip_offset_(0), skip_pos_(0)
    {}
    
    /// Read GAP block from the stream
    void read_gap_block(decoder_type&   decoder, 
                        unsigned        block_type, 
                        bm::gap_word_t* dst_block,
                        bm::gap_word_t& gap_head);

	/// Read list of bit ids
	///
	/// @return number of ids
	unsigned read_id_list(decoder_type&   decoder, 
                          unsigned        block_type, 
                          bm::gap_word_t* dst_arr);
    
    /// Read binary interpolated list into a bit-set
    void read_bic_arr(decoder_type&   decoder, 
                      bm::word_t* blk, unsigned block_type) BMNOEXCEPT;

	/// Read list of bit ids for super-blocks
	///
	/// @return number of ids
    unsigned read_bic_sb_arr(decoder_type&   decoder,
                             unsigned  block_type,
                             unsigned*  dst_arr,
                             unsigned*  sb_idx);

    /// Read binary interpolated gap blocks into a bitset
    void read_bic_gap(decoder_type&   decoder, bm::word_t* blk) BMNOEXCEPT;

    /// Read inverted binary interpolated list into a bit-set
    void read_bic_arr_inv(decoder_type&   decoder, bm::word_t* blk) BMNOEXCEPT;
    
    /// Read digest0-type bit-block
    void read_digest0_block(decoder_type& decoder, bm::word_t* blk) BMNOEXCEPT;
    
    
    /// read bit-block encoded as runs
    static
    void read_0runs_block(decoder_type& decoder, bm::word_t* blk) BMNOEXCEPT;
    
    static
    const char* err_msg() BMNOEXCEPT { return "BM::Invalid serialization format"; }

    /// Try to skip if skip bookmark is available within reach
    /// @return new block idx if skip went well
    ///
    block_idx_type try_skip(decoder_type&  decoder,
                            block_idx_type nb,
                            block_idx_type expect_nb) BMNOEXCEPT;

protected:
    bm::gap_word_t*   id_array_; ///< ptr to idx array for temp decode use
    unsigned*         sb_id_array_; ///< ptr to super-block idx array (temp)

    block_idx_type       bookmark_idx_;///< last bookmark block index
    unsigned             skip_offset_; ///< bookmark to skip 256 encoded blocks
    const unsigned char* skip_pos_;    ///< decoder skip position
};

/**
    Deserializer for bit-vector
    \ingroup bvserial 
*/
template<class BV, class DEC>
class deserializer :
            protected deseriaizer_base<DEC, typename BV::block_idx_type>
{
public:
    typedef BV                                             bvector_type;
    typedef typename bvector_type::allocator_type          allocator_type;
    typedef typename BV::size_type                         size_type;
    typedef typename bvector_type::block_idx_type          block_idx_type;
    typedef deseriaizer_base<DEC, block_idx_type>          parent_type;
    typedef typename parent_type::decoder_type             decoder_type;
    typedef bm::bv_ref_vector<BV>                          bv_ref_vector_type;

public:
    deserializer();
    ~deserializer();

    /*!  Deserialize bit-vector (equivalent to logical OR)
        @param bv - target bit-vector
        @param buf - BLOB memory pointer
        @param temp_block - temporary buffer [block size] (not used)

        @return number of consumed bytes
    */
    size_t deserialize(bvector_type&        bv,
                       const unsigned char* buf,
                       bm::word_t*          temp_block = 0);

    // ----------------------------------------------------------------
    /**
        Attach collection of reference vectors for XOR de-serialization
        (no transfer of ownership for the pointer)
        @internal
    */
    void set_ref_vectors(const bv_ref_vector_type* ref_vect);


    /**
        set deserialization range [from, to]
        This is NOT exact, approximate range, content outside range
        is not guaranteed to be absent
        @sa unset_range()
    */
    void set_range(size_type from, size_type to) BMNOEXCEPT
    {
        is_range_set_ = 1; idx_from_ = from; idx_to_ = to;
    }

    /**
        Disable range deserialization
        @sa set_range()
    */
    void unset_range() BMNOEXCEPT { is_range_set_ = 0; }

    /** reset range deserialization and reference vectors
        @sa set_range()
    */
    void reset() BMNOEXCEPT
    {
        unset_range(); set_ref_vectors(0);
    }
protected:
   typedef typename BV::blocks_manager_type blocks_manager_type;

protected:
   void xor_decode(blocks_manager_type& bman);
   void xor_decode_chain(bm::word_t* BMRESTRICT blk) BMNOEXCEPT;
   void xor_reset() BMNOEXCEPT;

   void deserialize_gap(unsigned char btype, decoder_type& dec, 
                        bvector_type&  bv, blocks_manager_type& bman,
                        block_idx_type nb,
                        bm::word_t* blk);
   void decode_bit_block(unsigned char btype, decoder_type& dec,
                         blocks_manager_type& bman,
                         block_idx_type nb,
                         bm::word_t* blk);

   void decode_block_bit(decoder_type& dec,
                         bvector_type&  bv,
                         block_idx_type nb,
                         bm::word_t* blk);

   void decode_block_bit_interval(decoder_type& dec,
                                  bvector_type&  bv,
                                  block_idx_type nb,
                                  bm::word_t* blk);

   void decode_arrbit(decoder_type& dec,
                      bvector_type&  bv,
                      block_idx_type nb,
                      bm::word_t* blk);

   void decode_arr_sblock(unsigned char btype, decoder_type& dec,
                         bvector_type&  bv);


protected:
    typedef bm::heap_vector<bm::gap_word_t, allocator_type, true> block_arridx_type;
    typedef bm::heap_vector<bm::word_t, allocator_type, true> sblock_arridx_type;
    typedef typename allocator_type::allocator_pool_type allocator_pool_type;

protected:
    block_arridx_type    bit_idx_arr_;
    sblock_arridx_type   sb_bit_idx_arr_;
    block_arridx_type    gap_temp_block_;
    bm::word_t*          temp_block_;

    allocator_pool_type  pool_;
    allocator_type       alloc_;

    // XOR compression memory
    //
    const bv_ref_vector_type* ref_vect_;  ///< ref.vector for XOR compression
    bm::word_t*               xor_block_; ///< xor product
    bm::word_t*               or_block_;
    size_type                 or_block_idx_;

    // XOR decode FSM
    //
    size_type                 x_ref_idx_;
    bm::id64_t                x_ref_d64_;
    block_idx_type            x_nb_;
    unsigned                  xor_chain_size_;
    bm::match_pair            xor_chain_[64];
//    bool                      x_ref_gap_;

    // Range deserialization settings
    //
    unsigned                  is_range_set_;
    size_type                 idx_from_;
    size_type                 idx_to_;


};


/**
    Iterator to walk forward the serialized stream.

    \internal
    \ingroup bvserial 
*/
template<class BV, class SerialIterator>
class iterator_deserializer
{
public:
    typedef BV                               bvector_type;
    typedef typename bvector_type::size_type size_type;
    typedef SerialIterator                   serial_iterator_type;
public:

    /// set deserialization range [from, to]
    void set_range(size_type from, size_type to);

    /// disable range filtration
    void unset_range() BMNOEXCEPT { is_range_set_ = false; }

    size_type deserialize(bvector_type&         bv,
                          serial_iterator_type& sit,
                          bm::word_t*           temp_block,
                          set_operation         op = bm::set_OR,
                          bool                  exit_on_one = false);

private:
    typedef typename BV::blocks_manager_type            blocks_manager_type;
    typedef typename bvector_type::block_idx_type       block_idx_type;

    /// load data from the iterator of type "id list"
    static
    void load_id_list(bvector_type&         bv, 
                      serial_iterator_type& sit,
                      unsigned              id_count,
                      bool                  set_clear);

    /// Finalize the deserialization (zero target vector tail or bit-count tail)
    static
    size_type finalize_target_vector(blocks_manager_type& bman,
                                     set_operation        op,
                                     size_type            bv_block_idx);

    /// Process (obsolete) id-list serialization format
    static
    size_type process_id_list(bvector_type&         bv,
                              serial_iterator_type& sit,
                              set_operation         op);
    static
    const char* err_msg() BMNOEXCEPT
                { return "BM::de-serialization format error"; }
private:
    bool                       is_range_set_ = false;
    size_type                  nb_range_from_ = 0;
    size_type                  nb_range_to_ = 0;
};

/*!
    @brief Serialization stream iterator

    Iterates blocks and control tokens of serialized bit-stream
    \ingroup bvserial
    @internal
*/
template<class DEC, typename BLOCK_IDX>
class serial_stream_iterator : protected deseriaizer_base<DEC, BLOCK_IDX>
{
public:
    typedef typename deseriaizer_base<DEC, BLOCK_IDX>::decoder_type decoder_type;
    typedef BLOCK_IDX   block_idx_type;
    typedef deseriaizer_base<DEC, block_idx_type>    parent_type;

public:
    serial_stream_iterator(const unsigned char* buf);
    ~serial_stream_iterator();

    /// serialized bitvector size
    block_idx_type bv_size() const { return bv_size_; }

    /// Returns true if end of bit-stream reached 
    bool is_eof() const { return end_of_stream_; }

    /// get next block
    void next();

	/// skip all zero or all-one blocks
	block_idx_type skip_mono_blocks() BMNOEXCEPT;

    /// read bit block, using logical operation
    unsigned get_bit_block(bm::word_t*       dst_block, 
                           bm::word_t*       tmp_block,
                           set_operation     op);


    /// Read gap block data (with head)
    void get_gap_block(bm::gap_word_t* dst_block);

    /// Return current decoder size
    unsigned dec_size() const { return decoder_.size(); }

    /// Get low level access to the decoder (use carefully)
    decoder_type& decoder() { return decoder_; }

    /// iterator is a state machine, this enum encodes 
    /// its key value
    ///
    enum iterator_state 
    {
        e_unknown = 0,
        e_list_ids,     ///< plain int array
        e_blocks,       ///< stream of blocks
        e_zero_blocks,  ///< one or more zero bit blocks
        e_one_blocks,   ///< one or more all-1 bit blocks
        e_bit_block,    ///< one bit block
        e_gap_block     ///< one gap block

    };

    /// Returns iterator internal state
    iterator_state state() const BMNOEXCEPT { return this->state_; }

    iterator_state get_state() const BMNOEXCEPT { return this->state_; }
    /// Number of ids in the inverted list (valid for e_list_ids)
    unsigned get_id_count() const BMNOEXCEPT { return this->id_cnt_; }

    /// Get last id from the id list
    bm::id_t get_id() const BMNOEXCEPT { return this->last_id_; }

    /// Get current block index 
    block_idx_type block_idx() const BMNOEXCEPT { return this->block_idx_; }

public:
    /// member function pointer for bitset-bitset get operations
    /// 
    typedef 
        unsigned (serial_stream_iterator<DEC, BLOCK_IDX>::*get_bit_func_type)
                                                (bm::word_t*,bm::word_t*);

    unsigned 
    get_bit_block_ASSIGN(bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_OR    (bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_AND   (bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_SUB   (bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_XOR   (bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_COUNT (bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_COUNT_AND(bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_COUNT_OR(bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_COUNT_XOR(bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_COUNT_SUB_AB(bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_COUNT_SUB_BA(bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_COUNT_A(bm::word_t* dst_block, bm::word_t* tmp_block);
    unsigned 
    get_bit_block_COUNT_B(bm::word_t* dst_block, bm::word_t* tmp_block)
    {
        return get_bit_block_COUNT(dst_block, tmp_block);
    }

    /// Get array of bits out of the decoder into bit block
    /// (Converts inverted list into bits)
    /// Returns number of words (bits) being read
    unsigned get_arr_bit(bm::word_t* dst_block, 
                         bool clear_target=true) BMNOEXCEPT;

	/// Get current block type
	unsigned get_block_type() const BMNOEXCEPT { return block_type_; }

	unsigned get_bit() BMNOEXCEPT;
 
    void get_inv_arr(bm::word_t* block) BMNOEXCEPT;

    /// Try to skip if skip bookmark is available within reach
    /// @return true if skip went well
    ///
    bool try_skip(block_idx_type nb, block_idx_type expect_nb) BMNOEXCEPT
    {
        block_idx_type new_nb = parent_type::try_skip(decoder_, nb, expect_nb);
        if (new_nb)
        {
            block_idx_ = new_nb; state_ = e_blocks;
        }
        return new_nb;
    }

protected:
    get_bit_func_type  bit_func_table_[bm::set_END];

    unsigned char header_flag_;

    decoder_type       decoder_;
    bool               end_of_stream_;
    block_idx_type     bv_size_;
    iterator_state     state_;
    unsigned           id_cnt_;  ///< Id counter for id list
    bm::id_t           last_id_; ///< Last id from the id list
    gap_word_t         glevels_[bm::gap_levels]; ///< GAP levels

    unsigned           block_type_;     ///< current block type
    block_idx_type     block_idx_;      ///< current block index
    block_idx_type     mono_block_cnt_; ///< number of 0 or 1 blocks

    gap_word_t         gap_head_;
    gap_word_t*        block_idx_arr_;
};

/**
    Deserializer, performs logical operations between bit-vector and
    serialized bit-vector. This utility class potentially provides faster
    and/or more memory efficient operation than more conventional deserialization
    into memory bvector and then logical operation

    \ingroup bvserial 
*/
template<typename BV>
class operation_deserializer
{
public:
    typedef BV                                      bvector_type;
    typedef typename BV::allocator_type             allocator_type;
    typedef typename bvector_type::size_type        size_type;
    typedef bm::bv_ref_vector<BV>                   bv_ref_vector_type;

public:
    operation_deserializer();
    ~operation_deserializer();

    /*!
    \brief Deserialize bvector using buffer as set operation argument

    \param bv - target bvector
    \param buf - serialized buffer used as as a logical operation argument
    \param op - set algebra operation (default: OR)
    \param exit_on_one - quick exit if set operation found some result

    \return bitcount (for COUNT_* operations)
    */
    size_type deserialize(bvector_type&       bv,
                         const unsigned char* buf,
                         set_operation        op,
                         bool                 exit_on_one = false);

    /*!
        Deserialize range using mask vector (AND)
        \param bv - target bvector (should be set ranged)
        \param buf - serialized buffer pointer
        \param idx_from - range of bits set for deserialization [from..to]
        \param idx_to - range of bits [from..to]
    */
    void deserialize_range(bvector_type&       bv,
                           const unsigned char* buf,
                           size_type            idx_from,
                           size_type            idx_to);




    // -----------------------------------------------------------------
    // obsolete methods (switch to new ones, without team_block)
    //

    /*!
    \brief Obsolete! Deserialize bvector using buffer as set operation argument
    
    \param bv - target bvector
    \param buf - serialized buffer as a logical argument
    \param op - set algebra operation (default: OR)
    \param exit_on_one - quick exit if set operation found some result
    
    \return bitcount (for COUNT_* operations)
    */
    size_type deserialize(bvector_type&       bv,
                         const unsigned char* buf, 
                         bm::word_t*,          /*temp_block,*/
                         set_operation        op = bm::set_OR,
                         bool                 exit_on_one = false)
        { return deserialize(bv, buf, op, exit_on_one); }

    /*!
        Deserialize range using mask vector (AND)
        \param bv - target bvector (should be set ranged)
        \param buf - serialized buffer pointer
        \param idx_from - range of bits set for deserialization [from..to]
        \param idx_to - range of bits [from..to]
    */
    void deserialize_range(bvector_type&       bv,
                           const unsigned char* buf,
                           bm::word_t*,         /* temp_block, */
                           size_type            idx_from,
                           size_type            idx_to)
        { deserialize_range(bv, buf, idx_from, idx_to); }
    // -----------------------------------------------------------------

    /**
        Attach collection of reference vectors for XOR serialization
        (no transfer of ownership for the pointer)
        @internal
    */
    void set_ref_vectors(const bv_ref_vector_type* ref_vect)
        { ref_vect_ = ref_vect; }

private:
    size_type deserialize_xor(bvector_type&       bv,
                              const unsigned char* buf,
                              set_operation        op,
                              bool                 exit_on_one);
    static
    size_type deserialize_xor(bvector_type&       bv,
                              bvector_type&       bv_tmp,
                              set_operation       op);

    void deserialize_xor_range(bvector_type&       bv,
                                    const unsigned char* buf,
                                    size_type            idx_from,
                                    size_type            idx_to);

private:
    typedef typename bvector_type::block_idx_type         block_idx_type;
    typedef
        typename BV::blocks_manager_type               blocks_manager_type;
    typedef
        serial_stream_iterator<bm::decoder, block_idx_type> serial_stream_current;
    typedef
        serial_stream_iterator<bm::decoder_big_endian, block_idx_type> serial_stream_be;
    typedef 
        serial_stream_iterator<bm::decoder_little_endian, block_idx_type> serial_stream_le;
    typedef
        bm::deserializer<BV, bm::decoder_little_endian> deserializer_le;
    typedef
        bm::deserializer<BV, bm::decoder_big_endian> deserializer_be;

private:
    allocator_type       alloc_;
    bm::word_t*          temp_block_;

    /// default stream iterator (same endian)
    bm::iterator_deserializer<BV, serial_stream_current> it_d_;
    /// big-endian stream iterator
    bm::iterator_deserializer<BV, serial_stream_be> it_d_be_;
    /// little-endian stream iterator
    bm::iterator_deserializer<BV, serial_stream_le> it_d_le_;

    deserializer<BV, bm::decoder> de_;
    deserializer_le de_le_;
    deserializer_be de_be_;

    bvector_type bv_tmp_;


    // XOR compression related fields
    //
    const bv_ref_vector_type* ref_vect_; ///< xor ref.vector


};




//----------------------------------------------------------------------------
//
//----------------------------------------------------------------------------


/// \internal
/// \ingroup bvserial
enum serialization_header_mask {
    BM_HM_DEFAULT = 1,
    BM_HM_RESIZE  = (1 << 1),  ///< resized vector
    BM_HM_ID_LIST = (1 << 2),  ///< id list stored
    BM_HM_NO_BO   = (1 << 3),  ///< no byte-order
    BM_HM_NO_GAPL = (1 << 4),  ///< no GAP levels
    BM_HM_64_BIT  = (1 << 5),  ///< 64-bit vector
    BM_HM_HXOR    = (1 << 6),  ///< horizontal XOR compression turned ON
    BM_HM_SPARSE  = (1 << 7)   ///< very sparse vector
};


// --------------------------------------------------------------------
// Serialization stream encoding constants
//

const unsigned char set_block_end               = 0;  //!< End of serialization
const unsigned char set_block_1zero             = 1;  //!< One all-zero block
const unsigned char set_block_1one              = 2;  //!< One block all-set (1111...)
const unsigned char set_block_8zero             = 3;  //!< Up to 256 zero blocks
const unsigned char set_block_8one              = 4;  //!< Up to 256 all-set blocks
const unsigned char set_block_16zero            = 5;  //!< Up to 65536 zero blocks
const unsigned char set_block_16one             = 6;  //!< UP to 65536 all-set blocks
const unsigned char set_block_32zero            = 7;  //!< Up to 4G zero blocks
const unsigned char set_block_32one             = 8;  //!< UP to 4G all-set blocks
const unsigned char set_block_azero             = 9;  //!< All other blocks zero
const unsigned char set_block_aone              = 10; //!< All other blocks one
const unsigned char set_block_bit               = 11; //!< Plain bit block
const unsigned char set_block_sgapbit           = 12; //!< SGAP compressed bitblock
const unsigned char set_block_sgapgap           = 13; //!< SGAP compressed GAP block
const unsigned char set_block_gap               = 14; //!< Plain GAP block
const unsigned char set_block_gapbit            = 15; //!< GAP compressed bitblock
const unsigned char set_block_arrbit            = 16; //!< List of bits ON
const unsigned char set_block_bit_interval      = 17; //!< Interval block
const unsigned char set_block_arrgap            = 18; //!< List of bits ON (GAP block)
const unsigned char set_block_bit_1bit          = 19; //!< Bit block with 1 bit ON
const unsigned char set_block_gap_egamma        = 20; //!< Gamma compressed GAP block
const unsigned char set_block_arrgap_egamma     = 21; //!< Gamma compressed delta GAP array
const unsigned char set_block_bit_0runs         = 22; //!< Bit block with encoded zero intervals
const unsigned char set_block_arrgap_egamma_inv = 23; //!< Gamma compressed inverted delta GAP array
const unsigned char set_block_arrgap_inv        = 24; //!< List of bits OFF (GAP block)
const unsigned char set_block_64zero            = 25; //!< lots of zero blocks
const unsigned char set_block_64one             = 26; //!< lots of all-set blocks

const unsigned char set_block_gap_bienc         = 27; //!< Interpolated GAP block (legacy)
const unsigned char set_block_arrgap_bienc      = 28; //!< Interpolated GAP array
const unsigned char set_block_arrgap_bienc_inv  = 29; //!< Interpolated GAP array (inverted)
const unsigned char set_block_arrbit_inv        = 30; //!< List of bits OFF
const unsigned char set_block_arr_bienc         = 31; //!< Interpolated block as int array
const unsigned char set_block_arr_bienc_inv     = 32; //!< Interpolated inverted block int array
const unsigned char set_block_bitgap_bienc      = 33; //!< Interpolated bit-block as GAPs
const unsigned char set_block_bit_digest0       = 34; //!< H-compression with digest mask

const unsigned char set_block_ref_eq            = 35; //!< block is a copy of a reference block
const unsigned char set_block_xor_ref8          = 36; //!< block is masked XOR of a reference block (8-bit)
const unsigned char set_block_xor_ref16         = 37; //!< block is masked XOR of a reference block (16-bit)
const unsigned char set_block_xor_ref32         = 38; //!< ..... 32-bit (should never happen)
const unsigned char set_block_xor_gap_ref8      = 39; //!< ..... 8-bit
const unsigned char set_block_xor_gap_ref16     = 40; //!< ..... 16-bit
const unsigned char set_block_xor_gap_ref32     = 41; //!< ..... 32-bit (should never happen)
const unsigned char set_block_xor_chain         = 42; //!< XOR chain (composit of sub-blocks)

const unsigned char set_block_gap_bienc_v2        = 43; //!< Interpolated GAP block (v2)
const unsigned char set_block_arrgap_bienc_v2     = 44; //!< //!< Interpolated GAP array (v2)
const unsigned char set_block_arrgap_bienc_inv_v2 = 45; //!< Interpolated GAP array (inverted)
const unsigned char set_block_bitgap_bienc_v2     = 46; //!< Interpolated bit-block as GAPs (v2 - reseved)

const unsigned char set_nb_bookmark16           = 47; //!< jump ahead mark (16-bit)
const unsigned char set_nb_bookmark24           = 48; //!< jump ahead mark (24-bit)
const unsigned char set_nb_bookmark32           = 49; //!< jump ahead mark (32-bit)
const unsigned char set_nb_sync_mark8           = 50; //!< bookmark sync point (8-bits)
const unsigned char set_nb_sync_mark16          = 51;
const unsigned char set_nb_sync_mark24          = 52;
const unsigned char set_nb_sync_mark32          = 53;
const unsigned char set_nb_sync_mark48          = 54;
const unsigned char set_nb_sync_mark64          = 55; //!< ..... 64-bit (should never happen)

const unsigned char set_sblock_bienc            = 56; //!< super-block interpolated list
const unsigned char set_block_arr_bienc_8bh     = 57; //!< BIC block 8bit header 

const unsigned char set_block_xor_ref8_um       = 58; //!< block is un-masked XOR of a reference block (8-bit)
const unsigned char set_block_xor_ref16_um      = 59; //!< block is un-masked XOR of a reference block (16-bit)
const unsigned char set_block_xor_ref32_um      = 60; //!< ..... 32-bit (should never happen)



const unsigned sparse_max_l5 = 48;
const unsigned sparse_max_l6 = 256;

template<class BV>
serializer<BV>::serializer(const allocator_type&   alloc,
                           bm::word_t*             temp_block)
: alloc_(alloc),
  compression_stat_(0),
  gap_serial_(false),
  byte_order_serial_(true),
  sb_bookmarks_(false),
  sb_range_(0),
  compression_level_(bm::set_compression_default),
  enc_header_pos_(0), header_flag_(0),
  ref_vect_(0),
  sim_model_(0),
  ref_idx_(0),
  xor_tmp_block_(0),
  sparse_cutoff_(sparse_max_l6)
{
    bit_idx_arr_.resize(bm::gap_max_bits);
    if (temp_block == 0)
    {
        temp_block_ = alloc_.alloc_bit_block();
        own_temp_block_ = true;
    }
    else
    {
        temp_block_ = temp_block;
        own_temp_block_ = false;
    }
    compression_stat_ = (size_type*) alloc_.alloc_bit_block();
    optimize_ = free_ = false;
    xor_tmp1_ = xor_tmp2_ = 0;
}

template<class BV>
serializer<BV>::serializer(bm::word_t*    temp_block)
: alloc_(allocator_type()),
  compression_stat_(0),
  gap_serial_(false),
  byte_order_serial_(true),
  sb_bookmarks_(false),
  sb_range_(0),
  compression_level_(bm::set_compression_default),
  enc_header_pos_(0), header_flag_(0),
  ref_vect_(0),
  sim_model_(0),
  ref_idx_(0),
  xor_tmp_block_(0),
  sparse_cutoff_(sparse_max_l6)
{
    bit_idx_arr_.resize(bm::gap_max_bits);
    if (temp_block == 0)
    {
        temp_block_ = alloc_.alloc_bit_block();
        own_temp_block_ = true;
    }
    else
    {
        temp_block_ = temp_block;
        own_temp_block_ = false;
    }
    compression_stat_ = (size_type*) alloc_.alloc_bit_block();
    optimize_ = free_ = false;
    xor_tmp1_ = xor_tmp2_ = 0;
}

template<class BV>
serializer<BV>::~serializer()
{
    if (own_temp_block_)
        alloc_.free_bit_block(temp_block_);
    if (compression_stat_)
        alloc_.free_bit_block((bm::word_t*)compression_stat_);
    if (xor_tmp_block_)
        alloc_.free_bit_block(xor_tmp_block_, 3);
}


template<class BV>
void serializer<BV>::reset_compression_stats() BMNOEXCEPT
{
    for (unsigned i = 0; i < 256; ++i)
        compression_stat_[i] = 0;
}

template<class BV>
void serializer<BV>::set_compression_level(unsigned clevel) BMNOEXCEPT
{
    if (clevel <= bm::set_compression_max)
        compression_level_ = clevel;
    if (compression_level_ == 5)
        sparse_cutoff_ = sparse_max_l5;
    else if (compression_level_ == 6)
        sparse_cutoff_ = sparse_max_l6;
}

template<class BV>
void serializer<BV>::set_sparse_cutoff(unsigned cutoff) BMNOEXCEPT
{
    BM_ASSERT(cutoff <= sparse_max_l6);
    if (cutoff <= sparse_max_l6)
        sparse_cutoff_ = cutoff;
    else
        sparse_cutoff_ = sparse_max_l6;
}

template<class BV>
void serializer<BV>::gap_length_serialization(bool value) BMNOEXCEPT
{
    gap_serial_ = value;
}

template<class BV>
void serializer<BV>::byte_order_serialization(bool value) BMNOEXCEPT
{
    byte_order_serial_ = value;
}

template<class BV>
void serializer<BV>::set_bookmarks(bool enable, unsigned bm_interval) BMNOEXCEPT
{
    sb_bookmarks_ = enable;
    if (enable)
    {
        if (bm_interval > 512)
            bm_interval = 512;
        else
            if (bm_interval < 4)
                bm_interval = 4;
    }
    sb_range_ = bm_interval;
}

template<class BV>
void serializer<BV>::set_ref_vectors(const bv_ref_vector_type* ref_vect)
{
    ref_vect_ = ref_vect;
    sim_model_ = 0;
    xor_scan_.set_ref_vector(ref_vect);
    if (!xor_tmp_block_ && ref_vect)
    {
        xor_tmp_block_ = alloc_.alloc_bit_block(3);
        xor_tmp1_ = &xor_tmp_block_[bm::set_block_size];
        xor_tmp2_ = &xor_tmp_block_[bm::set_block_size*2];
    }
}

template<class BV>
bool serializer<BV>::compute_sim_model(xor_sim_model_type&       sim_model,
                                       const bv_ref_vector_type& ref_vect,
                                       const bm::xor_sim_params& params)
{
    return xor_scan_.compute_sim_model(sim_model, ref_vect, params);
}

template<class BV>
void serializer<BV>::set_sim_model(const xor_sim_model_type* sim_model) BMNOEXCEPT
{
    sim_model_ = sim_model;
}

template<class BV>
void serializer<BV>::set_curr_ref_idx(size_type ref_idx) BMNOEXCEPT
{
    ref_idx_ = ref_idx;
}

template<class BV>
void serializer<BV>::encode_header(const BV& bv, bm::encoder& enc) BMNOEXCEPT
{
    const blocks_manager_type& bman = bv.get_blocks_manager();

    header_flag_ = 0;
    if (bv.size() == bm::id_max) // no dynamic resize
        header_flag_ |= BM_HM_DEFAULT;
    else 
        header_flag_ |= BM_HM_RESIZE;

    if (!byte_order_serial_) 
        header_flag_ |= BM_HM_NO_BO;

    if (!gap_serial_) 
        header_flag_ |= BM_HM_NO_GAPL;

    #ifdef BM64ADDR
        header_flag_ |= BM_HM_64_BIT;
    #endif

    if (ref_vect_)
    {
        // TODO: check if XOR search found anything at all
        header_flag_ |= BM_HM_HXOR; // XOR compression turned ON
    }

    enc_header_pos_ = enc.get_pos();
    enc.put_8(header_flag_);

    if (byte_order_serial_)
    {
        ByteOrder bo = globals<true>::byte_order();
        enc.put_8((unsigned char)bo);
    }
    // keep GAP levels information
    if (gap_serial_)
    {
        enc.put_16(bman.glen(), bm::gap_levels);
    }

    // save size (only if bvector has been down-sized)
    if (header_flag_ & BM_HM_RESIZE)
    {
    #ifdef BM64ADDR
        enc.put_64(bv.size());
    #else
        enc.put_32(bv.size());
    #endif
    }
}

template<class BV>
void serializer<BV>::interpolated_encode_gap_block(
            const bm::gap_word_t* gap_block, bm::encoder& enc) BMNOEXCEPT
{
    unsigned len = bm::gap_length(gap_block);
    if (len > 4) // BIC encoding
    {
        encoder::position_type enc_pos0 = enc.get_pos();
        BM_ASSERT(gap_block[len-1] == 65535);

        bm::gap_word_t head = gap_block[0];
        head &= bm::gap_word_t(~(3 << 1)); // clear the level flags
        bm::gap_word_t min_v = gap_block[1];
        bm::gap_word_t max_v = gap_block[len-2];
        bm::gap_word_t tail_delta = bm::gap_word_t(65535 - max_v);

        if (min_v < 256)
            head |= (1 << 1);
        if (tail_delta < 256)
            head |= (1 << 2);

        enc.put_8(bm::set_block_gap_bienc_v2);
        enc.put_16(head);
        if (min_v < 256)
            enc.put_8((unsigned char)min_v);
        else
            enc.put_16(min_v);

        if (tail_delta < 256)
            enc.put_8((unsigned char)tail_delta);
        else
            enc.put_16(tail_delta);

        bit_out_type bout(enc);
        bout.bic_encode_u16(&gap_block[2], len-4, min_v, max_v);
        bout.flush();

        // re-evaluate coding efficiency
        //
        encoder::position_type enc_pos1 = enc.get_pos();
        unsigned gamma_size = (unsigned)(enc_pos1 - enc_pos0);
        if (gamma_size > (len-1)*sizeof(gap_word_t))
        {
            enc.set_pos(enc_pos0);
        }
        else
        {
            compression_stat_[bm::set_block_gap_bienc]++;
            return;
        }
    }

    // save as plain GAP block
    enc.put_8(bm::set_block_gap);
    enc.put_16(gap_block, len-1);

    compression_stat_[bm::set_block_gap]++;
}


template<class BV>
void serializer<BV>::gamma_gap_block(const bm::gap_word_t* gap_block,
                                     bm::encoder& enc) BMNOEXCEPT
{
    unsigned len = gap_length(gap_block);
    if (len > 3 && (compression_level_ > 3)) // Use Elias Gamma encoding
    {
        encoder::position_type enc_pos0 = enc.get_pos();
        {
            bit_out_type bout(enc);
            gamma_encoder_func gamma(bout);

            enc.put_8(bm::set_block_gap_egamma);
            enc.put_16(gap_block[0]);

            for_each_dgap(gap_block, gamma);        
        }
        // re-evaluate coding efficiency
        //
        encoder::position_type enc_pos1 = enc.get_pos();
        unsigned gamma_size = (unsigned)(enc_pos1 - enc_pos0);        
        if (gamma_size > (len-1)*sizeof(gap_word_t))
        {
            enc.set_pos(enc_pos0);
        }
        else
        {
            compression_stat_[bm::set_block_gap_egamma]++;
            return;
        }
    }

    // save as plain GAP block 
    enc.put_8(bm::set_block_gap);
    enc.put_16(gap_block, len-1);
    
    compression_stat_[bm::set_block_gap]++;
}

template<class BV>
void serializer<BV>::gamma_gap_array(const bm::gap_word_t* gap_array, 
                                     unsigned              arr_len, 
                                     bm::encoder&          enc,
                                     bool                  inverted) BMNOEXCEPT
{
    unsigned char scode = inverted ? bm::set_block_arrgap_egamma_inv
                                   : bm::set_block_arrgap_egamma;
    if (compression_level_ > 3 && arr_len > 1)
    {        
        encoder::position_type enc_pos0 = enc.get_pos();
        {
            bit_out_type bout(enc);
            enc.put_8(scode);
            bout.gamma(arr_len);
            gap_word_t prev = gap_array[0];
            bout.gamma(prev + 1);

            for (unsigned i = 1; i < arr_len; ++i)
            {
                gap_word_t curr = gap_array[i];
                bout.gamma(curr - prev);
                prev = curr;
            }
        }
        encoder::position_type enc_pos1 = enc.get_pos();
        unsigned gamma_size = (unsigned)(enc_pos1 - enc_pos0);
        unsigned plain_size = (unsigned)(sizeof(gap_word_t)+arr_len*sizeof(gap_word_t));
        if (gamma_size >= plain_size)
        {
            enc.set_pos(enc_pos0); // rollback the bit stream
        }
        else
        {
            compression_stat_[scode]++;
            return;
        }
    }
    // save as a plain array
    scode = inverted ? bm::set_block_arrgap_inv : bm::set_block_arrgap;
    enc.put_prefixed_array_16(scode, gap_array, arr_len, true);
    compression_stat_[scode]++;
}


template<class BV>
void serializer<BV>::interpolated_gap_array_v0(
                                const bm::gap_word_t* gap_block,
                                unsigned              arr_len,
                                bm::encoder&          enc,
                                bool                  inverted) BMNOEXCEPT
{
    BM_ASSERT(arr_len <= 65535);
    unsigned char scode = inverted ? bm::set_block_arrgap_bienc_inv
                                   : bm::set_block_arrgap_bienc;
    if (arr_len > 4)
    {
        encoder::position_type enc_pos0 = enc.get_pos();
        {
            bit_out_type bout(enc);
            
            bm::gap_word_t min_v = gap_block[0];
            bm::gap_word_t max_v = gap_block[arr_len-1];
            BM_ASSERT(max_v > min_v);

            enc.put_8(scode);
            enc.put_16(min_v);
            enc.put_16(max_v);

            bout.gamma(arr_len-4);
            bout.bic_encode_u16(&gap_block[1], arr_len-2, min_v, max_v);
            bout.flush();
        }
        encoder::position_type enc_pos1 = enc.get_pos();
        unsigned enc_size = (unsigned)(enc_pos1 - enc_pos0);
        unsigned raw_size = (unsigned)(sizeof(gap_word_t)+arr_len*sizeof(gap_word_t));
        if (enc_size >= raw_size)
        {
            enc.set_pos(enc_pos0); // rollback the bit stream
        }
        else
        {
            compression_stat_[scode]++;
            return;
        }
    }
    // save as a plain array
    scode = inverted ? bm::set_block_arrgap_inv : bm::set_block_arrgap;
    enc.put_prefixed_array_16(scode, gap_block, arr_len, true);
    compression_stat_[scode]++;
}


template<class BV>
void serializer<BV>::interpolated_gap_array(const bm::gap_word_t* gap_block,
                                            unsigned              arr_len,
                                            bm::encoder&          enc,
                                            bool                  inverted) BMNOEXCEPT
{
    BM_ASSERT(arr_len <= 65535);

    unsigned char scode = inverted ? bm::set_block_arrgap_bienc_inv_v2
                                   : bm::set_block_arrgap_bienc_v2;
    if (arr_len > 4)
    {
        bm::gap_word_t min_v = gap_block[0];
        bm::gap_word_t max_v = gap_block[arr_len-1];
        bm::gap_word_t tail = bm::gap_word_t(max_v - min_v);

        if (min_v >= 256 && tail >= 256)// || arr_len > 128)
        {
            interpolated_gap_array_v0(gap_block, arr_len, enc, inverted);
            return;
        }

        BM_ASSERT(arr_len < 16383);
        encoder::position_type enc_pos0 = enc.get_pos();
        {
            bit_out_type bout(enc);

            BM_ASSERT(max_v > min_v);

            enc.put_8(scode);

            BM_ASSERT((arr_len & (3 << 14)) == 0);
            arr_len <<= 2;
            if (min_v < 256)
                arr_len |= 1;
            if (tail < 256)
                arr_len |= (1 << 1);

            enc.put_16(bm::gap_word_t(arr_len));
            if (min_v < 256)
                enc.put_8((unsigned char)min_v);
            else
                enc.put_16(min_v);

            if (tail < 256)
                enc.put_8((unsigned char)tail);
            else
                enc.put_16(tail);

            arr_len >>= 2;

            bout.bic_encode_u16(&gap_block[1], arr_len-2, min_v, max_v);
            bout.flush();
        }
        encoder::position_type enc_pos1 = enc.get_pos();
        unsigned enc_size = (unsigned)(enc_pos1 - enc_pos0);
        unsigned raw_size = (unsigned)(sizeof(gap_word_t)+arr_len*sizeof(gap_word_t));
        if (enc_size >= raw_size)
        {
            enc.set_pos(enc_pos0); // rollback the bit stream
        }
        else
        {
            compression_stat_[scode]++;
            return;
        }
    }
    // save as a plain array
    scode = inverted ? bm::set_block_arrgap_inv : bm::set_block_arrgap;
    enc.put_prefixed_array_16(scode, gap_block, arr_len, true);
    compression_stat_[scode]++;
}



template<class BV>
void serializer<BV>::add_model(unsigned char mod, unsigned score) BMNOEXCEPT
{
    BM_ASSERT(mod_size_ < 64); // too many models (memory corruption?)
    scores_[mod_size_] = score; models_[mod_size_] = mod;
    ++mod_size_;
}

template<class BV>
unsigned char
serializer<BV>::find_bit_best_encoding_l5(const bm::word_t* block) BMNOEXCEPT
{
    const float bie_bits_per_int = compression_level_ < 6 ? 3.75f : 2.5f;
    const unsigned bie_limit = unsigned(float(bm::gap_max_bits) / bie_bits_per_int);

    unsigned bc, ibc, gc;
    
    add_model(bm::set_block_bit, bm::gap_max_bits); // default model (bit-block)
    
    bit_model_0run_size_ = bm::bit_count_nonzero_size(block, bm::set_block_size);
    add_model(bm::set_block_bit_0runs, bit_model_0run_size_ * 8);

    bm::id64_t d0 = digest0_ = bm::calc_block_digest0(block);
    if (!d0)
        return bm::set_block_azero;

    unsigned d0_bc = word_bitcount64(d0);
    bit_model_d0_size_ = unsigned(8 + (32 * d0_bc * sizeof(bm::word_t)));
    if (d0 != ~0ull)
        add_model(bm::set_block_bit_digest0, bit_model_d0_size_ * 8);

    bm::bit_block_change_bc(block, &gc, &bc);
    ibc = bm::gap_max_bits - bc;
    if (bc == 1)
        return bm::set_block_bit_1bit;
    if (!ibc)
        return bm::set_block_aone;
    {
        unsigned arr_size =
            unsigned(sizeof(gap_word_t) + (bc * sizeof(gap_word_t)));
        unsigned arr_size_inv =
            unsigned(sizeof(gap_word_t) + (ibc * sizeof(gap_word_t)));

        add_model(bm::set_block_arrbit, arr_size * 8);
        add_model(bm::set_block_arrbit_inv, arr_size_inv * 8);
    }

    float gcf=float(gc);

    if (gc > 3 && gc < bm::gap_max_buff_len)
        add_model(bm::set_block_gap_bienc,
                  32 + unsigned((gcf-1) * bie_bits_per_int));


    float bcf=float(bc), ibcf=float(ibc);

    if (bc < bie_limit) 
        add_model(bm::set_block_arr_bienc, 16 * 3 + unsigned(bcf * bie_bits_per_int));
    else
    {
        if (ibc < bie_limit)
            add_model(bm::set_block_arr_bienc_inv,
                      16 * 3 + unsigned(ibcf * bie_bits_per_int));
    }

    gc -= gc > 2 ? 2 : 0;
    gcf = float(gc);
    if (gc < bm::gap_max_buff_len) // GAP block
    {
        add_model(bm::set_block_bitgap_bienc,
                  16 * 4 + unsigned(gcf * bie_bits_per_int));
    }
    else
    {
        if (gc < bie_limit)
            add_model(bm::set_block_bitgap_bienc,
                      16 * 4 + unsigned(gcf * bie_bits_per_int));
    }

    // find the best representation based on computed approx.models
    //
    unsigned min_score = bm::gap_max_bits;
    unsigned char model = bm::set_block_bit;
    for (unsigned i = 0; i < mod_size_; ++i)
    {
        if (scores_[i] < min_score)
        {
            min_score = scores_[i];
            model = models_[i];
        }
    }
#if 0
    if (model == set_block_bit_0runs)
    {
        std::cout << "   0runs=" << (bit_model_0run_size_ * 8) << std::endl;
        std::cout << " GAP BIC=" << (16 * 4 + unsigned(gcf * bie_bits_per_int)) << std::endl;
        std::cout << " ARR BIC=" << (16 * 3 + unsigned(bcf * bie_bits_per_int)) << std::endl;
        std::cout << "BC,GC=[" << bc <<", " << gc << "]" << std::endl;
        std::cout << "bie_limit=" << bie_limit << std::endl;

    }
    switch(model)
    {
    case set_block_bit: std::cout << "BIT=" << "[" << bc <<", " << gc << "]"; break;
    case set_block_bit_1bit: std::cout << "1bit "; break;
    case set_block_arrgap: std::cout << "arr_gap "; break;
    case set_block_arrgap_egamma: std::cout << "arrgap_egamma "; break;
    case set_block_arrgap_bienc: std::cout << "arrgap_BIC "; break;
    case set_block_arrgap_inv: std::cout << "arrgap_INV "; break;
    case set_block_arrgap_egamma_inv: std::cout << "arrgap_egamma_INV "; break;
    case set_block_arrgap_bienc_inv: std::cout << "arrgap_BIC_INV "; break;
    case set_block_gap_egamma: std::cout << "gap_egamma "; break;
    case set_block_gap_bienc: std::cout << "gap_BIC "; break;
    case set_block_bitgap_bienc: std::cout << "bitgap_BIC "; break;
    case set_block_bit_digest0: std::cout << "D0 "; break;
    case set_block_bit_0runs: std::cout << "0runs=[" << bc <<", " << gc << " lmt=" << bie_limit << "]"; break;
    case set_block_arr_bienc_inv: std::cout << "arr_BIC_INV "; break;
    case set_block_arr_bienc: std::cout << "arr_BIC "; break;
    default: std::cout << "UNK=" << int(model); break;
    }
#endif
    return model;
}

template<class BV>
unsigned char
serializer<BV>::find_bit_best_encoding(const bm::word_t* block) BMNOEXCEPT
{
    // experimental code
#if 0
    {
        const float bie_bits_per_int = compression_level_ < 6 ? 3.75f : 2.5f;

        unsigned wave_matches[e_bit_end] = {0, };

        bm::block_waves_xor_descr x_descr;
        unsigned s_gc, s_bc;
        bm::compute_s_block_descr(block, x_descr, &s_gc, &s_bc);
        const unsigned wave_max_bits = bm::set_block_digest_wave_size * 32;

        for (unsigned i = 0; i < bm::block_waves; ++i)
        {
            s_gc =  x_descr.sb_gc[i];
            s_bc = x_descr.sb_bc[i];
            unsigned curr_best;
            bm::bit_representation best_rep =
                bm::best_representation(s_bc, s_gc, wave_max_bits, bie_bits_per_int, &curr_best);
            wave_matches[best_rep]++;
        } // for
        unsigned sum_cases = 0;
        bool sub_diff = false;
        unsigned v_count = 0;
        for (unsigned i = 0; i < e_bit_end; ++i)
        {
            sum_cases += wave_matches[i];
            if (wave_matches[i] != 0 && wave_matches[i] < 64)
            {
                sub_diff = true; v_count++;
            }
        }
        if (sub_diff)
        {
            std::cout << "-" << v_count;
            if (v_count > 2)
            {
                for (unsigned i=0; i < e_bit_end; ++i)
                {
                    if (wave_matches[i])
                    {
                        switch (i)
                        {
                        case e_bit_GAP: std::cout << " G" << wave_matches[i]; break;
                        case e_bit_INT: std::cout << " I" << wave_matches[i]; break;
                        case e_bit_IINT: std::cout << "iI" << wave_matches[i]; break;
                        case e_bit_1: std::cout << " 1s"  << wave_matches[i]; break;
                        case e_bit_0: std::cout << " 0s" << wave_matches[i]; break;
                        case e_bit_bit: std::cout << " B" << wave_matches[i]; break;
                        }
                    }
                } // for
                std::cout << " |";
            }
        }
        else
        {
            //std::cout << "=";
        }
        BM_ASSERT(sum_cases == 64);
    }
#endif
    reset_models();
    
    if (compression_level_ >= 5)
        return find_bit_best_encoding_l5(block);
    
    unsigned bc, bit_gaps;
    
    // heuristics and hard-coded rules to determine
    // the best representation for bit-block
    //
    add_model(bm::set_block_bit, bm::gap_max_bits); // default model (bit-block)
    
    if (compression_level_ <= 1)
        return bm::set_block_bit;

    // check if it is a very sparse block with some areas of dense areas
    bit_model_0run_size_ = bm::bit_count_nonzero_size(block, bm::set_block_size);
    if (compression_level_ <= 5)
        add_model(bm::set_block_bit_0runs, bit_model_0run_size_ * 8);
    
    if (compression_level_ >= 2)
    {
        bm::id64_t d0 = digest0_ = bm::calc_block_digest0(block);
        if (!d0)
        {
            add_model(bm::set_block_azero, 0);
            return bm::set_block_azero;
        }
        unsigned d0_bc = word_bitcount64(d0);
        bit_model_d0_size_ = unsigned(8 + (32 * d0_bc * sizeof(bm::word_t)));
        if (d0 != ~0ull)
            add_model(bm::set_block_bit_digest0, bit_model_d0_size_ * 8);

        if (compression_level_ >= 4)
        {
            bm::bit_block_change_bc(block, &bit_gaps, &bc);
        }
        else
        {
            bc = bm::bit_block_count(block);
            bit_gaps = 65535;
        }
        BM_ASSERT(bc);

        if (bc == 1)
        {
            add_model(bm::set_block_bit_1bit, 16);
            return bm::set_block_bit_1bit;
        }
        unsigned inverted_bc = bm::gap_max_bits - bc;
        if (!inverted_bc)
        {
            add_model(bm::set_block_aone, 0);
            return bm::set_block_aone;
        }
        
        if (compression_level_ >= 3)
        {
            unsigned arr_size =
                unsigned(sizeof(gap_word_t) + (bc * sizeof(gap_word_t)));
            unsigned arr_size_inv =
                unsigned(sizeof(gap_word_t) + (inverted_bc * sizeof(gap_word_t)));
            
            add_model(bm::set_block_arrbit, arr_size*8);
            add_model(bm::set_block_arrbit_inv, arr_size_inv*8);
            
            if (compression_level_ >= 4)
            {
                const unsigned gamma_bits_per_int = 6;
                //unsigned bit_gaps = bm::bit_block_calc_change(block);

                if (compression_level_ == 4)
                {
                    if (bit_gaps > 3 && bit_gaps < bm::gap_max_buff_len)
                        add_model(bm::set_block_gap_egamma,
                                  16 + (bit_gaps-1) * gamma_bits_per_int);
                    if (bc < bit_gaps && bc < bm::gap_equiv_len)
                        add_model(bm::set_block_arrgap_egamma,
                                  16 + bc * gamma_bits_per_int);
                    if (inverted_bc > 3 && inverted_bc < bit_gaps && inverted_bc < bm::gap_equiv_len)
                        add_model(bm::set_block_arrgap_egamma_inv,
                                  16 + inverted_bc * gamma_bits_per_int);
                }
            } // level >= 3
        } // level >= 3
    } // level >= 2
    
    // find the best representation based on computed approx.models
    //
    unsigned min_score = bm::gap_max_bits;
    unsigned char model = bm::set_block_bit;
    for (unsigned i = 0; i < mod_size_; ++i)
    {
        if (scores_[i] < min_score)
        {
            min_score = scores_[i];
            model = models_[i];
        }
    }
    return model;
}

template<class BV>
unsigned char
serializer<BV>::find_gap_best_encoding(const bm::gap_word_t* gap_block) BMNOEXCEPT
{
    // heuristics and hard-coded rules to determine
    // the best representation for d-GAP block
    //
    if (compression_level_ <= 2)
        return bm::set_block_gap;

    unsigned len = bm::gap_length(gap_block);
    if (len == 2)
        return bm::set_block_gap;

    unsigned bc = bm::gap_bit_count_unr(gap_block);
    unsigned ibc = bm::gap_max_bits - bc;

    if (bc == 1)
        return bm::set_block_bit_1bit;

    bc += 2; ibc += 2; // correct counts because effective GAP len = len - 2
    if (bc < len)
    {
        if (compression_level_ < 4 || len < 6)
            return bm::set_block_arrgap;

        if (compression_level_ == 4)
            return bm::set_block_arrgap_egamma;

        return bm::set_block_arrgap_bienc;
    }
    if (ibc < len)
    {
        if (compression_level_ < 4 || len < 6)
            return bm::set_block_arrgap_inv;

        if (compression_level_ == 4)
            return bm::set_block_arrgap_egamma_inv;
        return bm::set_block_arrgap_bienc_inv;
    }
    if (len < 6)
        return bm::set_block_gap;

    if (compression_level_ == 4)
        return bm::set_block_gap_egamma;

    return bm::set_block_gap_bienc;
}




template<class BV>
void serializer<BV>::encode_gap_block(const bm::gap_word_t* gap_block, bm::encoder& enc)
{
    gap_word_t*  gap_temp_block = (gap_word_t*) temp_block_;
    
    gap_word_t arr_len;
    bool invert = false;

    unsigned char enc_choice = find_gap_best_encoding(gap_block);
    switch (enc_choice)
    {
    case bm::set_block_gap:
        gamma_gap_block(gap_block, enc); // TODO: use plain encode (non-gamma)
        break;
        
    case bm::set_block_bit_1bit:
        arr_len = bm::gap_convert_to_arr(gap_temp_block,
                                         gap_block,
                                         bm::gap_equiv_len-10);
        BM_ASSERT(arr_len == 1);
        enc.put_8(bm::set_block_bit_1bit);
        enc.put_16(gap_temp_block[0]);
        compression_stat_[bm::set_block_bit_1bit]++;
        break;
    case bm::set_block_arrgap_inv:
    case bm::set_block_arrgap_egamma_inv:
        invert = true;
        BM_FALLTHROUGH;
        // fall through
    case bm::set_block_arrgap:
        BM_FALLTHROUGH;
        // fall through
    case bm::set_block_arrgap_egamma:
        arr_len = gap_convert_to_arr(gap_temp_block,
                                     gap_block,
                                     bm::gap_equiv_len-10,
                                     invert);
        BM_ASSERT(arr_len);
        gamma_gap_array(gap_temp_block, arr_len, enc, invert);
        break;
    case bm::set_block_gap_bienc:
        interpolated_encode_gap_block(gap_block, enc);
        break;
    case bm::set_block_arrgap_bienc_inv:
        invert = true;
        BM_FALLTHROUGH;
        // fall through
    case bm::set_block_arrgap_bienc:
        arr_len = gap_convert_to_arr(gap_temp_block,
                                     gap_block,
                                     bm::gap_equiv_len-64,
                                     invert);
        BM_ASSERT(arr_len);
        interpolated_gap_array(gap_temp_block, arr_len, enc, invert);
        break;
    default:
        gamma_gap_block(gap_block, enc);
    } // switch
}

template<class BV>
void serializer<BV>::encode_bit_interval(const bm::word_t* blk, 
                                         bm::encoder&      enc,
                                         unsigned          //size_control
                                         ) BMNOEXCEPT
{
    enc.put_8(bm::set_block_bit_0runs);
    enc.put_8((blk[0]==0) ? 0 : 1); // encode start
    
    unsigned i, j;
    for (i = 0; i < bm::set_block_size; ++i)
    {
        if (blk[i] == 0)
        {
            // scan fwd to find 0 island length
            for (j = i+1; j < bm::set_block_size; ++j)
            {
                if (blk[j] != 0)
                    break;
            }
            BM_ASSERT(j-i);
            enc.put_16((gap_word_t)(j-i)); 
            i = j - 1;
        }
        else
        {
            // scan fwd to find non-0 island length
            for (j = i+1; j < bm::set_block_size; ++j)
            {
                if (blk[j] == 0)
                {
                    // look ahead to identify and ignore short 0-run
                    if (((j+1 < bm::set_block_size) && blk[j+1]) ||
                        ((j+2 < bm::set_block_size) && blk[j+2]))
                    {
                        ++j; // skip zero word
                        continue;
                    }
                    break;
                }
            }
            BM_ASSERT(j-i);
            enc.put_16((gap_word_t)(j-i));
            enc.put_32(blk + i, j - i); // stream all bit-words now

            i = j - 1;
        }
    }
    compression_stat_[bm::set_block_bit_0runs]++;
}


template<class BV>
void serializer<BV>::encode_bit_digest(const bm::word_t* block,
                                       bm::encoder&     enc,
                                       bm::id64_t       d0) BMNOEXCEPT
{
    // evaluate a few "sure" models here and pick the best
    //
    if (d0 != ~0ull)
    {
        if (bit_model_0run_size_ < bit_model_d0_size_)
        {
            encode_bit_interval(block, enc, 0); // TODO: get rid of param 3 (0)
            return;
        }
        
        // encode using digest0 method
        //
        enc.put_8(bm::set_block_bit_digest0);
        enc.put_64(d0);
        while (d0)
        {
            bm::id64_t t = bm::bmi_blsi_u64(d0); // d & -d;
            
            unsigned wave = bm::word_bitcount64(t - 1);
            unsigned off = wave * bm::set_block_digest_wave_size;
            unsigned j = 0;
            do
            {
                enc.put_32(block[off+j+0]);
                enc.put_32(block[off+j+1]);
                enc.put_32(block[off+j+2]);
                enc.put_32(block[off+j+3]);
                j += 4;
            } while (j < bm::set_block_digest_wave_size);
            
            d0 = bm::bmi_bslr_u64(d0); // d &= d - 1;
        } // while
        
        compression_stat_[bm::set_block_bit_digest0]++;
    }
    else
    {
        if (bit_model_0run_size_ < unsigned(bm::set_block_size*sizeof(bm::word_t)))
        {
            encode_bit_interval(block, enc, 0); // TODO: get rid of param 3 (0)
            return;
        }

        enc.put_prefixed_array_32(bm::set_block_bit, block, bm::set_block_size);
        compression_stat_[bm::set_block_bit]++;
    }
}

template<class BV>
void serializer<BV>::encode_xor_match_chain(bm::encoder& enc,
                            const block_match_chain_type& mchain) BMNOEXCEPT
{
    size_type chain_size = (size_type)mchain.chain_size;
    BM_ASSERT(chain_size);

    unsigned char vbr_flag = bm::check_pair_vect_vbr(mchain, *ref_vect_);

    enc.put_8(bm::set_block_xor_chain);
    enc.put_8(vbr_flag); // flag (reserved)

    size_type ridx = mchain.ref_idx[0];
    bm::id64_t d64 = mchain.xor_d64[0];
    ridx = ref_vect_->get_row_idx(ridx);

    switch(vbr_flag)
    {
    case 1: enc.put_8((unsigned char)ridx); break;
    case 2: enc.put_16((unsigned short)ridx); break;
    case 0: enc.put_32((unsigned)ridx); break;
    default: BM_ASSERT(0); break;
    } // switch
    enc.put_h64(d64);
    enc.put_8((unsigned char) (chain_size-1));

    for (unsigned ci = 1; ci < chain_size; ++ci)
    {
        ridx = mchain.ref_idx[ci];
        d64 = mchain.xor_d64[ci];
        ridx = ref_vect_->get_row_idx(ridx);
        switch(vbr_flag)
        {
        case 1: enc.put_8((unsigned char)ridx); break;
        case 2: enc.put_16((unsigned short)ridx); break;
        case 0: enc.put_32((unsigned)ridx); break;
        default: BM_ASSERT(0); break;
        } // switch
        enc.put_h64(d64);
    } // for ci
    compression_stat_[bm::set_block_xor_chain]++;
}



template<class BV>
void serializer<BV>::xor_tmp_product(
                    const bm::word_t* s_block,
                    const block_match_chain_type& mchain,
                    unsigned i, unsigned j) BMNOEXCEPT
{
    if (BM_IS_GAP(s_block))
    {
        bm::gap_convert_to_bitset(xor_tmp1_, BMGAP_PTR(s_block));
        s_block = xor_tmp1_;
    }
    size_type ridx = mchain.ref_idx[0];
    const bm::word_t* ref_block = xor_scan_.get_ref_block(ridx, i, j);
    if (BM_IS_GAP(ref_block))
    {
        bm::gap_convert_to_bitset(xor_tmp2_, BMGAP_PTR(ref_block));
        ref_block = xor_tmp2_;
    }
    bm::id64_t d64 = mchain.xor_d64[0];
    bm::bit_block_xor(xor_tmp_block_, s_block, ref_block, d64);
    for (unsigned k = 1; k < mchain.chain_size; ++k)
    {
        ridx = mchain.ref_idx[k];
        ref_block = xor_scan_.get_ref_block(ridx, i, j);
        if (BM_IS_GAP(ref_block))
        {
            bm::gap_convert_to_bitset(xor_tmp2_, BMGAP_PTR(ref_block));
            ref_block = xor_tmp2_;
        }
        d64 = mchain.xor_d64[k];
        bm::bit_block_xor(xor_tmp_block_, ref_block, d64);
    } // for i
}


template<class BV>
void serializer<BV>::serialize(const BV& bv,
                               typename serializer<BV>::buffer& buf,
                               const statistics_type* bv_stat)
{
    statistics_type stat;
    if (!bv_stat)
    {
        bv.calc_stat(&stat);
        bv_stat = &stat;
    }
    
    buf.resize(bv_stat->max_serialize_mem, false); // no-copy resize
    optimize_ = free_ = false;

    unsigned char* data_buf = buf.data();
    size_t buf_size = buf.size();
    size_type slen = this->serialize(bv, data_buf, buf_size);
    BM_ASSERT(slen <= buf.size()); // or we have a BIG problem with prediction
    BM_ASSERT(slen);
    
    buf.resize(slen);
}

template<class BV>
void serializer<BV>::optimize_serialize_destroy(BV& bv,
                                        typename serializer<BV>::buffer& buf)
{
    statistics_type st;
    optimize_ = true;
    if (!ref_vect_) // do not use block-free if XOR compression is setup
        free_ = true; // set the destructive mode

    typename bvector_type::mem_pool_guard mp_g_z;
    mp_g_z.assign_if_not_set(pool_, bv);

    bv.optimize(temp_block_, BV::opt_compress, &st);
    serialize(bv, buf, &st);
    
    optimize_ = free_ = false; // restore the default mode
}

template<class BV>
void serializer<BV>::encode_bit_array(const bm::word_t* block,
                                      bm::encoder&      enc,
                                      bool              inverted) BMNOEXCEPT
{
    unsigned arr_len =
        bm::bit_block_convert_to_arr(bit_idx_arr_.data(), block, inverted);

    if (arr_len)
    {
        unsigned char scode =
                    inverted ? bm::set_block_arrbit_inv : bm::set_block_arrbit;
        enc.put_prefixed_array_16(scode, bit_idx_arr_.data(), arr_len, true);
        compression_stat_[scode]++;
        return;
    }
    encode_bit_digest(block, enc, digest0_);
}

template<class BV>
void serializer<BV>::gamma_gap_bit_block(const bm::word_t* block,
                                         bm::encoder&      enc) BMNOEXCEPT
{
    unsigned len = bm::bit_to_gap(bit_idx_arr_.data(), block, bm::gap_equiv_len);
    BM_ASSERT(len); (void)len;
    gamma_gap_block(bit_idx_arr_.data(), enc);
}

template<class BV>
void serializer<BV>::gamma_arr_bit_block(const bm::word_t* block,
                                         bm::encoder&      enc,
                                         bool              inverted) BMNOEXCEPT
{
    unsigned arr_len = 
        bm::bit_block_convert_to_arr(bit_idx_arr_.data(), block, inverted);
    if (arr_len)
    {
        gamma_gap_array(bit_idx_arr_.data(), arr_len, enc, inverted);
        return;
    }
    enc.put_prefixed_array_32(bm::set_block_bit, block, bm::set_block_size);
    compression_stat_[bm::set_block_bit]++;
}

template<class BV>
void serializer<BV>::bienc_arr_bit_block(const bm::word_t* block,
                                        bm::encoder&       enc,
                                        bool               inverted) BMNOEXCEPT
{
    unsigned arr_len = 
        bm::bit_block_convert_to_arr(bit_idx_arr_.data(), block, inverted);
    if (arr_len)
    {
        interpolated_gap_array(bit_idx_arr_.data(), arr_len, enc, inverted);
        return;
    }
    encode_bit_digest(block, enc, digest0_);
}

template<class BV>
void serializer<BV>::interpolated_gap_bit_block(const bm::word_t* block,
                                                bm::encoder&      enc) BMNOEXCEPT
{
    unsigned len = bm::bit_to_gap(bit_idx_arr_.data(), block, bm::gap_max_bits);
    BM_ASSERT(len); (void)len;
    interpolated_encode_gap_block(bit_idx_arr_.data(), enc);
}


template<class BV>
void serializer<BV>::bienc_gap_bit_block(const bm::word_t* block,
                                         bm::encoder& enc) BMNOEXCEPT
{
    unsigned len = bm::bit_to_gap(bit_idx_arr_.data(), block, bm::gap_max_bits);
    BM_ASSERT(len); (void)len;
    
    const unsigned char scode = bm::set_block_bitgap_bienc;

    encoder::position_type enc_pos0 = enc.get_pos();
    {
        bit_out_type bout(enc);
        
        bm::gap_word_t head = (bit_idx_arr_[0] & 1); // isolate start flag
        bm::gap_word_t min_v = bit_idx_arr_[1];

        BM_ASSERT(bit_idx_arr_[len] == 65535);
        BM_ASSERT(bit_idx_arr_[len] > min_v);

        enc.put_8(scode);
        
        enc.put_8((unsigned char)head);
        enc.put_16(bm::gap_word_t(len));
        enc.put_16(min_v);
        bout.bic_encode_u16(&bit_idx_arr_[2], len-2, min_v, 65535);
        bout.flush();
    }
    encoder::position_type enc_pos1 = enc.get_pos();
    unsigned enc_size = (unsigned)(enc_pos1 - enc_pos0);
    unsigned raw_size = sizeof(word_t) * bm::set_block_size;
    if (enc_size >= raw_size)
    {
        enc.set_pos(enc_pos0); // rollback the bit stream
    }
    else
    {
        compression_stat_[scode]++;
        return;
    }
    // if we got to this point it means coding was not efficient 
    // and we rolled back to simpler method
    //
    encode_bit_digest(block, enc, digest0_);
}

//--------------------------------------------------------------------
//
const unsigned sblock_flag_sb16  = (1u << 0); ///< 16-bit SB index (8-bit by default) 
const unsigned sblock_flag_sb32  = (1u << 1); ///< 32-bit SB index 

const unsigned sblock_flag_min16 = (1u << 2); ///< 16-bit minv
const unsigned sblock_flag_min24 = (1u << 3); ///< 24-bit minv
const unsigned sblock_flag_min32 = bm::sblock_flag_min16 | bm::sblock_flag_min24;

const unsigned sblock_flag_len16 = (1u << 4); ///< 16-bit len (8-bit by default)
const unsigned sblock_flag_max16 = (1u << 5); 
const unsigned sblock_flag_max24 = (1u << 6);
const unsigned sblock_flag_max32 = bm::sblock_flag_max16 | bm::sblock_flag_max24;

template<class BV>
void serializer<BV>::bienc_arr_sblock(const BV& bv, unsigned sb,
    bm::encoder& enc) BMNOEXCEPT
{
    unsigned sb_flag = 0;

    unsigned char scode = bm::set_sblock_bienc;
    bm::convert_sub_to_arr(bv, sb, sb_bit_idx_arr_);
    unsigned len = (unsigned)sb_bit_idx_arr_.size();

    BM_ASSERT(sb_bit_idx_arr_.size() < 65536);
    BM_ASSERT(sb_bit_idx_arr_.size());

    bm::word_t min_v = sb_bit_idx_arr_[0];
    bm::word_t max_v = sb_bit_idx_arr_[len - 1];
    BM_ASSERT(max_v <= bm::set_sub_total_bits);
    bm::word_t max_v_delta = bm::set_sub_total_bits - max_v;

    // build decoding flags
    if (sb > 65535)
        sb_flag |= bm::sblock_flag_sb32;
    else if (sb > 255)
        sb_flag |= bm::sblock_flag_sb16;

    if (len > 255)
        sb_flag |= bm::sblock_flag_len16;

    if (min_v > 65535)
        if (min_v < 0xFFFFFF)
            sb_flag |= bm::sblock_flag_min24;
        else
            sb_flag |= bm::sblock_flag_min32; // 24 and 16
    else if (min_v > 255)
        sb_flag |= bm::sblock_flag_min16;

    if (max_v_delta > 65535)
        if (max_v_delta < 0xFFFFFF)
            sb_flag |= bm::sblock_flag_max24;
        else
            sb_flag |= bm::sblock_flag_max32;
    else if (max_v_delta > 255)
        sb_flag |= bm::sblock_flag_max16;

    // encoding header 
    //
    enc.put_8(scode);
    enc.put_8((unsigned char)sb_flag);

    if (sb > 65535)
        enc.put_32(sb);
    else if (sb > 255)
        enc.put_16((unsigned short)sb);
    else
        enc.put_8((unsigned char)sb);

    if (len > 255)
        enc.put_16((unsigned short)len);
    else
        enc.put_8((unsigned char)len);

    if (min_v > 65535)
        if (min_v < 0xFFFFFF)
            enc.put_24(min_v);
        else
            enc.put_32(min_v);
    else if (min_v > 255)
        enc.put_16((unsigned short)min_v);
    else
        enc.put_8((unsigned char)min_v);

    if (max_v_delta > 65535)
        if (max_v < 0xFFFFFF)
            enc.put_24(max_v_delta);
        else
            enc.put_32(max_v_delta);
    else if (max_v_delta > 255)
        enc.put_16((unsigned short)max_v_delta);
    else
        enc.put_8((unsigned char)max_v_delta);

    bit_out_type bout(enc);
    bout.bic_encode_u32_cm(sb_bit_idx_arr_.data()+1,
                           (unsigned)sb_bit_idx_arr_.size()-2,
                           min_v, max_v);

    compression_stat_[scode]++;
}



template<class BV>
void
serializer<BV>::interpolated_arr_bit_block(const bm::word_t* block,
                                           bm::encoder&      enc,
                                           bool              inverted) BMNOEXCEPT
{
    unsigned arr_len = bm::bit_block_convert_to_arr(
                            bit_idx_arr_.data(), block, inverted);

    if (arr_len)
    {
        unsigned char scode =
            inverted ? bm::set_block_arr_bienc_inv : bm::set_block_arr_bienc;
        
        encoder::position_type enc_pos0 = enc.get_pos();
        {
            bit_out_type bout(enc);
            
            bm::gap_word_t min_v = bit_idx_arr_[0];
            bm::gap_word_t max_v = bit_idx_arr_[arr_len-1];
            BM_ASSERT(max_v > min_v);
            bm::gap_word_t max_delta = bm::gap_word_t(65536 - max_v);

            if (!inverted && min_v <= 0xFF && max_delta <= 0xFF) // 8-bit header
            {
                enc.put_8(bm::set_block_arr_bienc_8bh);
                enc.put_8((unsigned char)min_v);
                enc.put_8((unsigned char)max_delta);
            }
            else
            {
                enc.put_8(scode);
                enc.put_16(min_v);
                enc.put_16(max_v);
            }
            enc.put_16(bm::gap_word_t(arr_len));

            bout.bic_encode_u16(&bit_idx_arr_[1], arr_len-2, min_v, max_v);
            bout.flush();
        }
        encoder::position_type enc_pos1 = enc.get_pos();
        unsigned enc_size = (unsigned)(enc_pos1 - enc_pos0);
        unsigned raw_size = sizeof(word_t) * bm::set_block_size;
        if (enc_size >= raw_size)
        {
            enc.set_pos(enc_pos0); // rollback the bit stream
        }
        else
        {
            if (digest0_ != ~0ull && enc_size > bit_model_d0_size_)
            {
                enc.set_pos(enc_pos0); // rollback the bit stream
            }
            else
            {
                compression_stat_[scode]++;
                return;
            }
        }
    }
    // coding did not result in best compression
    // use simpler method
    encode_bit_digest(block, enc, digest0_);
}



#define BM_SER_NEXT_GRP(enc, nb, B_1ZERO, B_8ZERO, B_16ZERO, B_32ZERO, B_64ZERO) \
   if (nb == 1u) \
      enc.put_8(B_1ZERO); \
   else if (nb < 256u) \
   { \
      enc.put_8(B_8ZERO); \
      enc.put_8((unsigned char)nb); \
   } \
   else if (nb < 65536u) \
   { \
      enc.put_8(B_16ZERO); \
      enc.put_16((unsigned short)nb); \
   } \
   else if (nb < bm::id_max32) \
   { \
      enc.put_8(B_32ZERO); \
      enc.put_32(unsigned(nb)); \
   } \
   else \
   {\
      enc.put_8(B_64ZERO); \
      enc.put_64(nb); \
   }


template<class BV>
void serializer<BV>::process_bookmark(block_idx_type   nb,
                                      bookmark_state&  bookm,
                                      bm::encoder&     enc) BMNOEXCEPT
{
    BM_ASSERT(bookm.nb_range_);

    block_idx_type nb_delta = nb - bookm.nb_;
    if (bookm.ptr_ && nb_delta >= bookm.nb_range_)
    {
        unsigned char* curr = enc.get_pos();
        size_t bytes_delta = size_t(curr - bookm.ptr_);
        if (bytes_delta > bookm.min_bytes_range_)
        {
            enc.set_pos(bookm.ptr_); // rewind back and save the skip
            switch (bookm.bm_type_)
            {
            case 0: // 32-bit mark
                bytes_delta -= sizeof(unsigned);
                if (bytes_delta < 0xFFFFFFFF) // range overflow check
                    enc.put_32(unsigned(bytes_delta));
                // if range is somehow off, bookmark remains 0 (NULL)
                break;
            case 1: // 24-bit mark
                bytes_delta -= (sizeof(unsigned)-1);
                if (bytes_delta < 0xFFFFFF)
                    enc.put_24(unsigned(bytes_delta));
                break;
            case 2: // 16-bit mark
                bytes_delta -= sizeof(unsigned short);
                if (bytes_delta < 0xFFFF)
                    enc.put_16((unsigned short)bytes_delta);
                break;
            default:
                BM_ASSERT(0);
                break;
            } // switch

            enc.set_pos(curr); // restore and save the sync mark

            if (nb_delta < 0xFF)
            {
                enc.put_8(set_nb_sync_mark8);
                enc.put_8((unsigned char) nb_delta);
            }
            else
            if (nb_delta < 0xFFFF)
            {
                enc.put_8(set_nb_sync_mark16);
                enc.put_16((unsigned short) nb_delta);
            }
            else
            if (nb_delta < 0xFFFFFF)
            {
                enc.put_8(set_nb_sync_mark24);
                enc.put_24(unsigned(nb_delta));
            }
            else
            if (nb_delta < ~0U)
            {
                enc.put_8(set_nb_sync_mark32);
                enc.put_32(unsigned(nb_delta));
            }
            else
            {
            #ifdef BM64ADDR
                if (nb_delta < 0xFFFFFFFFFFFFUL)
                {
                    enc.put_8(set_nb_sync_mark48);
                    enc.put_48(nb_delta);
                }
                else
                {
                    enc.put_8(set_nb_sync_mark64);
                    enc.put_64(nb_delta);
                }
            #endif
            }
            bookm.ptr_ = 0;
        }
    }

    if (!bookm.ptr_) // start new bookmark
    {
        // bookmarks use VBR to save offset
        bookm.nb_ = nb;
        bookm.ptr_ = enc.get_pos() + 1;
        switch (bookm.bm_type_)
        {
        case 0: // 32-bit mark
            enc.put_8(bm::set_nb_bookmark32);
            enc.put_32(0); // put NULL mark at first 
            break;
        case 1: // 24-bit mark
            enc.put_8(bm::set_nb_bookmark24);
            enc.put_24(0);
            break;
        case 2: // 16-bit mark
            enc.put_8(bm::set_nb_bookmark16);
            enc.put_16(0);
            break;
        default:
            BM_ASSERT(0);
            break;
        } // switch
    }
}


template<class BV>
typename serializer<BV>::size_type
serializer<BV>::serialize(const BV& bv,
                          unsigned char* buf, size_t buf_size)
{
    BM_ASSERT(temp_block_);

    if (allow_stat_reset_)
        reset_compression_stats();
    const blocks_manager_type& bman = bv.get_blocks_manager();

    bm::encoder enc(buf, buf_size);  // create the encoder
    enc_header_pos_ = 0;
    encode_header(bv, enc);

    bookmark_state  sb_bookmark(sb_range_);

    unsigned i_last = ~0u;

    block_idx_type i, j;
    for (i = 0; i < bm::set_total_blocks; ++i)
    {
        unsigned i0, j0;
        bm::get_block_coord(i, i0, j0);

        if (i0 < bman.top_block_size())
        {
            // ----------------------------------------------------
            // bookmark the stream to allow fast skip on decode
            //
            if (sb_bookmarks_)
            {
                process_bookmark(i, sb_bookmark, enc);
            }

            // ---------------------------------------------------
            // check if top level block is embarassingly sparse
            // and can be encoded as an array of unsigned
            //
            if ((compression_level_ >= 5) && (i0 != i_last))
            {
                i_last = i0;
                bool is_sparse_sub = bman.is_sparse_sblock(i0, sparse_cutoff_);
                if (is_sparse_sub)
                {
                    header_flag_ |= BM_HM_SPARSE;
                    bienc_arr_sblock(bv, i0, enc);
                    i += (bm::set_sub_array_size - j0) - 1;
                    continue;
                }
            }
        }
        else
        {
            break;
        }

        const bm::word_t* blk = bman.get_block(i0, j0);

        // ----------------------------------------------------
        // Empty or ONE block serialization
        //
        // TODO: make a function to check this in ONE pass
        //
        bool flag;
        flag = bm::check_block_zero(blk, false/*shallow check*/);
        if (flag)
        {
        zero_block:
            flag = 1;
            block_idx_type next_nb = bman.find_next_nz_block(i+1, false);
            if (next_nb == bm::set_total_blocks) // no more blocks
            {
                enc.put_8(set_block_azero);
                size_type sz = (size_type)enc.size();

                // rewind back to save header flag
                enc.set_pos(enc_header_pos_);
                enc.put_8(header_flag_);
                return sz;
            }
            block_idx_type nb = next_nb - i;
            
            if (nb > 1 && nb < 128)
            {
                // special (but frequent) case -- GAP delta fits in 7bits
                unsigned char c = (unsigned char)((1u << 7) | nb);
                enc.put_8(c);
            }
            else 
            {
                BM_SER_NEXT_GRP(enc, nb, set_block_1zero,
                                      set_block_8zero, 
                                      set_block_16zero, 
                                      set_block_32zero,
                                      set_block_64zero)
            }
            i = next_nb - 1;
            continue;
        }
        else
        {
            flag = bm::check_block_one(blk, false); // shallow scan
            if (flag)
            {
            full_block:
                flag = 1;
                // Look ahead for similar blocks
                for(j = i+1; j < bm::set_total_blocks; ++j)
                {
                    bm::get_block_coord(j, i0, j0);
                    if (!j0) // look ahead if the whole superblock is 0xFF
                    {
                        bm::word_t*** blk_root = bman.top_blocks_root();
                        if ((bm::word_t*)blk_root[i0] == FULL_BLOCK_FAKE_ADDR)
                        {
                            j += 255;
                            continue;
                        }
                    }
                    const bm::word_t* blk_next = bman.get_block(i0, j0);
                    if (flag != bm::check_block_one(blk_next, true)) // deep scan
                       break;
                } // for j

                // TODO: improve this condition, because last block is always
                // has 0 at the end, thus this never happen in practice
                if (j == bm::set_total_blocks)
                {
                    enc.put_8(set_block_aone);
                    break;
                }
                else
                {
                   block_idx_type nb = j - i;
                   BM_SER_NEXT_GRP(enc, nb, set_block_1one,
                                         set_block_8one, 
                                         set_block_16one, 
                                         set_block_32one,
                                         set_block_64one)
                }
                i = j - 1;
                continue;
            }
        }

        if (ref_vect_) // XOR filter
        {
            // Similarity model must be attached with the ref.vectors
            // for XOR filter to work
            BM_ASSERT(sim_model_);

            bool nb_indexed = sim_model_->bv_blocks.test(i);
            BM_ASSERT(nb_indexed); // Model is correctly computed from ref-vector
            if (nb_indexed)
            {
                // TODO: use rs-index or count blocks
                size_type rank = sim_model_->bv_blocks.count_range(0, i);
                BM_ASSERT(rank);
                --rank;
                const block_match_chain_type& mchain =
                    sim_model_->matr.get(ref_idx_, rank);
                BM_ASSERT(mchain.nb == i);
                switch (mchain.match)
                {
                case e_no_xor_match:
                    break;
                case e_xor_match_EQ:
                    {
                        BM_ASSERT(mchain.chain_size == 1);
                        size_type ridx = mchain.ref_idx[0];
                        size_type plain_idx = ref_vect_->get_row_idx(ridx);
                        enc.put_8(bm::set_block_ref_eq);
                        enc.put_32(unsigned(plain_idx));
                        compression_stat_[bm::set_block_ref_eq]++;
                        continue;
                    }
                    break;
                case e_xor_match_GC:
                case e_xor_match_BC:
                case e_xor_match_iBC:
                    {
                        BM_ASSERT(mchain.chain_size);
                        xor_tmp_product(blk, mchain, i0, j0);
                        // TODO: validate xor_tmp_block_
                        if (mchain.chain_size == 1)
                        {
                            size_type ridx = mchain.ref_idx[0];
                            bm::id64_t d64 = mchain.xor_d64[0];
                            size_type plain_idx = ref_vect_->get_row_idx(ridx);
                            if (d64 == ~0ull)
                            {
                                enc.put_8_16_32(unsigned(plain_idx),
                                                bm::set_block_xor_ref8_um,
                                                bm::set_block_xor_ref16_um,
                                                bm::set_block_xor_ref32_um);
                            }
                            else
                            {
                                enc.put_8_16_32(unsigned(plain_idx),
                                                bm::set_block_xor_ref8,
                                                bm::set_block_xor_ref16,
                                                bm::set_block_xor_ref32);
                                enc.put_64(d64); // xor digest mask
                            }
                            compression_stat_[bm::set_block_xor_ref32]++;
                        }
                        else // chain
                        {
                            encode_xor_match_chain(enc, mchain);
                        }
                        blk = xor_tmp_block_; // substitute with XOR product
                    }
                    break;
                default:
                    BM_ASSERT(0);
                } // switch xor_match
            }
        }

        // --------------------------------------------------
        // GAP serialization
        //
        if (BM_IS_GAP(blk))
        {
            encode_gap_block(BMGAP_PTR(blk), enc);
        }
        else // bit-block
        {
            // ----------------------------------------------
            // BIT BLOCK serialization
            //

            unsigned char model = find_bit_best_encoding(blk);
            switch (model)
            {
            case bm::set_block_bit:
                enc.put_prefixed_array_32(set_block_bit, blk, bm::set_block_size);
                break;
            case bm::set_block_bit_1bit:
            {
                unsigned bit_idx = 0;
                bm::bit_block_find(blk, bit_idx, &bit_idx);
                BM_ASSERT(bit_idx < 65536);
                enc.put_8(bm::set_block_bit_1bit); enc.put_16(bm::short_t(bit_idx));
                compression_stat_[bm::set_block_bit_1bit]++;
                continue;
            }
            break;
            case bm::set_block_azero: // empty block all of the sudden ?
                goto zero_block;
            case bm::set_block_aone:
                goto full_block;
            case bm::set_block_arrbit:
                encode_bit_array(blk, enc, false);
                break;
            case bm::set_block_arrbit_inv:
                encode_bit_array(blk, enc, true);
                break;
            case bm::set_block_gap_egamma:
                gamma_gap_bit_block(blk, enc);
                break;
            case bm::set_block_bit_0runs:
                encode_bit_interval(blk, enc, 0); // TODO: get rid of param 3 (0)
                break;
            case bm::set_block_arrgap_egamma:
                gamma_arr_bit_block(blk, enc, false);
                break;
            case bm::set_block_arrgap_egamma_inv:
                gamma_arr_bit_block(blk, enc, true);
                break;
            case bm::set_block_arrgap_bienc:
                bienc_arr_bit_block(blk, enc, false);
                break;
            case bm::set_block_arrgap_bienc_inv:
                bienc_arr_bit_block(blk, enc, true);
                break;
            case bm::set_block_arr_bienc:
                interpolated_arr_bit_block(blk, enc, false);
                break;
            case bm::set_block_arr_bienc_inv:
                interpolated_arr_bit_block(blk, enc, true); // inverted
                break;
            case bm::set_block_gap_bienc:
                interpolated_gap_bit_block(blk, enc);
                break;
            case bm::set_block_bitgap_bienc:
                bienc_gap_bit_block(blk, enc);
                break;
            case bm::set_block_bit_digest0:
                encode_bit_digest(blk, enc, digest0_);
                break;
            default:
                BM_ASSERT(0); // predictor returned an unknown model
                enc.put_prefixed_array_32(set_block_bit, blk, bm::set_block_size);
            }
        } // bit-block processing
        
        // destructive serialization mode
        //
        if (free_)
        {
            // const cast is ok, because it is a requested mode
            const_cast<blocks_manager_type&>(bman).zero_block(i);
        }
 
    } // for i

    enc.put_8(set_block_end);
    size_type sz = (size_type)enc.size();

    // rewind back to save header flag
    enc.set_pos(enc_header_pos_);
    enc.put_8(header_flag_);

    return sz;
}



/// Bit mask flags for serialization algorithm
/// \ingroup bvserial 
enum serialization_flags {
    BM_NO_BYTE_ORDER = 1,       ///< save no byte-order info (save some space)
    BM_NO_GAP_LENGTH = (1 << 1) ///< save no GAP info (save some space)
};

/*!
   \brief Saves bitvector into memory.

   Function serializes content of the bitvector into memory.
   Serialization adaptively uses compression(variation of GAP encoding) 
   when it is benefitial. 
   
   \param bv - source bvecor
   \param buf - pointer on target memory area. No range checking in the
   function. It is responsibility of programmer to allocate sufficient 
   amount of memory using information from calc_stat function.

   \param temp_block - pointer on temporary memory block. Cannot be 0; 
   If you want to save memory across multiple bvectors
   allocate temporary block using allocate_tempblock and pass it to 
   serialize.
   (Serialize does not deallocate temp_block.)

   \param serialization_flags
   Flags controlling serilization (bit-mask) 
   (use OR-ed serialization flags)

   \ingroup bvserial 

   \return Size of serialization block.
   \sa calc_stat, serialization_flags

*/
/*!
 Serialization format:
 <pre>

 | HEADER | BLOCKS |

 Header structure:
   BYTE : Serialization header (bit mask of BM_HM_*)
   BYTE : Byte order ( 0 - Big Endian, 1 - Little Endian)
   INT16: Reserved (0)
   INT16: Reserved Flags (0)

 </pre>
*/
template<class BV>
size_t serialize(const BV& bv,
                   unsigned char* buf, 
                   bm::word_t*    temp_block = 0,
                   unsigned       serialization_flags = 0)
{
    bm::serializer<BV> bv_serial(bv.get_allocator(), temp_block);
    
    if (serialization_flags & BM_NO_BYTE_ORDER) 
        bv_serial.byte_order_serialization(false);
        
    if (serialization_flags & BM_NO_GAP_LENGTH) 
        bv_serial.gap_length_serialization(false);
    else
        bv_serial.gap_length_serialization(true);

    return (size_t) bv_serial.serialize(bv, buf, 0);
}

/*!
   @brief Saves bitvector into memory.
   Allocates temporary memory block for bvector.
 
   \param bv - source bvecor
   \param buf - pointer on target memory area. No range checking in the
   function. It is responsibility of programmer to allocate sufficient 
   amount of memory using information from calc_stat function.

   \param serialization_flags
   Flags controlling serilization (bit-mask) 
   (use OR-ed serialization flags)
 
   \ingroup bvserial
*/
template<class BV>
size_t serialize(BV& bv,
                 unsigned char* buf,
                 unsigned  serialization_flags=0)
{
    return bm::serialize(bv, buf, 0, serialization_flags);
}



/*!
    @brief Bitvector deserialization from a memory BLOB

    @param bv - target bvector
    @param buf - pointer on memory which keeps serialized bvector
    @param temp_block - pointer on temporary block, 
            if NULL bvector allocates own.
    @param ref_vect - [in] optional pointer to a list of 
                      reference vectors used for
                      XOR compression.

    @return Number of bytes consumed by deserializer.

    Function deserializes bitvector from memory block containig results
    of previous serialization. Function does not remove bits 
    which are currently set. Effectively it is OR logical operation
    between the target bit-vector and serialized one.

    @sa deserialize_range

    @ingroup bvserial
*/
template<class BV>
size_t deserialize(BV& bv,
                   const unsigned char* buf,
                   bm::word_t* temp_block = 0,
                   const bm::bv_ref_vector<BV>* ref_vect = 0)
{
    ByteOrder bo_current = globals<true>::byte_order();

    bm::decoder dec(buf);
    unsigned char header_flag = dec.get_8();
    ByteOrder bo = bo_current;
    if (!(header_flag & BM_HM_NO_BO))
    {
        bo = (bm::ByteOrder) dec.get_8();
    }

    if (bo_current == bo)
    {
        deserializer<BV, bm::decoder> deserial;
        deserial.set_ref_vectors(ref_vect);
        return deserial.deserialize(bv, buf, temp_block);
    }
    switch (bo_current) 
    {
    case BigEndian:
        {
        deserializer<BV, bm::decoder_big_endian> deserial;
        deserial.set_ref_vectors(ref_vect);
        return deserial.deserialize(bv, buf, temp_block);
        }
    case LittleEndian:
        {
        deserializer<BV, bm::decoder_little_endian> deserial;
        deserial.set_ref_vectors(ref_vect);
        return deserial.deserialize(bv, buf, temp_block);
        }
    default:
        BM_ASSERT(0);
    };
    return 0;
}


/*!
    @brief Bitvector range deserialization from a memory BLOB

    @param bv - target bvector
    @param buf - pointer on memory which keeps serialized bvector
    @param from - bit-vector index to deserialize from
    @param to   - bit-vector index to deserialize to
    @param ref_vect - [in] optional pointer to a list of
                      reference vectors used for
                      XOR compression.

    Function deserializes bitvector from memory block containig results
    of previous serialization. Function does not remove bits
    which are currently set. Effectively it is OR logical operation
    between the target bit-vector and serialized one.

    @sa deserialize

    @ingroup bvserial
*/
template<class BV>
void deserialize_range(BV& bv,
                   const unsigned char* buf,
                   typename BV::size_type from,
                   typename BV::size_type to,
                   const bm::bv_ref_vector<BV>* ref_vect = 0)
{
    ByteOrder bo_current = globals<true>::byte_order();

    bm::decoder dec(buf);
    unsigned char header_flag = dec.get_8();
    ByteOrder bo = bo_current;
    if (!(header_flag & BM_HM_NO_BO))
    {
        bo = (bm::ByteOrder) dec.get_8();
    }

    if (bo_current == bo)
    {
        deserializer<BV, bm::decoder> deserial;
        deserial.set_ref_vectors(ref_vect);
        deserial.set_range(from, to);
        deserial.deserialize(bv, buf);
    }
    else
    {
        switch (bo_current)
        {
        case BigEndian:
            {
            deserializer<BV, bm::decoder_big_endian> deserial;
            deserial.set_ref_vectors(ref_vect);
            deserial.set_range(from, to);
            deserial.deserialize(bv, buf);
            }
            break;
        case LittleEndian:
            {
            deserializer<BV, bm::decoder_little_endian> deserial;
            deserial.set_ref_vectors(ref_vect);
            deserial.set_range(from, to);
            deserial.deserialize(bv, buf);
            }
            break;;
        default:
            BM_ASSERT(0);
        };
    }
    bv.keep_range(from, to);
}


template<typename DEC, typename BLOCK_IDX>
unsigned deseriaizer_base<DEC, BLOCK_IDX>::read_id_list(
                                            decoder_type&   decoder,
		    							    unsigned        block_type,
				   					        bm::gap_word_t* dst_arr)
{
	bm::gap_word_t len = 0;

    switch (block_type)
    {
    case set_block_bit_1bit:
        dst_arr[0] = decoder.get_16();
		len = 1;
		break;
    case set_block_arrgap:
    case set_block_arrgap_inv:
        len = decoder.get_16();
        decoder.get_16(dst_arr, len);
		break;
    case set_block_arrgap_egamma:
    case set_block_arrgap_egamma_inv:
        {
            bit_in_type bin(decoder);
            len = (gap_word_t)bin.gamma();
            gap_word_t prev = 0;
            for (gap_word_t k = 0; k < len; ++k)
            {
                gap_word_t bit_idx = (gap_word_t)bin.gamma();
                if (k == 0) --bit_idx; // TODO: optimization
                bit_idx = (gap_word_t)(bit_idx + prev);
                prev = bit_idx;
				dst_arr[k] = bit_idx;
            } // for
        }
        break;
    case set_block_arrgap_bienc:
    case set_block_arrgap_bienc_inv:
        {
            bm::gap_word_t min_v = decoder.get_16();
            bm::gap_word_t max_v = decoder.get_16();

            bit_in_type bin(decoder);
            len = bm::gap_word_t(bin.gamma() + 4);
            dst_arr[0] = min_v;
            dst_arr[len-1] = max_v;
            bin.bic_decode_u16(&dst_arr[1], len-2, min_v, max_v);
        }
        break;
    case set_block_arrgap_bienc_v2:
    case set_block_arrgap_bienc_inv_v2:
        {
            bm::gap_word_t min_v, max_v;
            len = decoder.get_16();
            if (len & 1)
                min_v = decoder.get_8();
            else
                min_v = decoder.get_16();

            if (len & (1<<1))
                max_v = decoder.get_8();
            else
                max_v = decoder.get_16();
            max_v = bm::gap_word_t(min_v + max_v);

            len = bm::gap_word_t(len >> 2);

            bit_in_type bin(decoder);
            dst_arr[0] = min_v;
            dst_arr[len-1] = max_v;
            bin.bic_decode_u16(&dst_arr[1], len-2, min_v, max_v);
        }
        break;

    default: // TODO: get rid of exception here to classify as "noexept"
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    }
	return len;
}

template<typename DEC, typename BLOCK_IDX>
void
deseriaizer_base<DEC, BLOCK_IDX>::read_bic_arr(decoder_type& dec,
    bm::word_t*   blk,
    unsigned block_type) BMNOEXCEPT
{
    BM_ASSERT(!BM_IS_GAP(blk));
    bm::gap_word_t min_v, max_v, max_delta, arr_len;

    switch (block_type)
    {
    case bm::set_block_arr_bienc:
        min_v = dec.get_16();
        max_v = dec.get_16();
        break;
    case bm::set_block_arr_bienc_8bh:
        min_v = dec.get_8();
        max_delta = dec.get_8();
        max_v = bm::gap_word_t(65536 - max_delta);
        break;
    default:
        BM_ASSERT(0);
        return;
    }

    arr_len = dec.get_16();
    
    bit_in_type bin(dec);

    if (!IS_VALID_ADDR(blk))
    {
        bin.bic_decode_u16_dry(arr_len-2, min_v, max_v);
        return;
    }
    bm::set_bit(blk, min_v);
    bm::set_bit(blk, max_v);
    bin.bic_decode_u16_bitset(blk, arr_len-2, min_v, max_v);
}

template<typename DEC, typename BLOCK_IDX>
unsigned deseriaizer_base<DEC, BLOCK_IDX>::read_bic_sb_arr(
                         decoder_type&   dec,
                         unsigned  block_type,
                         unsigned*  dst_arr,
                         unsigned*  sb_idx)
{
	unsigned len(0), sb_flag(0);

    bit_in_type bin(dec);

    switch (block_type)
    {
    case bm::set_sblock_bienc:
        {
            sb_flag = dec.get_8();

            if (sb_flag & bm::sblock_flag_sb32)
                *sb_idx = dec.get_32();
            else if (sb_flag & bm::sblock_flag_sb16)
                *sb_idx = dec.get_16();
            else
                *sb_idx = dec.get_8();

            if (sb_flag & bm::sblock_flag_len16)
                len = dec.get_16();
            else
                len = dec.get_8();

            bm::word_t min_v;
            if (sb_flag & bm::sblock_flag_min24)
                if (sb_flag & bm::sblock_flag_min16) // 24 and 16
                    min_v = dec.get_32();
                else
                    min_v = dec.get_24(); // 24 but not 16
            else if (sb_flag & bm::sblock_flag_min16)
                min_v = dec.get_16();
            else
                min_v = dec.get_8();
            
            bm::word_t max_v;// = dec.get_32();
            if (sb_flag & bm::sblock_flag_max24)
                if (sb_flag & bm::sblock_flag_max16)
                    max_v = dec.get_32(); // 24 and 16
                else
                    max_v = dec.get_24(); // 24 but not 16
            else if (sb_flag & bm::sblock_flag_max16)
                max_v = dec.get_16();
            else
                max_v = dec.get_8();
            max_v = bm::set_sub_total_bits - max_v;

            dst_arr[0] = min_v;
            dst_arr[len-1] = max_v;
            bin.bic_decode_u32_cm(&dst_arr[1], len-2, min_v, max_v);
        }
        break;
    default: // TODO: get rid of exception here to classify as "noexept"
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
    return len;
}


template<typename DEC, typename BLOCK_IDX>
void
deseriaizer_base<DEC, BLOCK_IDX>::read_bic_arr_inv(decoder_type&   decoder,
                                                   bm::word_t* blk) BMNOEXCEPT
{
    // TODO: optimization
    bm::bit_block_set(blk, 0);
    this->read_bic_arr(decoder, blk, bm::set_block_arr_bienc);
    bm::bit_invert(blk);
}

template<typename DEC, typename BLOCK_IDX>
void deseriaizer_base<DEC, BLOCK_IDX>::read_bic_gap(decoder_type& dec,
                                                    bm::word_t*   blk) BMNOEXCEPT
{
    BM_ASSERT(!BM_IS_GAP(blk));
    
    bm::gap_word_t head = dec.get_8();
    unsigned arr_len = dec.get_16();
    bm::gap_word_t min_v = dec.get_16();
    
    
    id_array_[0] = head;
    id_array_[1] = min_v;
    id_array_[arr_len] = 65535;
    
    bit_in_type bin(dec);
    bin.bic_decode_u16(&id_array_[2], arr_len-2, min_v, 65535);

    if (!IS_VALID_ADDR(blk))
        return;
    bm::gap_add_to_bitset(blk, id_array_, arr_len);
}

template<typename DEC, typename BLOCK_IDX>
void deseriaizer_base<DEC, BLOCK_IDX>::read_digest0_block(
                                                decoder_type& dec,
                                                bm::word_t*   block) BMNOEXCEPT
{
    bm::id64_t d0 = dec.get_64();
    while (d0)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d0); // d & -d;
        
        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        unsigned j = 0;
        if (!IS_VALID_ADDR(block))
        {
            do
            {
                dec.get_32();
                dec.get_32();
                dec.get_32();
                dec.get_32();
                j += 4;
            } while (j < bm::set_block_digest_wave_size);
        }
        else
        {
            do
            {
                block[off+j+0] |= dec.get_32();
                block[off+j+1] |= dec.get_32();
                block[off+j+2] |= dec.get_32();
                block[off+j+3] |= dec.get_32();
                j += 4;
            } while (j < bm::set_block_digest_wave_size);
        }
        
        d0 = bm::bmi_bslr_u64(d0); // d &= d - 1;
    } // while
}

template<typename DEC, typename BLOCK_IDX>
void deseriaizer_base<DEC, BLOCK_IDX>::read_0runs_block(
                                            decoder_type& dec,
                                            bm::word_t* blk) BMNOEXCEPT
{
    //TODO: optimization if block exists and it is OR-ed read
    bm::bit_block_set(blk, 0);

    unsigned char run_type = dec.get_8();
    for (unsigned j = 0; j < bm::set_block_size; run_type = !run_type)
    {
        unsigned run_length = dec.get_16();
        if (run_type)
        {
            unsigned run_end = j + run_length;
            BM_ASSERT(run_end <= bm::set_block_size);
            for (;j < run_end; ++j)
            {
                unsigned w = dec.get_32();
                blk[j] = w;
            }
        }
        else
        {
            j += run_length;
        }
    } // for j
}


template<typename DEC, typename BLOCK_IDX>
void
deseriaizer_base<DEC, BLOCK_IDX>::read_gap_block(decoder_type&   decoder,
                                           unsigned        block_type, 
                                           bm::gap_word_t* dst_block,
                                           bm::gap_word_t& gap_head)
{
//    typedef bit_in<DEC> bit_in_type;
    switch (block_type)
    {
    case set_block_gap:
        {
            unsigned len = gap_length(&gap_head);
            --len;
            *dst_block = gap_head;
            decoder.get_16(dst_block+1, len - 1);
            dst_block[len] = gap_max_bits - 1;
        }
        break;
    case set_block_bit_1bit:
        {
			gap_set_all(dst_block, bm::gap_max_bits, 0);
            gap_word_t bit_idx = decoder.get_16();
			gap_add_value(dst_block, bit_idx);
        }
        break;
    case set_block_arrgap:
    case set_block_arrgap_inv:
        {
            gap_set_all(dst_block, bm::gap_max_bits, 0);
            gap_word_t len = decoder.get_16();
            for (gap_word_t k = 0; k < len; ++k)
            {
                gap_word_t bit_idx = decoder.get_16();
				bm::gap_add_value(dst_block, bit_idx);
            } // for
        }
        break;
    case set_block_arrgap_egamma_inv:
    case set_block_arrgap_bienc_inv:
    case set_block_arrgap_bienc_inv_v2:
    case set_block_arrgap_egamma:
    case set_block_arrgap_bienc:
    case set_block_arrgap_bienc_v2:
        {
        	unsigned arr_len = read_id_list(decoder, block_type, id_array_);
            dst_block[0] = 0;
            unsigned gap_len =
                gap_set_array(dst_block, id_array_, arr_len);
            BM_ASSERT(gap_len == bm::gap_length(dst_block));
            (void)(gap_len);
        }
        break;
    case set_block_gap_egamma:
        {
            unsigned len = (gap_head >> 3);
            --len;
            // read gamma GAP block into a dest block
            *dst_block = gap_head;
            gap_word_t* gap_data_ptr = dst_block + 1;

            bit_in_type bin(decoder);
            gap_word_t v = (gap_word_t)bin.gamma();
            gap_word_t gap_sum = *gap_data_ptr = (gap_word_t)(v - 1);
            for (unsigned i = 1; i < len; ++i)
            {
                v = (gap_word_t)bin.gamma();
                gap_sum = (gap_word_t)(gap_sum + v);
                *(++gap_data_ptr) = gap_sum;
            }
            dst_block[len+1] = bm::gap_max_bits - 1;
        }
        break;
    case set_block_gap_bienc:
        {
            unsigned len = (gap_head >> 3);
            *dst_block = gap_head;
            bm::gap_word_t min_v = decoder.get_16();
            dst_block[1] = min_v;
            bit_in_type bin(decoder);
            bin.bic_decode_u16(&dst_block[2], len-2, min_v, 65535);
            dst_block[len] = bm::gap_max_bits - 1;
        }
        break;
    case set_block_gap_bienc_v2:
        {
            bm::gap_word_t min_v, max_v;
            unsigned len = (gap_head >> 3);
            bm::gap_word_t min8 = gap_head & (1 << 1);
            bm::gap_word_t tail8 = gap_head & (1 << 2);
            gap_head &= bm::gap_word_t(~(3 << 1)); // clear the flags
            if (min8)
                min_v = decoder.get_8();
            else
                min_v = decoder.get_16();
            if (tail8)
                max_v = decoder.get_8();
            else
                max_v = decoder.get_16();
            max_v = bm::gap_word_t(65535 - max_v); // tail correction

            dst_block[0] = gap_head;
            dst_block[1] = min_v;
            bit_in_type bin(decoder);
            bin.bic_decode_u16(&dst_block[2], len-3, min_v, max_v);
            dst_block[len-1] = max_v;
            dst_block[len] = bm::gap_max_bits - 1;
        }
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    }

    if (block_type == set_block_arrgap_egamma_inv || 
        block_type == set_block_arrgap_inv ||
        block_type == set_block_arrgap_bienc_inv ||
        block_type == set_block_arrgap_bienc_inv_v2)
    {
        gap_invert(dst_block);
    }
}

template<typename DEC, typename BLOCK_IDX>
typename deseriaizer_base<DEC, BLOCK_IDX>::block_idx_type
deseriaizer_base<DEC, BLOCK_IDX>::try_skip(
                                        decoder_type&   decoder,
                                        block_idx_type nb,
                                        block_idx_type expect_nb) BMNOEXCEPT
{
    if (skip_offset_) // skip bookmark is available
    {
        const unsigned char* save_pos = decoder.get_pos(); // save position

        if (save_pos > skip_pos_) // checkpoint passed
        {
            skip_offset_ = 0;
            return false;
        }
        decoder.set_pos(skip_pos_);

        unsigned char sync_mark = decoder.get_8();
        block_idx_type nb_sync;
        switch (sync_mark)
        {
        case set_nb_sync_mark8:
            nb_sync = decoder.get_8();
            break;
        case set_nb_sync_mark16:
            nb_sync = decoder.get_16();
            break;
        case set_nb_sync_mark24:
            nb_sync = decoder.get_24();
            break;
        case set_nb_sync_mark32:
            nb_sync = decoder.get_32();
            break;
        case set_nb_sync_mark48:
            nb_sync = block_idx_type(decoder.get_48());
            #ifndef BM64ADDR
                BM_ASSERT(0);
                decoder.set_pos(save_pos);
                skip_offset_ = 0;
                return 0; // invalid bookmark from 64-bit serialization
            #endif
            break;
        case set_nb_sync_mark64:
            nb_sync = block_idx_type(decoder.get_64());
            #ifndef BM64ADDR
                BM_ASSERT(0);
                decoder.set_pos(save_pos);
                skip_offset_ = 0;
                return 0; // invalid bookmark from 64-bit serialization
            #endif
            break;
        default:
            BM_ASSERT(0);
            decoder.set_pos(save_pos);
            skip_offset_ = 0;
            return 0;
        } // switch

        nb_sync += nb;
        if (nb_sync <= expect_nb) // within reach
        {
            skip_offset_ = 0;
            return nb_sync;
        }
        skip_offset_ = 0;
        decoder.set_pos(save_pos);
    }
    return 0;
}


// -------------------------------------------------------------------------

template<class BV, class DEC>
deserializer<BV, DEC>::deserializer()
: ref_vect_(0),
  xor_block_(0),
  or_block_(0),
  or_block_idx_(0),
  is_range_set_(0)
{
    temp_block_ = alloc_.alloc_bit_block();

    bit_idx_arr_.resize(bm::gap_max_bits);
    this->id_array_ = bit_idx_arr_.data();

    sb_bit_idx_arr_.resize(bm::gap_max_bits);
    this->sb_id_array_ = sb_bit_idx_arr_.data();

    gap_temp_block_.resize(bm::gap_max_bits);

    alloc_.set_pool(&pool_);
}

template<class BV, class DEC>
deserializer<BV, DEC>::~deserializer()
{
     alloc_.free_bit_block(temp_block_);
     if (xor_block_)
        alloc_.free_bit_block(xor_block_, 2);
    BM_ASSERT(!or_block_);
}

template<class BV, class DEC>
void deserializer<BV, DEC>::set_ref_vectors(const bv_ref_vector_type* ref_vect)
{
    ref_vect_ = ref_vect;
    if (ref_vect_ && !xor_block_)
        xor_block_ = alloc_.alloc_bit_block(2);
}

template<class BV, class DEC>
void 
deserializer<BV, DEC>::deserialize_gap(unsigned char btype, decoder_type& dec, 
                                       bvector_type&  bv, blocks_manager_type& bman,
                                       block_idx_type nb,
                                       bm::word_t* blk)
{
    gap_word_t gap_head; 
    bm::gap_word_t* gap_temp_block = gap_temp_block_.data();

    bool inv_flag = false;
    unsigned i0, j0;
    bm::get_block_coord(nb, i0, j0);
    bman.reserve_top_blocks(i0+1);
    bman.check_alloc_top_subblock(i0);

    switch (btype)
    {
    case set_block_gap: 
        BM_FALLTHROUGH;
    case set_block_gapbit:
    {
        gap_head = (gap_word_t)
            (sizeof(gap_word_t) == 2 ? dec.get_16() : dec.get_32());

        unsigned len = gap_length(&gap_head);
        int level = gap_calc_level(len, bman.glen());
        --len;
        if (level == -1)  // Too big to be GAP: convert to BIT block
        {
            *gap_temp_block = gap_head;
            dec.get_16(gap_temp_block+1, len - 1);
            gap_temp_block[len] = gap_max_bits - 1;

            if (blk == 0)  // block does not exist yet
            {
                blk = bman.get_allocator().alloc_bit_block();
                bman.set_block(nb, blk);
                gap_convert_to_bitset(blk, gap_temp_block);
            }
            else  // We have some data already here. Apply OR operation.
            {
                gap_convert_to_bitset(temp_block_, 
                                      gap_temp_block);
                bv.combine_operation_block_or(i0, j0, blk, temp_block_);
            }
            return;
        } // level == -1

        set_gap_level(&gap_head, level);

        if (blk == 0)
        {
            BM_ASSERT(level >= 0);
            gap_word_t* gap_blk = 
              bman.get_allocator().alloc_gap_block(unsigned(level), bman.glen());
            gap_word_t* gap_blk_ptr = BMGAP_PTR(gap_blk);
            *gap_blk_ptr = gap_head;
            bm::set_gap_level(gap_blk_ptr, level);
            blk = bman.set_block(nb, (bm::word_t*)BMPTR_SETBIT0(gap_blk));
            BM_ASSERT(blk == 0);
            
            if (len > 1)
                dec.get_16(gap_blk + 1, len - 1);
            gap_blk[len] = bm::gap_max_bits - 1;
        }
        else // target block exists
        {
            // read GAP block into a temp memory and perform OR
            *gap_temp_block = gap_head;
            dec.get_16(gap_temp_block + 1, len - 1);
            gap_temp_block[len] = bm::gap_max_bits - 1;
            break;
        }
        return;
    }
    case set_block_arrgap_bienc_inv_v2:
        inv_flag = true;
        BM_FALLTHROUGH;
    case set_block_arrgap: 
        BM_FALLTHROUGH;
    case set_block_arrgap_egamma:
        BM_FALLTHROUGH;
    case set_block_arrgap_bienc:
        BM_FALLTHROUGH;
    case set_block_arrgap_bienc_v2:
        {
        	unsigned arr_len = this->read_id_list(dec, btype, this->id_array_);
            gap_temp_block[0] = 0; // reset unused bits in gap header
            unsigned gap_len =
              bm::gap_set_array(gap_temp_block, this->id_array_, arr_len);
            if (inv_flag)
                bm::gap_invert(gap_temp_block);
            
            BM_ASSERT(gap_len == bm::gap_length(gap_temp_block));
            int level = bm::gap_calc_level(gap_len, bman.glen());
            if (level == -1)  // Too big to be GAP: convert to BIT block
            {
                bm::gap_convert_to_bitset(temp_block_, gap_temp_block);
                bv.combine_operation_block_or(i0, j0, blk, temp_block_);
                return;
            }
            break;
        }
    case set_block_gap_egamma:            
        gap_head = dec.get_16();
        BM_FALLTHROUGH;
    case set_block_arrgap_egamma_inv:
        BM_FALLTHROUGH;
    case set_block_arrgap_inv:
        BM_FALLTHROUGH;
    case set_block_arrgap_bienc_inv:
        this->read_gap_block(dec, btype, gap_temp_block, gap_head);
        break;
    case bm::set_block_gap_bienc:
        BM_FALLTHROUGH;
    case bm::set_block_gap_bienc_v2:
        gap_head = dec.get_16();
        this->read_gap_block(dec, btype, gap_temp_block, gap_head);
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    }

    if (x_ref_d64_) // this is a delayed bit-XOR to be played here
    {   // deoptimize the operation it will turn to bit block anyways
        if (!blk)
        {
            blk = bman.get_allocator().alloc_bit_block();
            bm::gap_convert_to_bitset(blk, gap_temp_block);
            bman.set_block_ptr(i0, j0, blk);
        }
        else
        {
            bm::gap_convert_to_bitset(temp_block_, gap_temp_block);
            bv.combine_operation_block_or(i0, j0, blk, temp_block_);
        }
    }
    else
    {
        bm::word_t* tmp_blk = (bm::word_t*)gap_temp_block;
        BMSET_PTRGAP(tmp_blk);
        bv.combine_operation_block_or(i0, j0, blk, tmp_blk);
    }
}

template<class BV, class DEC>
void deserializer<BV, DEC>::decode_bit_block(unsigned char btype,
                              decoder_type&        dec,
                              blocks_manager_type& bman,
                              block_idx_type       nb,
                              bm::word_t*          blk)
{
    if (!blk)
    {
        blk = bman.get_allocator().alloc_bit_block();
        bman.set_block(nb, blk);
        bm::bit_block_set(blk, 0);
    }
    else
    if (BM_IS_GAP(blk))
        blk = bman.deoptimize_block(nb);
    
    BM_ASSERT(blk != temp_block_);
    
    switch (btype)
    {
    case set_block_arrbit_inv:
        if (IS_FULL_BLOCK(blk))
            blk = bman.deoptimize_block(nb);
        bm::bit_block_set(temp_block_, ~0u);
        {
            gap_word_t len = dec.get_16();
            for (unsigned k = 0; k < len; ++k)
            {
                gap_word_t bit_idx = dec.get_16();
                bm::clear_bit(temp_block_, bit_idx);
            } // for
        }
        bm::bit_block_or(blk, temp_block_);
        break;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        this->read_bic_arr(dec, blk, btype);
        break;
    case bm::set_block_arr_bienc_inv:
        BM_ASSERT(blk != temp_block_);
        if (IS_FULL_BLOCK(blk))
            blk = bman.deoptimize_block(nb);
        // TODO: optimization
        bm::bit_block_set(temp_block_, 0);
        this->read_bic_arr(dec, temp_block_, bm::set_block_arr_bienc);
        bm::bit_invert(temp_block_);
        bm::bit_block_or(blk, temp_block_);
        break;
    case bm::set_block_bitgap_bienc:
        this->read_bic_gap(dec, blk);
        break;
    case bm::set_block_bit_digest0:
        this->read_digest0_block(dec, blk);
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
}

template<class BV, class DEC>
void deserializer<BV, DEC>::decode_arr_sblock(unsigned char  btype,
                                              decoder_type&  dec,
                                              bvector_type&  bv)
{
    unsigned sb;
    unsigned* arr = this->sb_id_array_;
    unsigned len = this->read_bic_sb_arr(dec, btype, arr, &sb);
    const typename BV::size_type sb_max_bc = bm::set_sub_array_size * bm::gap_max_bits;
    typename BV::size_type from = sb * sb_max_bc;

    if (is_range_set_)
    {
        for (typename BV::size_type i = 0; i < len; ++i)
        {
            typename BV::size_type idx = from + arr[i];
            if (idx > idx_to_)
                break;
            if (idx < idx_from_)
                continue;
            bv.set_bit_no_check(idx);
        } // for
    }
    else // range restriction is not set
    {
        for (typename BV::size_type i = 0; i < len; ++i)
        {
            typename BV::size_type idx = from + arr[i];
            bv.set_bit_no_check(idx);
        } // for
    }
}


template<class BV, class DEC>
void deserializer<BV, DEC>::decode_block_bit(decoder_type& dec,
                                             bvector_type&  bv,
                                             block_idx_type nb,
                                             bm::word_t* blk)
{
    if (!blk)
    {
        blocks_manager_type& bman = bv.get_blocks_manager();
        blk = bman.get_allocator().alloc_bit_block();
        bman.set_block(nb, blk);
        dec.get_32(blk, bm::set_block_size);
    }
    else
    {
        dec.get_32(temp_block_, bm::set_block_size);
        bv.combine_operation_with_block(nb, temp_block_, 0, BM_OR);
    }
}

template<class BV, class DEC>
void deserializer<BV, DEC>::decode_block_bit_interval(decoder_type& dec,
                                                      bvector_type&  bv,
                                                      block_idx_type nb,
                                                      bm::word_t* blk)
{
    unsigned head_idx = dec.get_16();
    unsigned tail_idx = dec.get_16();
    if (!blk)
    {
        blocks_manager_type& bman = bv.get_blocks_manager();
        blk = bman.get_allocator().alloc_bit_block();
        bman.set_block(nb, blk);
        for (unsigned k = 0; k < head_idx; ++k)
            blk[k] = 0;
        dec.get_32(blk + head_idx, tail_idx - head_idx + 1);
        for (unsigned j = tail_idx + 1; j < bm::set_block_size; ++j)
            blk[j] = 0;
    }
    else
    {
        bm::bit_block_set(temp_block_, 0);
        dec.get_32(temp_block_ + head_idx, tail_idx - head_idx + 1);
        bv.combine_operation_with_block(nb, temp_block_, 0, BM_OR);
    }
}

template<class BV, class DEC>
void deserializer<BV, DEC>::decode_arrbit(decoder_type& dec,
                                          bvector_type&  bv,
                                          block_idx_type nb,
                                          bm::word_t* blk)
{
    blocks_manager_type& bman = bv.get_blocks_manager();
    bm::gap_word_t len = dec.get_16();
    if (BM_IS_GAP(blk))
    {
        blk = bman.deoptimize_block(nb);
    }
    else
    {
        if (!blk)  // block does not exists yet
        {
            blk = bman.get_allocator().alloc_bit_block();
            bm::bit_block_set(blk, 0);
            bman.set_block(nb, blk);
        }
        else
            if (IS_FULL_BLOCK(blk)) // nothing to do
            {
                for (unsigned k = 0; k < len; ++k) // dry read
                    dec.get_16();
                return;
            }
    }
    // Get the array one by one and set the bits.
    for (unsigned k = 0; k < len; ++k)
    {
        gap_word_t bit_idx = dec.get_16();
        bm::set_bit(blk, bit_idx);
    }
}



template<class BV, class DEC>
size_t deserializer<BV, DEC>::deserialize(bvector_type&        bv,
                                          const unsigned char* buf,
                                          bm::word_t*          /*temp_block*/)
{
    blocks_manager_type& bman = bv.get_blocks_manager();
    if (!bman.is_init())
        bman.init_tree();
    
    bm::word_t* temp_block = temp_block_;

    bm::strategy  strat = bv.get_new_blocks_strat();
    bv.set_new_blocks_strat(BM_GAP);
    typename bvector_type::mem_pool_guard mp_guard_bv;
    mp_guard_bv.assign_if_not_set(pool_, bv);


    decoder_type dec(buf);

    // Reading th serialization header
    //
    unsigned char header_flag =  dec.get_8();
    if (!(header_flag & BM_HM_NO_BO))
    {
        /*ByteOrder bo = (bm::ByteOrder)*/dec.get_8();
    }
    if (header_flag & BM_HM_64_BIT)
    {
    #ifndef BM64ADDR
        BM_ASSERT(0); // 64-bit address vector cannot read on 32
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    #endif
    }

    if (header_flag & BM_HM_ID_LIST)
    {
        // special case: the next comes plain list of integers
        if (header_flag & BM_HM_RESIZE)
        {
            block_idx_type bv_size;
            if (header_flag & BM_HM_64_BIT)
            {
                BM_ASSERT(sizeof(block_idx_type)==8);
                bv_size = (block_idx_type)dec.get_64();
            }
            else
                bv_size = dec.get_32();
            if (bv_size > bv.size())
                bv.resize(bv_size);
        }
        for (unsigned cnt = dec.get_32(); cnt; --cnt)
        {
            bm::id_t idx = dec.get_32();
            bv.set(idx);
        } // for
        // -1 for compatibility with other deserialization branches
        return dec.size()-1;
    }

    if (!(header_flag & BM_HM_NO_GAPL))
    {
        for (unsigned k = 0; k < bm::gap_levels; ++k) // read GAP levels
        {
            /*glevels[i] =*/ dec.get_16();
        }
    }
    if (header_flag & BM_HM_RESIZE)
    {
        block_idx_type bv_size;
        if (header_flag & BM_HM_64_BIT)
        {
            // 64-bit vector cannot be deserialized into 32-bit
            BM_ASSERT(sizeof(block_idx_type)==8);
            bv_size = (block_idx_type)dec.get_64();
            #ifndef BM64ADDR
                #ifndef BM_NO_STL
                    throw std::logic_error(this->err_msg());
                #else
                    BM_THROW(BM_ERR_SERIALFORMAT);
                #endif
            #endif
        }
        else
            bv_size = dec.get_32();
        if (bv_size > bv.size())
            bv.resize(bv_size);
    }

    if (header_flag & BM_HM_HXOR) // XOR compression mode
    {
        if (!xor_block_)
            xor_block_ = alloc_.alloc_bit_block();
    }

    bv.set_new_blocks_strat(bm::BM_GAP); // minimize deserialization footprint

    size_type full_blocks = 0;

    // reference XOR compression FSM fields
    //
    xor_reset();

    unsigned row_idx(0);

    block_idx_type nb_sync;

    unsigned char btype;
    block_idx_type nb;
    unsigned i0, j0;

    block_idx_type nb_i = 0;
    do
    {
        if (is_range_set_)
        {
            block_idx_type nb_to = (idx_to_ >> bm::set_block_shift);
            if (nb_i > nb_to)
                break; // early exit (out of target range)
        }

        btype = dec.get_8();
        if (btype & (1 << 7)) // check if short zero-run packed here
        {
            nb = btype & ~(1 << 7);
            BM_ASSERT(nb);
            nb_i += nb;
            continue;
        }
        bm::get_block_coord(nb_i, i0, j0);
        bm::word_t* blk = bman.get_block_ptr(i0, j0);

        switch (btype)
        {
        case set_block_azero: 
        case set_block_end:
            nb_i = bm::set_total_blocks;
            break;
        case set_block_1zero:
            break;
        case set_block_8zero:
            nb = dec.get_8();
            BM_ASSERT(nb);
            nb_i += nb;
            continue; // bypass ++nb_i;
        case set_block_16zero:
            nb = dec.get_16();
            BM_ASSERT(nb);
            nb_i += nb;
            continue; // bypass ++nb_i;
        case set_block_32zero:
            nb = dec.get_32();
            BM_ASSERT(nb);
            nb_i += nb;
            continue; // bypass ++nb_i;
        case set_block_64zero:
            #ifdef BM64ADDR
                nb = dec.get_64();
                BM_ASSERT(nb);
                nb_i += nb;
                continue; // bypass ++nb_i;
            #else
                BM_ASSERT(0); // attempt to read 64-bit vector in 32-bit mode
                #ifndef BM_NO_STL
                    throw std::logic_error(this->err_msg());
                #else
                    BM_THROW(BM_ERR_SERIALFORMAT);
                #endif
            #endif
            break;
        case set_block_aone:
            bman.set_all_set(nb_i, bm::set_total_blocks-1);
            nb_i = bm::set_total_blocks;
            break;
        case set_block_1one:
            bman.set_block_all_set(nb_i);
            break;
        case set_block_8one:
            full_blocks = dec.get_8();
            goto process_full_blocks;
            break;
        case set_block_16one:
            full_blocks = dec.get_16();
            goto process_full_blocks;
            break;
        case set_block_32one:
            full_blocks = dec.get_32();
            goto process_full_blocks;
            break;
        case set_block_64one:
    #ifdef BM64ADDR
            full_blocks = dec.get_64();
            goto process_full_blocks;
    #else
            BM_ASSERT(0); // 32-bit vector cannot read 64-bit
            dec.get_64();
            #ifndef BM_NO_STL
                throw std::logic_error(this->err_msg());
            #else
                BM_THROW(BM_ERR_SERIALFORMAT);
            #endif
    #endif
            process_full_blocks:
            {
                BM_ASSERT(full_blocks);
                size_type from = nb_i * bm::gap_max_bits;
                size_type to = from + full_blocks * bm::gap_max_bits;
                bv.set_range(from, to-1);
                nb_i += full_blocks-1;
            }
            break;
        case set_block_bit:
            decode_block_bit(dec, bv, nb_i, blk);
            break;
        case set_block_bit_1bit:
        {
            size_type bit_idx = dec.get_16();
            bit_idx += nb_i * bm::bits_in_block;
            bv.set_bit_no_check(bit_idx);
            break;
        }
        case set_block_bit_0runs:
        {
            //TODO: optimization if block exists
            this->read_0runs_block(dec, temp_block);
            bv.combine_operation_with_block(nb_i, temp_block, 0, BM_OR);
            break;
        }
        case set_block_bit_interval: 
            decode_block_bit_interval(dec, bv, nb_i, blk);
            break;
        case set_block_gap:
        case set_block_gapbit:
        case set_block_arrgap:
        case set_block_gap_egamma:
        case set_block_arrgap_egamma:
        case set_block_arrgap_egamma_inv:
        case set_block_arrgap_inv:
        case set_block_gap_bienc:
        case set_block_gap_bienc_v2:
        case set_block_arrgap_bienc:
        case set_block_arrgap_bienc_inv:
        case set_block_arrgap_bienc_v2:
        case set_block_arrgap_bienc_inv_v2:
            deserialize_gap(btype, dec, bv, bman, nb_i, blk);
            break;
        case set_block_arrbit:
            decode_arrbit(dec, bv, nb_i, blk);
            break;
        case bm::set_block_arr_bienc:
        case bm::set_block_arrbit_inv:
        case bm::set_block_arr_bienc_inv:
        case bm::set_block_bitgap_bienc:
        case bm::set_block_arr_bienc_8bh:
            decode_bit_block(btype, dec, bman, nb_i, blk);
            break;
        case bm::set_block_bit_digest0:
            decode_bit_block(btype, dec, bman, nb_i, blk);
            break;

        // --------------------------------------- super-block encodings
        case bm::set_sblock_bienc:
            decode_arr_sblock(btype, dec, bv);
            nb_i += (bm::set_sub_array_size - j0);
            continue; // bypass ++i;

        // --------------------------------------- bookmarks and skip jumps
        //
        case set_nb_bookmark32:
            this->bookmark_idx_ = nb_i;
            this->skip_offset_ = dec.get_32();
            goto process_bookmark;
        case set_nb_bookmark24:
            this->bookmark_idx_ = nb_i;
            this->skip_offset_ = dec.get_24();
            goto process_bookmark;
        case set_nb_bookmark16:
            this->bookmark_idx_ = nb_i;
            this->skip_offset_ = dec.get_16();
        process_bookmark:
            if (is_range_set_)
            {
                this->skip_pos_ = dec.get_pos() + this->skip_offset_;
                block_idx_type nb_from = (idx_from_ >> bm::set_block_shift);
                nb_from = this->try_skip(dec, nb_i, nb_from);
                if (nb_from)
                    nb_i = nb_from;
            }
            continue; // bypass ++i;

        case set_nb_sync_mark8:
            nb_sync = dec.get_8();
            goto process_nb_sync;
        case set_nb_sync_mark16:
            nb_sync = dec.get_16();
            goto process_nb_sync;
        case set_nb_sync_mark24:
            nb_sync = dec.get_24();
            goto process_nb_sync;
        case set_nb_sync_mark32:
            nb_sync = dec.get_32();
            goto process_nb_sync;
        case set_nb_sync_mark48:
            nb_sync = block_idx_type(dec.get_48());
            goto process_nb_sync;
        case set_nb_sync_mark64:
            nb_sync = block_idx_type(dec.get_64());
        process_nb_sync:                
                BM_ASSERT(nb_i == this->bookmark_idx_ + nb_sync);
                if (nb_i != this->bookmark_idx_ + nb_sync)
                {
                    #ifndef BM_NO_STL
                        throw std::logic_error(this->err_msg());
                    #else
                        BM_THROW(BM_ERR_SERIALFORMAT);
                    #endif
                }
                
            continue; // bypass ++i;

        // ------------------------------------  XOR compression markers
        case bm::set_block_ref_eq:
            {
                BM_ASSERT(ref_vect_); // reference vector has to be attached
                if (x_ref_d64_) // previous delayed XOR post proc.
                    xor_decode(bman);

                row_idx = dec.get_32();
                size_type idx = ref_vect_->find(row_idx);
                if (idx == ref_vect_->not_found())
                {
                    BM_ASSERT(0);
                    goto throw_err;
                }
                const bvector_type* ref_bv = ref_vect_->get_bv(idx);
                BM_ASSERT(ref_bv); // some incorrect work with the ref.vector
                BM_ASSERT(ref_bv != &bv);
                const blocks_manager_type& ref_bman = ref_bv->get_blocks_manager();
                const bm::word_t* ref_blk = ref_bman.get_block_ptr(i0, j0);
                if (ref_blk)
                    bv.combine_operation_with_block(nb_i, ref_blk,
                                                    BM_IS_GAP(ref_blk), BM_OR);
                break;
            }
        case bm::set_block_xor_ref8:
        case bm::set_block_xor_ref8_um:
                if (x_ref_d64_) // previous delayed XOR post proc.
                    xor_decode(bman);
                row_idx = dec.get_8();
                goto process_xor;
        case bm::set_block_xor_ref16:
        case bm::set_block_xor_ref16_um:
                if (x_ref_d64_) // previous delayed XOR post proc.
                    xor_decode(bman);
                row_idx = dec.get_16();
                goto process_xor;
        case bm::set_block_xor_ref32:
        case bm::set_block_xor_ref32_um:
            {
                if (x_ref_d64_) // previous delayed XOR post proc.
                    xor_decode(bman);
                row_idx = dec.get_32();
            process_xor:
                if (btype <= bm::set_block_xor_ref32)
                    x_ref_d64_ = dec.get_64();
                else
                    x_ref_d64_ = ~0ull;
            process_xor_ref:
                BM_ASSERT(ref_vect_); // reference vector has to be attached
                x_ref_idx_ = ref_vect_->find(row_idx);
                x_nb_ = nb_i;
                if (x_ref_idx_ == ref_vect_->not_found())
                {
                    BM_ASSERT(0); // XOR de-filter setup issue cannot find ref vector
                    goto throw_err;
                }
                if (blk)
                {
                    BM_ASSERT(!or_block_);
                    or_block_ = bman.deoptimize_block(nb_i);
                    bman.set_block_ptr(nb_i, 0); // borrow OR target before XOR
                    or_block_idx_ = nb_i;
                }
            }
            continue; // important! cont to avoid inc(i)
        case bm::set_block_xor_gap_ref8:
            if (x_ref_d64_) // previous delayed XOR post proc.
                xor_decode(bman);
            row_idx = dec.get_8();
            x_ref_d64_ = ~0ULL;
            goto process_xor_ref;
        case bm::set_block_xor_gap_ref16:
            if (x_ref_d64_) // previous delayed XOR post proc.
                xor_decode(bman);
            row_idx = dec.get_16();
            x_ref_d64_ = ~0ULL;
            goto process_xor_ref;
        case bm::set_block_xor_gap_ref32:
            if (x_ref_d64_) // previous delayed XOR post proc.
                xor_decode(bman);
            row_idx = dec.get_32();
            x_ref_d64_ = ~0ULL;
            goto process_xor_ref;
        case bm::set_block_xor_chain:
            if (x_ref_d64_) // previous delayed XOR post proc.
                xor_decode(bman);
            {
                unsigned char vbr_flag = dec.get_8(); // reserved
                switch (vbr_flag)
                {
                case 1: row_idx = dec.get_8(); break;
                case 2: row_idx = dec.get_16(); break;
                case 0: row_idx = dec.get_32(); break;
                default: BM_ASSERT(0); break;
                } // switch
                bm::id64_t acc64 = x_ref_d64_ = dec.get_h64();
                BM_ASSERT(!xor_chain_size_);
                xor_chain_size_ = dec.get_8();
                BM_ASSERT(xor_chain_size_);
                for (unsigned ci = 0; ci < xor_chain_size_; ++ci)
                {
                    switch (vbr_flag)
                    {
                    case 1: xor_chain_[ci].ref_idx = dec.get_8(); break;
                    case 2: xor_chain_[ci].ref_idx = dec.get_16(); break;
                    case 0: xor_chain_[ci].ref_idx = dec.get_32(); break;
                    default: BM_ASSERT(0); break;
                    } // switch
                    xor_chain_[ci].xor_d64 = dec.get_h64();

                    BM_ASSERT((xor_chain_[ci].xor_d64 & acc64) == 0);
                    acc64 |= xor_chain_[ci].xor_d64; // control
                } // for
            }
            goto process_xor_ref;

        default:
            BM_ASSERT(0); // unknown block type
            throw_err:
            #ifndef BM_NO_STL
                throw std::logic_error(this->err_msg());
            #else
                BM_THROW(BM_ERR_SERIALFORMAT);
            #endif
        } // switch

        ++nb_i;
    } while (nb_i < bm::set_total_blocks);

    // process the last delayed XOR ref here
    //
    if (x_ref_d64_) // XOR post proc.
        xor_decode(bman);

    bv.set_new_blocks_strat(strat);

    return dec.size();
}

// ---------------------------------------------------------------------------

template<class BV, class DEC>
void deserializer<BV, DEC>::xor_decode_chain(bm::word_t* BMRESTRICT blk) BMNOEXCEPT
{
    BM_ASSERT(xor_chain_size_);

    unsigned i0, j0;
    bm::get_block_coord(x_nb_, i0, j0);
    for (unsigned ci = 0; ci < xor_chain_size_; ++ci)
    {
        unsigned ref_idx = (unsigned)ref_vect_->find(xor_chain_[ci].ref_idx);
        const bvector_type* BMRESTRICT ref_bv = ref_vect_->get_bv(ref_idx);
        const blocks_manager_type& ref_bman = ref_bv->get_blocks_manager();
        BM_ASSERT(ref_bv);

        const bm::word_t* BMRESTRICT ref_blk = ref_bman.get_block_ptr(i0, j0);
        if (BM_IS_GAP(ref_blk))
        {
            bm::gap_word_t* BMRESTRICT gap_block = BMGAP_PTR(ref_blk);
            bm::gap_convert_to_bitset(xor_block_, gap_block);
            ref_blk = xor_block_;
        }
        else
            if (IS_FULL_BLOCK(ref_blk))
                ref_blk = FULL_BLOCK_REAL_ADDR;
        if (ref_blk)
            bm::bit_block_xor(blk, ref_blk, xor_chain_[ci].xor_d64);
    } // for ci
}

// ---------------------------------------------------------------------------

template<class BV, class DEC>
void deserializer<BV, DEC>::xor_decode(blocks_manager_type& bman)
{
    BM_ASSERT(ref_vect_);

    unsigned i0, j0;
    const bm::word_t* ref_blk;

    {
        const bvector_type* ref_bv = ref_vect_->get_bv(x_ref_idx_);
        const blocks_manager_type& ref_bman = ref_bv->get_blocks_manager();
        BM_ASSERT(ref_bv);
        BM_ASSERT(&ref_bman != &bman); // some incorrect work with the ref.vect
        bm::get_block_coord(x_nb_, i0, j0);
        ref_blk = ref_bman.get_block_ptr(i0, j0);
    }
    BM_ASSERT(!or_block_ || or_block_idx_ == x_nb_);

    if (!ref_blk)
    {
        if (or_block_)
        {
            bm::word_t* blk = bman.deoptimize_block(i0, j0, true); // true to alloc
            if (blk)
            {
                bm::bit_block_or(blk, or_block_);
                alloc_.free_bit_block(or_block_);
            }
            else
            {
                bman.set_block_ptr(x_nb_, or_block_); // return OR block
            }
            or_block_ = 0; or_block_idx_ = 0;
        }

        if (xor_chain_size_)
        {
            bm::word_t* blk = bman.deoptimize_block(i0, j0, true);
            xor_decode_chain(blk);
        }
        xor_reset();
        return;
    }
    if (IS_FULL_BLOCK(ref_blk))
        ref_blk = FULL_BLOCK_REAL_ADDR;
    else
    {
        if (BM_IS_GAP(ref_blk))
        {
            bm::word_t* blk = bman.get_block_ptr(i0, j0);
            bm::gap_word_t* gap_block = BMGAP_PTR(ref_blk);
            if (BM_IS_GAP(blk) && (x_ref_d64_==~0ULL) && !or_block_) // two GAPs no FULL digest
            {
                BM_ASSERT(!xor_chain_size_); // since x_ref_d64_ == 0xFF...FF
                bm::gap_word_t* tmp_buf = (bm::gap_word_t*)xor_block_;
                const bm::gap_word_t* res;
                unsigned res_len;
                res = bm::gap_operation_xor(BMGAP_PTR(blk),
                                            gap_block,
                                            tmp_buf,
                                            res_len);
                BM_ASSERT(res == tmp_buf);
                bman.assign_gap_check(i0, j0, res, ++res_len, blk, tmp_buf);

                xor_reset();
                return;
            }

            bm::gap_convert_to_bitset(xor_block_, gap_block);
            ref_blk = xor_block_;
        }
        else
        {
            if (IS_FULL_BLOCK(ref_blk))
                ref_blk = FULL_BLOCK_REAL_ADDR;
        }
    }
    bm::word_t* blk = bman.deoptimize_block(i0, j0, true); // true to alloc

    if (x_ref_d64_)
    {
        bm::bit_block_xor(blk, blk, ref_blk, x_ref_d64_);
        if (xor_chain_size_)
            xor_decode_chain(blk);
    }
    else
    {
        BM_ASSERT(xor_chain_size_ == 0);
        bm::bit_block_xor(blk, ref_blk);
    }
    xor_reset();

    if (or_block_)
    {
        bm::bit_block_or(blk, or_block_);
        alloc_.free_bit_block(or_block_);
        or_block_ = 0;
        or_block_idx_ = 0;
    }

    // target BLOCK post-processing
    //
    // check if we range setting overrides the need for block optimization
    //
    if (is_range_set_)
    {
        block_idx_type nb_from = (idx_from_ >> bm::set_block_shift);
        block_idx_type nb_to = (idx_to_ >> bm::set_block_shift);
        if (nb_from == x_nb_ || nb_to == x_nb_)
            return;
    }
    bman.optimize_bit_block(i0, j0, BV::opt_compress);

}
// ---------------------------------------------------------------------------

template<class BV, class DEC>
void deserializer<BV, DEC>::xor_reset() BMNOEXCEPT
{
    x_ref_idx_ = 0; x_ref_d64_ = 0; xor_chain_size_ = 0;
    x_nb_ = 0;
}

// ---------------------------------------------------------------------------

template<typename DEC, typename BLOCK_IDX>
serial_stream_iterator<DEC, BLOCK_IDX>::serial_stream_iterator(const unsigned char* buf)
  : header_flag_(0),
    decoder_(buf),
    end_of_stream_(false),
    bv_size_(0),
    state_(e_unknown),
    id_cnt_(0),
    block_idx_(0),
    mono_block_cnt_(0),
    block_idx_arr_(0)
{
    ::memset(bit_func_table_, 0, sizeof(bit_func_table_));

    bit_func_table_[bm::set_AND] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_AND;

    bit_func_table_[bm::set_ASSIGN] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_ASSIGN;
    bit_func_table_[bm::set_OR]     = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_OR;
    bit_func_table_[bm::set_SUB] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_SUB;
    bit_func_table_[bm::set_XOR] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_XOR;
    bit_func_table_[bm::set_COUNT] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT;
    bit_func_table_[bm::set_COUNT_AND] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_AND;
    bit_func_table_[bm::set_COUNT_XOR] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_XOR;
    bit_func_table_[bm::set_COUNT_OR] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_OR;
    bit_func_table_[bm::set_COUNT_SUB_AB] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_SUB_AB;
    bit_func_table_[bm::set_COUNT_SUB_BA] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_SUB_BA;
    bit_func_table_[bm::set_COUNT_A] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_A;
    bit_func_table_[bm::set_COUNT_B] = 
        &serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT;


    // reading stream header
    header_flag_ =  decoder_.get_8();
    if (!(header_flag_ & BM_HM_NO_BO))
    {
        /*ByteOrder bo = (bm::ByteOrder)*/decoder_.get_8();
    }

    // check if bitvector comes as an inverted, sorted list of ints
    //
    if (header_flag_ & BM_HM_ID_LIST)
    {
        // special case: the next comes plain list of unsigned integers
        if (header_flag_ & BM_HM_RESIZE)
        {
            if (header_flag_ & BM_HM_64_BIT)
            {
                BM_ASSERT(sizeof(block_idx_type)==8);
                bv_size_ = (block_idx_type)decoder_.get_64();
            }
            else
                bv_size_ = decoder_.get_32();
        }

        state_ = e_list_ids;
        id_cnt_ = decoder_.get_32();
        next(); // read first id
    }
    else
    {
        if (!(header_flag_ & BM_HM_NO_GAPL))
        {
            for (unsigned i = 0; i < bm::gap_levels; ++i) // load the GAP levels
                glevels_[i] = decoder_.get_16();
        }
        if (header_flag_ & BM_HM_RESIZE)
        {
            if (header_flag_ & BM_HM_64_BIT)
            {
                BM_ASSERT(sizeof(block_idx_type)==8);
                bv_size_ = (block_idx_type)decoder_.get_64();
            }
            else
                bv_size_ = decoder_.get_32();
        }
        state_ = e_blocks;
    }
    block_idx_arr_=(gap_word_t*)::malloc(sizeof(gap_word_t) * bm::gap_max_bits);
    if (!block_idx_arr_)
    {
        #ifndef BM_NO_STL
            throw std::bad_alloc();
        #else
            BM_THROW(BM_ERR_BADALLOC);
        #endif
    }

    this->id_array_ = block_idx_arr_;
}

template<typename DEC, typename BLOCK_IDX>
serial_stream_iterator<DEC, BLOCK_IDX>::~serial_stream_iterator()
{
    if (block_idx_arr_)
        ::free(block_idx_arr_);
}


template<typename DEC, typename BLOCK_IDX>
void serial_stream_iterator<DEC, BLOCK_IDX>::next()
{
    if (is_eof())
    {
        ++block_idx_;
        return;
    }
    block_idx_type nb_sync;

    switch (state_) 
    {
    case e_list_ids:
        // read inverted ids one by one
        if (id_cnt_ == 0)
        {
            end_of_stream_ = true;
            state_ = e_unknown;
            break;
        }
        last_id_ = decoder_.get_32();
        --id_cnt_;
        break;

    case e_blocks:
        if (block_idx_ == bm::set_total_blocks)
        {
            end_of_stream_ = true;
            state_ = e_unknown;
            break;
        }

        block_type_ = decoder_.get_8();

        // pre-check for 7-bit zero block
        //
        if (block_type_ & (1u << 7u))
        {
            mono_block_cnt_ = (block_type_ & ~(1u << 7u)) - 1;
            state_ = e_zero_blocks;
            break;
        }

        switch (block_type_)
        {
        case set_block_azero:
        case set_block_end:
            end_of_stream_ = true; state_ = e_unknown;
            break;
        case set_block_1zero:
            state_ = e_zero_blocks;
            mono_block_cnt_ = 0;
            break;
        case set_block_8zero:
            state_ = e_zero_blocks;
            mono_block_cnt_ = decoder_.get_8()-1;
            break;
        case set_block_16zero:
            state_ = e_zero_blocks;
            mono_block_cnt_ = decoder_.get_16()-1;
            break;
        case set_block_32zero:
            state_ = e_zero_blocks;
            mono_block_cnt_ = decoder_.get_32()-1;
            break;
        case set_block_aone:
            state_ = e_one_blocks;
            mono_block_cnt_ = bm::set_total_blocks - block_idx_;
            break;
        case set_block_1one:
            state_ = e_one_blocks;
            mono_block_cnt_ = 0;
            break;
        case set_block_8one:
            state_ = e_one_blocks;
            mono_block_cnt_ = decoder_.get_8()-1;
            break;
        case set_block_16one:
            state_ = e_one_blocks;
            mono_block_cnt_ = decoder_.get_16()-1;
            break;
        case set_block_32one:
            state_ = e_one_blocks;
            mono_block_cnt_ = decoder_.get_32()-1;
            break;
        case set_block_64one:
            state_ = e_one_blocks;
            mono_block_cnt_ = block_idx_type(decoder_.get_64()-1);
            break;

        case bm::set_block_bit:
        case bm::set_block_bit_interval:
        case bm::set_block_bit_0runs:
        case bm::set_block_arrbit:
        case bm::set_block_arrbit_inv:
        case bm::set_block_arr_bienc:
        case bm::set_block_arr_bienc_inv:
        case bm::set_block_arr_bienc_8bh:
        case bm::set_block_bitgap_bienc:
        case bm::set_block_bit_digest0:
            state_ = e_bit_block;
            break;
        case set_block_gap:
            BM_FALLTHROUGH;
        case set_block_gap_egamma:
            BM_FALLTHROUGH;
        case set_block_gap_bienc:
            BM_FALLTHROUGH;
        case set_block_gap_bienc_v2:
            gap_head_ = decoder_.get_16();
            BM_FALLTHROUGH;
        case set_block_arrgap:
            BM_FALLTHROUGH;
        case set_block_arrgap_egamma:
            BM_FALLTHROUGH;
        case set_block_arrgap_egamma_inv:
            BM_FALLTHROUGH;
        case set_block_arrgap_inv:
            BM_FALLTHROUGH;
		case set_block_bit_1bit:
            BM_FALLTHROUGH;
        case set_block_arrgap_bienc:
            BM_FALLTHROUGH;
        case set_block_arrgap_bienc_v2:
            BM_FALLTHROUGH;
        case set_block_arrgap_bienc_inv_v2:
            BM_FALLTHROUGH;
        case set_block_arrgap_bienc_inv:
            state_ = e_gap_block;
            break;        
        case set_block_gapbit:
            state_ = e_gap_block; //e_bit_block; // TODO: make a better decision here
            break;

        // --------------------------------------------- bookmarks and syncs
        //
        case set_nb_bookmark32:
            this->bookmark_idx_ = block_idx_;
            this->skip_offset_ = decoder_.get_32();
            this->skip_pos_ = decoder_.get_pos() + this->skip_offset_;
            break;
        case set_nb_bookmark24:
            this->bookmark_idx_ = block_idx_;
            this->skip_offset_ = decoder_.get_24();
            this->skip_pos_ = decoder_.get_pos() + this->skip_offset_;
            break;
        case set_nb_bookmark16:
            this->bookmark_idx_ = block_idx_;
            this->skip_offset_ = decoder_.get_16();
            this->skip_pos_ = decoder_.get_pos() + this->skip_offset_;
            break;

        case set_nb_sync_mark8:
            nb_sync = decoder_.get_8();
            goto process_nb_sync;
        case set_nb_sync_mark16:
            nb_sync = decoder_.get_16();
            goto process_nb_sync;
        case set_nb_sync_mark24:
            nb_sync = decoder_.get_24();
            goto process_nb_sync;
        case set_nb_sync_mark32:
            nb_sync = decoder_.get_32();
            goto process_nb_sync;
        case set_nb_sync_mark48:
            nb_sync = block_idx_type(decoder_.get_48());
            goto process_nb_sync;
        case set_nb_sync_mark64:
            nb_sync = block_idx_type(decoder_.get_64());
            process_nb_sync:
                BM_ASSERT(block_idx_ == this->bookmark_idx_ + nb_sync);
                if (block_idx_ != this->bookmark_idx_ + nb_sync)
                {
                    #ifndef BM_NO_STL
                        throw std::logic_error(this->err_msg());
                    #else
                        BM_THROW(BM_ERR_SERIALFORMAT);
                    #endif
                }
            break;



        // --------------------------------------------------------------
        // XOR deserials are incompatible with the operation deserialial
        // throw or assert here
        //
        case set_block_ref_eq:
        case set_block_xor_ref8:
        case set_block_xor_ref16:
        case set_block_xor_ref32:
        case set_block_xor_ref8_um:
        case set_block_xor_ref16_um:
        case set_block_xor_ref32_um:
            BM_FALLTHROUGH;
            // fall through
        case set_sblock_bienc:
            BM_ASSERT(0); 
            BM_FALLTHROUGH;
            // fall through
        default:
            BM_ASSERT(0);
            #ifndef BM_NO_STL
                throw std::logic_error(this->err_msg());
            #else
                BM_THROW(BM_ERR_SERIALFORMAT);
            #endif
        } // switch

        break;

    case e_zero_blocks:
    case e_one_blocks:
        ++block_idx_;
        if (!mono_block_cnt_)
        {
            state_ = e_blocks; // get new block token
            break;
        }
        --mono_block_cnt_;
        break;

    case e_unknown:
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
}

template<typename DEC, typename BLOCK_IDX>
typename serial_stream_iterator<DEC, BLOCK_IDX>::block_idx_type
serial_stream_iterator<DEC, BLOCK_IDX>::skip_mono_blocks() BMNOEXCEPT
{
	BM_ASSERT(state_ == e_zero_blocks || state_ == e_one_blocks);
    if (!mono_block_cnt_)
		++block_idx_;
	else
	{
		block_idx_ += mono_block_cnt_+1;
		mono_block_cnt_ = 0;
	}
    state_ = e_blocks;
    return block_idx_;
}

template<typename DEC, typename BLOCK_IDX>
void
serial_stream_iterator<DEC, BLOCK_IDX>::get_inv_arr(bm::word_t* block) BMNOEXCEPT
{
    gap_word_t len = decoder_.get_16();
    if (block)
    {
        bm::bit_block_set(block, ~0u);
        for (unsigned k = 0; k < len; ++k)
        {
            bm::gap_word_t bit_idx = decoder_.get_16();
            bm::clear_bit(block, bit_idx);
        }
    }
    else // dry read
    {
        for (unsigned k = 0; k < len; ++k)
            decoder_.get_16();
    }
}


template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_ASSIGN(
                                            bm::word_t*  dst_block,
                                            bm::word_t*  tmp_block)
{
    //  ASSIGN should be ready for dry read (dst_block can be NULL)
    //
    BM_ASSERT(this->state_ == e_bit_block);
    unsigned count = 0;

    switch (this->block_type_)
    {
    case set_block_bit:
        decoder_.get_32(dst_block, bm::set_block_size);
        break;
    case set_block_bit_0runs: 
        {
        if (IS_VALID_ADDR(dst_block))
            bm::bit_block_set(dst_block, 0);
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            if (run_type)
            {
				decoder_.get_32(dst_block ? dst_block + j : dst_block, run_length);
			}
			j += run_length;
        } // for
        }
        break;
    case set_block_bit_interval:
        {
            unsigned head_idx = decoder_.get_16();
            unsigned tail_idx = decoder_.get_16();
            if (dst_block) 
            {
                for (unsigned i = 0; i < head_idx; ++i)
                    dst_block[i] = 0;
                decoder_.get_32(dst_block + head_idx, 
                                tail_idx - head_idx + 1);
                for (unsigned j = tail_idx + 1; j < bm::set_block_size; ++j)
                    dst_block[j] = 0;
            }
            else
            {
                int pos = int(tail_idx - head_idx) + 1;
                pos *= 4u;
                decoder_.seek(pos);
            }
        }
        break;
    case set_block_arrbit:
    case set_block_bit_1bit:
        get_arr_bit(dst_block, true /*clear target*/);
        break;
    case set_block_gapbit:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
        break;
    case set_block_arrbit_inv:
        get_inv_arr(dst_block);
        break;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        if (IS_VALID_ADDR(dst_block))
            bm::bit_block_set(dst_block, 0);
        this->read_bic_arr(decoder_, dst_block, this->block_type_);
        break;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        if (IS_VALID_ADDR(dst_block))
            bm::bit_block_copy(dst_block, tmp_block);
        break;
    case bm::set_block_bitgap_bienc:
        if (IS_VALID_ADDR(dst_block))
            bm::bit_block_set(dst_block, 0);
        this->read_bic_gap(decoder_, dst_block);
        break;
    case bm::set_block_bit_digest0:
        if (IS_VALID_ADDR(dst_block))
            bm::bit_block_set(dst_block, 0);
        this->read_digest0_block(decoder_, dst_block);
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
    return count;
}

template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_OR(
                                                bm::word_t*  dst_block,
                                                bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    unsigned count = 0;
    switch (block_type_)
    {
    case set_block_bit:
        decoder_.get_32_OR(dst_block, bm::set_block_size);
        break;
    case set_block_bit_interval:
        {
        unsigned head_idx = decoder_.get_16();
        unsigned tail_idx = decoder_.get_16();
        for (unsigned i = head_idx; i <= tail_idx; ++i)
            dst_block[i] |= decoder_.get_32();
        }
        break;
    case set_block_bit_0runs:
        {
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            if (run_type)
            {
                unsigned run_end = j + run_length;
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    dst_block[j] |= decoder_.get_32();
                }
            }
            else
            {
                j += run_length;
            }
        } // for
        }
        break;
    case set_block_bit_1bit:
    case set_block_arrbit:
        get_arr_bit(dst_block, false /*don't clear target*/);
        break;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        bm::bit_block_or(dst_block, tmp_block);
        break;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        this->read_bic_arr(decoder_, dst_block, this->block_type_);
        break;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        bm::bit_block_or(dst_block, tmp_block);
        break;
    case bm::set_block_bitgap_bienc:
        this->read_bic_gap(decoder_, dst_block);
        break;
    case bm::set_block_bit_digest0:
        this->read_digest0_block(decoder_, dst_block);
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
    return count;
}

template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_AND(
                                        bm::word_t* BMRESTRICT dst_block,
                                        bm::word_t* BMRESTRICT tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    BM_ASSERT(dst_block != tmp_block);
    unsigned count = 0;
    switch (block_type_)
    {
    case set_block_bit:
        decoder_.get_32_AND(dst_block, bm::set_block_size);
        break;
    case set_block_bit_0runs:
        {
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();

            unsigned run_end = j + run_length;
            if (run_type)
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    dst_block[j] &= decoder_.get_32();
                }
            }
            else
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    dst_block[j] = 0;
                }
            }
        } // for
        }
        break;
    case set_block_bit_interval:
        {
            unsigned head_idx = decoder_.get_16();
            unsigned tail_idx = decoder_.get_16();
            unsigned i;
            for ( i = 0; i < head_idx; ++i)
                dst_block[i] = 0;
            for ( i = head_idx; i <= tail_idx; ++i)
                dst_block[i] &= decoder_.get_32();
            for ( i = tail_idx + 1; i < bm::set_block_size; ++i)
                dst_block[i] = 0;
        }
        break;
    case set_block_bit_1bit:
    case set_block_arrbit:
        get_arr_bit(tmp_block, true /*clear target*/);
        if (dst_block)
            bm::bit_block_and(dst_block, tmp_block);
        break;		
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        if (dst_block)
            bm::bit_block_and(dst_block, tmp_block);
        break;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        if (dst_block)
        {
            bm::bit_block_set(tmp_block, 0);
            this->read_bic_arr(decoder_, tmp_block, block_type_);
            bm::bit_block_and(dst_block, tmp_block);
        }
        else
            this->read_bic_arr(decoder_, 0, block_type_); // dry read
        break;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        if (dst_block)
            bm::bit_block_and(dst_block, tmp_block);
        break;
    case bm::set_block_bitgap_bienc:
        if (dst_block)
        {
            BM_ASSERT(IS_VALID_ADDR(dst_block));
            bm::bit_block_set(tmp_block, 0);
            this->read_bic_gap(decoder_, tmp_block);
            bm::bit_block_and(dst_block, tmp_block);
        }
        else
            this->read_bic_gap(decoder_, 0); // dry read
        break;
    case bm::set_block_bit_digest0:
        if (dst_block)
        {
            BM_ASSERT(IS_VALID_ADDR(dst_block));
            bm::bit_block_set(tmp_block, 0);
            this->read_digest0_block(decoder_, tmp_block);
            bm::bit_block_and(dst_block, tmp_block);
        }
        else
            this->read_digest0_block(decoder_, 0); // dry read
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
    return count;
}

template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_XOR(
                                                bm::word_t*  dst_block,
                                                bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    BM_ASSERT(dst_block != tmp_block);

    unsigned count = 0;
    switch (block_type_)
    {
    case set_block_bit:
        for (unsigned i = 0; i < bm::set_block_size; ++i)
            dst_block[i] ^= decoder_.get_32();
        break;
    case set_block_bit_0runs:
        {
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            if (run_type)
            {
                unsigned run_end = j + run_length;
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    dst_block[j] ^= decoder_.get_32();
                }
            }
            else
            {
                j += run_length;
            }
        } // for
        }
        break;
    case set_block_bit_interval:
        {
            unsigned head_idx = decoder_.get_16();
            unsigned tail_idx = decoder_.get_16();
            for (unsigned i = head_idx; i <= tail_idx; ++i)
                dst_block[i] ^= decoder_.get_32();
        }
        break;
    case set_block_bit_1bit:
        // TODO: optimization
    case set_block_arrbit:
        get_arr_bit(tmp_block, true /*clear target*/);
        if (dst_block)
            bm::bit_block_xor(dst_block, tmp_block);
        break;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        if (dst_block)
            bm::bit_block_xor(dst_block, tmp_block);
        break;
    case set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_arr(decoder_, tmp_block, block_type_);
        if (dst_block)
            bm::bit_block_xor(dst_block, tmp_block);
        break;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        if (dst_block)
        {
            BM_ASSERT(IS_VALID_ADDR(dst_block));
            bm::bit_block_xor(dst_block, tmp_block);
        }
        break;
    case bm::set_block_bitgap_bienc:
        if (dst_block)
        {
            BM_ASSERT(IS_VALID_ADDR(dst_block));
            bm::bit_block_set(tmp_block, 0);
            this->read_bic_gap(decoder_, tmp_block);
            bm::bit_block_xor(dst_block, tmp_block);
        }
        else
            this->read_bic_gap(decoder_, 0); // dry read
        break;
    case bm::set_block_bit_digest0:
        if (dst_block)
        {
            BM_ASSERT(IS_VALID_ADDR(dst_block));
            bm::bit_block_set(tmp_block, 0);
            this->read_digest0_block(decoder_, tmp_block);
            bm::bit_block_xor(dst_block, tmp_block);
        }
        else
            this->read_digest0_block(decoder_, 0); // dry read
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
    return count;
}

template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_SUB(
                                                bm::word_t*  dst_block,
                                                bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    BM_ASSERT(dst_block != tmp_block);

    unsigned count = 0;
    switch (block_type_)
    {
    case set_block_bit:
        for (unsigned i = 0; i < bm::set_block_size; ++i)
            dst_block[i] &= ~decoder_.get_32();
        break;
    case set_block_bit_0runs:
        {
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            if (run_type)
            {
                unsigned run_end = j + run_length;
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    dst_block[j] &= ~decoder_.get_32();
                }
            }
            else
            {
                j += run_length;
            }
        } // for
        }
        break;
    case set_block_bit_interval:
        {
            unsigned head_idx = decoder_.get_16();
            unsigned tail_idx = decoder_.get_16();
            for (unsigned i = head_idx; i <= tail_idx; ++i)
                dst_block[i] &= ~decoder_.get_32();
        }
        break;
    case set_block_bit_1bit:
        // TODO: optimization
    case set_block_arrbit:
        get_arr_bit(tmp_block, true /*clear target*/);
        if (dst_block)
            bm::bit_block_sub(dst_block, tmp_block);
        break;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        if (dst_block)
            bm::bit_block_sub(dst_block, tmp_block);
        break;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_arr(decoder_, tmp_block, block_type_);
        if (dst_block)
            bm::bit_block_sub(dst_block, tmp_block);
        break;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        if (dst_block)
            bm::bit_block_sub(dst_block, tmp_block);
        break;
    case bm::set_block_bitgap_bienc:
        if (dst_block)
        {
            BM_ASSERT(IS_VALID_ADDR(dst_block));
            bm::bit_block_set(tmp_block, 0);
            this->read_bic_gap(decoder_, tmp_block);
            bm::bit_block_sub(dst_block, tmp_block);
        }
        else
            this->read_bic_gap(decoder_, 0); // dry read
        break;
    case bm::set_block_bit_digest0:
        if (dst_block)
        {
            BM_ASSERT(IS_VALID_ADDR(dst_block));
            bm::bit_block_set(tmp_block, 0);
            this->read_digest0_block(decoder_, tmp_block);
            bm::bit_block_sub(dst_block, tmp_block);
        }
        else
            this->read_digest0_block(decoder_, 0); // dry read
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
    return count;
}


template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT(
                                                 bm::word_t*  /*dst_block*/,
                                                 bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);

    unsigned count = 0;
    switch (block_type_)
    {
    case set_block_bit:
        for (unsigned i = 0; i < bm::set_block_size; ++i)
            count += bm::word_bitcount(decoder_.get_32());
        break;
    case set_block_bit_0runs:
        {
        //count = 0;
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            if (run_type)
            {
                unsigned run_end = j + run_length;
                for (;j < run_end; ++j)
                {
                    count += word_bitcount(decoder_.get_32());
                }
            }
            else
            {
                j += run_length;
            }
        } // for
        return count;
        }
    case set_block_bit_interval:
        {
            unsigned head_idx = decoder_.get_16();
            unsigned tail_idx = decoder_.get_16();
            for (unsigned i = head_idx; i <= tail_idx; ++i)
                count += bm::word_bitcount(decoder_.get_32());
        }
        break;
    case set_block_arrbit:
        count += get_arr_bit(0);
        break;
    case set_block_bit_1bit:
        ++count;
        decoder_.get_16();
        break;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        goto count_tmp;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        bm::bit_block_set(tmp_block, 0); // TODO: just add a counted read
        this->read_bic_arr(decoder_, tmp_block, block_type_);
        goto count_tmp;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bit_digest0:
        bm::bit_block_set(tmp_block, 0);
        this->read_digest0_block(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bitgap_bienc:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_gap(decoder_, tmp_block);
    count_tmp:
        count += bm::bit_block_count(tmp_block);
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif

    } // switch
    return count;
}

template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_A(
                                                    bm::word_t*  dst_block,
                                                    bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    unsigned count = 0;
    if (dst_block)
    {
        // count the block bitcount
        count = bm::bit_block_count(dst_block);
    }

    switch (block_type_)
    {
    case set_block_bit:
        decoder_.get_32(0, bm::set_block_size);
        break;
    case set_block_bit_0runs:
        {
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            if (run_type)
            {
                unsigned run_end = j + run_length;
                for (;j < run_end; ++j)
                {
                    decoder_.get_32();
                }
            }
            else
            {
                j += run_length;
            }
        } // for
        }
        break;

    case set_block_bit_interval:
        {
            unsigned head_idx = decoder_.get_16();
            unsigned tail_idx = decoder_.get_16();
            for (unsigned i = head_idx; i <= tail_idx; ++i)
                decoder_.get_32();
        }
        break;
    case set_block_arrbit:
        get_arr_bit(0);
        break;
    case set_block_bit_1bit:
        decoder_.get_16();
        break;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        break;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        this->read_bic_arr(decoder_, tmp_block, block_type_); // TODO: add dry read
        break;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block); // TODO: add dry read
        break;
    case bm::set_block_bitgap_bienc:
        this->read_bic_gap(decoder_, tmp_block);
        break;
    case bm::set_block_bit_digest0:
        this->read_digest0_block(decoder_, 0); // dry read
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif

    } // switch
    return count;
}


template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_AND(
                                                    bm::word_t*  dst_block,
                                                    bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    BM_ASSERT(dst_block);

    unsigned count = 0;
    switch (block_type_)
    {
    case set_block_bit:
        for (unsigned i = 0; i < bm::set_block_size; ++i)
            count += word_bitcount(dst_block[i] & decoder_.get_32());
        break;
    case set_block_bit_0runs:
        {
        //count = 0;
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            if (run_type)
            {
                unsigned run_end = j + run_length;
                for (;j < run_end; ++j)
                {
                    count += word_bitcount(dst_block[j] & decoder_.get_32());
                }
            }
            else
            {
                j += run_length;
            }
        } // for
        return count;
        }
    case set_block_bit_interval:
        {
        unsigned head_idx = decoder_.get_16();
        unsigned tail_idx = decoder_.get_16();
        for (unsigned i = head_idx; i <= tail_idx; ++i)
            count += word_bitcount(dst_block[i] & decoder_.get_32());
        }
        break;
    case set_block_bit_1bit:
        // TODO: optimization
    case set_block_arrbit:
        get_arr_bit(tmp_block, true /*clear target*/);
        goto count_tmp;
        break;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        goto count_tmp;
        break;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_arr(decoder_, tmp_block, block_type_);
        goto count_tmp;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bit_digest0:
        bm::bit_block_set(tmp_block, 0);
        this->read_digest0_block(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bitgap_bienc:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_gap(decoder_, tmp_block);
    count_tmp:
        count += bm::bit_operation_and_count(dst_block, tmp_block);
        break;
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif

    } // switch
    return count;
}

template<typename DEC,typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_OR(
                                                    bm::word_t*  dst_block,
                                                    bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    BM_ASSERT(dst_block);

    bitblock_sum_adapter count_adapter;
    switch (block_type_)
    {
    case set_block_bit:
        {
        bitblock_get_adapter ga(dst_block);
        bit_COUNT_OR<bm::word_t> func;
        
        bit_recomb(ga,
                   decoder_,
                   func,
                   count_adapter
                  );
        }
        break;
    case set_block_bit_0runs: 
        {
        unsigned count = 0;
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            unsigned run_end = j + run_length;
            if (run_type)
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    count += word_bitcount(dst_block[j] | decoder_.get_32());
                }
            }
            else
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    count += word_bitcount(dst_block[j]);
                }
            }
        } // for
        return count;
        }
    case set_block_bit_interval:
        {
        unsigned head_idx = decoder_.get_16();
        unsigned tail_idx = decoder_.get_16();
        unsigned count = 0;
        unsigned i;
        for (i = 0; i < head_idx; ++i)
            count += bm::word_bitcount(dst_block[i]);
        for (i = head_idx; i <= tail_idx; ++i)
            count += bm::word_bitcount(dst_block[i] | decoder_.get_32());
        for (i = tail_idx + 1; i < bm::set_block_size; ++i)
            count += bm::word_bitcount(dst_block[i]);
        return count;
        }
    case set_block_bit_1bit:
        // TODO: optimization
    case set_block_arrbit:
        get_arr_bit(tmp_block, true /* clear target*/);
        return bit_operation_or_count(dst_block, tmp_block);
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        goto count_tmp;
    case set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_arr(decoder_, tmp_block, block_type_);
        goto count_tmp;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bit_digest0:
        bm::bit_block_set(tmp_block, 0);
        this->read_digest0_block(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bitgap_bienc:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_gap(decoder_, tmp_block);
    count_tmp:
        return bm::bit_operation_or_count(dst_block, tmp_block);
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif

    } // switch
    return count_adapter.sum();
}

template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_XOR(
                                                    bm::word_t*  dst_block,
                                                    bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    BM_ASSERT(dst_block);

    bitblock_sum_adapter count_adapter;
    switch (block_type_)
    {
    case set_block_bit:
        {
        bitblock_get_adapter ga(dst_block);
        bit_COUNT_XOR<bm::word_t> func;
        
        bit_recomb(ga,
                   decoder_,
                   func,
                   count_adapter
                  );
        }
        break;
    case set_block_bit_0runs: 
        {
        unsigned count = 0;
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            unsigned run_end = j + run_length;
            if (run_type)
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    count += bm::word_bitcount(dst_block[j] ^ decoder_.get_32());
                }
            }
            else
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    count += bm::word_bitcount(dst_block[j]);
                }
            }
        } // for
        return count;
        }
    case set_block_bit_interval:
        {
        unsigned head_idx = decoder_.get_16();
        unsigned tail_idx = decoder_.get_16();
        unsigned count = 0;
        unsigned i;
        for (i = 0; i < head_idx; ++i)
            count += bm::word_bitcount(dst_block[i]);
        for (i = head_idx; i <= tail_idx; ++i)
            count += bm::word_bitcount(dst_block[i] ^ decoder_.get_32());
        for (i = tail_idx + 1; i < bm::set_block_size; ++i)
            count += bm::word_bitcount(dst_block[i]);
        return count;
        }
    case set_block_bit_1bit:
        // TODO: optimization
    case set_block_arrbit:
        get_arr_bit(tmp_block, true /* clear target*/);
        goto count_tmp;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        goto count_tmp;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_arr(decoder_, tmp_block, block_type_);
        goto count_tmp;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        goto count_tmp;
        break;
    case bm::set_block_bit_digest0:
        bm::bit_block_set(tmp_block, 0);
        this->read_digest0_block(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bitgap_bienc:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_gap(decoder_, tmp_block);
    count_tmp:
        return bm::bit_operation_xor_count(dst_block, tmp_block);
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif

    } // switch
    return count_adapter.sum();
}

template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_SUB_AB(
                                                    bm::word_t*  dst_block,
                                                    bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    BM_ASSERT(dst_block);

    bitblock_sum_adapter count_adapter;
    switch (block_type_)
    {
    case set_block_bit:
        {
        bitblock_get_adapter ga(dst_block);
        bit_COUNT_SUB_AB<bm::word_t> func;
        
        bit_recomb(ga, 
                   decoder_,
                   func,
                   count_adapter
                  );
        }
        break;
    case set_block_bit_0runs: 
        {
        unsigned count = 0;
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            unsigned run_end = j + run_length;
            if (run_type)
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    count += word_bitcount(dst_block[j] & ~decoder_.get_32());
                }
            }
            else
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    count += bm::word_bitcount(dst_block[j]);
                }
            }
        } // for
        return count;
        }
    case set_block_bit_interval:
        {
        unsigned head_idx = decoder_.get_16();
        unsigned tail_idx = decoder_.get_16();
        unsigned count = 0;
        unsigned i;
        for (i = 0; i < head_idx; ++i)
            count += bm::word_bitcount(dst_block[i]);
        for (i = head_idx; i <= tail_idx; ++i)
            count += bm::word_bitcount(dst_block[i] & (~decoder_.get_32()));
        for (i = tail_idx + 1; i < bm::set_block_size; ++i)
            count += bm::word_bitcount(dst_block[i]);
        return count;
        }
        break;
    case set_block_bit_1bit:
        //TODO: optimization
    case set_block_arrbit:
        get_arr_bit(tmp_block, true /* clear target*/);
        goto count_tmp;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        goto count_tmp;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_arr(decoder_, tmp_block, block_type_);
        goto count_tmp;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bit_digest0:
        bm::bit_block_set(tmp_block, 0);
        this->read_digest0_block(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bitgap_bienc:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_gap(decoder_, tmp_block);
    count_tmp:
        return bm::bit_operation_sub_count(dst_block, tmp_block);
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif

    } // switch
    return count_adapter.sum();
}

template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block_COUNT_SUB_BA(
                                                        bm::word_t*  dst_block,
                                                        bm::word_t*  tmp_block)
{
    BM_ASSERT(this->state_ == e_bit_block);
    BM_ASSERT(dst_block);

    bitblock_sum_adapter count_adapter;
    switch (block_type_)
    {
    case set_block_bit:
        {
        bitblock_get_adapter ga(dst_block);
        bit_COUNT_SUB_BA<bm::word_t> func;

        bit_recomb(ga,
                   decoder_,
                   func,
                   count_adapter
                  );
        }
        break;
    case set_block_bit_0runs: 
        {
        unsigned count = 0;
        unsigned char run_type = decoder_.get_8();
        for (unsigned j = 0; j < bm::set_block_size;run_type = !run_type)
        {
            unsigned run_length = decoder_.get_16();
            unsigned run_end = j + run_length;
            if (run_type)
            {
                for (;j < run_end; ++j)
                {
                    BM_ASSERT(j < bm::set_block_size);
                    count += word_bitcount(decoder_.get_32() & (~dst_block[j]));
                }
            }
            else
            {
                j += run_length;
            }
        } // for
        return count;
        }
    case set_block_bit_interval:
        {
        unsigned head_idx = decoder_.get_16();
        unsigned tail_idx = decoder_.get_16();
        unsigned count = 0;
        unsigned i;
        for (i = head_idx; i <= tail_idx; ++i)
            count += bm::word_bitcount(decoder_.get_32() & (~dst_block[i]));
        return count;
        }
        break;
    case set_block_bit_1bit:
        // TODO: optimization
    case set_block_arrbit:
        get_arr_bit(tmp_block, true /* clear target*/);
        goto count_tmp;
    case set_block_arrbit_inv:
        get_inv_arr(tmp_block);
        goto count_tmp;
    case bm::set_block_arr_bienc:
    case bm::set_block_arr_bienc_8bh:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_arr(decoder_, tmp_block, block_type_);
        goto count_tmp;
    case bm::set_block_arr_bienc_inv:
        this->read_bic_arr_inv(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bit_digest0:
        bm::bit_block_set(tmp_block, 0);
        this->read_digest0_block(decoder_, tmp_block);
        goto count_tmp;
    case bm::set_block_bitgap_bienc:
        bm::bit_block_set(tmp_block, 0);
        this->read_bic_gap(decoder_, tmp_block);
    count_tmp:
        return bm::bit_operation_sub_count(tmp_block, dst_block);
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(this->err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch
    return count_adapter.sum();
}



template<typename DEC, typename BLOCK_IDX>
unsigned serial_stream_iterator<DEC, BLOCK_IDX>::get_arr_bit(
                                          bm::word_t* dst_block,
                                          bool        clear_target) BMNOEXCEPT
{
    BM_ASSERT(this->block_type_ == set_block_arrbit || 
              this->block_type_ == set_block_bit_1bit);
    
    gap_word_t len = decoder_.get_16(); // array length / 1bit_idx
    if (dst_block)
    {
        if (clear_target)
            bm::bit_block_set(dst_block, 0);

        if (this->block_type_ == set_block_bit_1bit)
        {
            // len contains idx of 1 bit set
            set_bit(dst_block, len);
            return 1;
        }

        for (unsigned k = 0; k < len; ++k)
        {
            gap_word_t bit_idx = decoder_.get_16();
            bm::set_bit(dst_block, bit_idx);
        }
    }
    else
    {
        if (this->block_type_ == set_block_bit_1bit)
            return 1; // nothing to do: len var already consumed 16 bits

        // fwd the decode stream
        decoder_.seek(len * 2);
    }
    return len;
}

template<typename DEC, typename BLOCK_IDX>
unsigned serial_stream_iterator<DEC, BLOCK_IDX>::get_bit() BMNOEXCEPT
{
    BM_ASSERT(this->block_type_ == set_block_bit_1bit);
    ++(this->block_idx_);
    this->state_ = e_blocks;

	return decoder_.get_16(); // 1bit_idx	
}

template<typename DEC, typename BLOCK_IDX>
void 
serial_stream_iterator<DEC, BLOCK_IDX>::get_gap_block(bm::gap_word_t* dst_block)
{
    BM_ASSERT(this->state_ == e_gap_block || 
              this->block_type_ == set_block_bit_1bit);
    BM_ASSERT(dst_block);

    this->read_gap_block(this->decoder_,
                   this->block_type_,
                   dst_block,
                   this->gap_head_);

    ++(this->block_idx_);
    this->state_ = e_blocks;
}


template<typename DEC, typename BLOCK_IDX>
unsigned 
serial_stream_iterator<DEC, BLOCK_IDX>::get_bit_block(
                                            bm::word_t*    dst_block,
                                            bm::word_t*    tmp_block,
                                            set_operation  op)
{
    BM_ASSERT(this->state_ == e_bit_block);
    
    get_bit_func_type bit_func = bit_func_table_[op];
    BM_ASSERT(bit_func);
    unsigned cnt = ((*this).*(bit_func))(dst_block, tmp_block);
    this->state_ = e_blocks;
    ++(this->block_idx_);
    return cnt;
}

// ----------------------------------------------------------------------
//
// ----------------------------------------------------------------------


template<class BV>
operation_deserializer<BV>::operation_deserializer()
: temp_block_(0), ref_vect_(0)
{
    temp_block_ = alloc_.alloc_bit_block();
}


template<class BV>
operation_deserializer<BV>::~operation_deserializer()
{
    if (temp_block_)
        alloc_.free_bit_block(temp_block_);
}

/**
    Utility function to process operation using temp vector
    @internal
 */
template<class BV>
typename BV::size_type process_operation(BV& bv,
                                         BV& bv_tmp,
                                         bm::set_operation op)
{
    typename BV::size_type count = 0;
    switch (op)
    {
    case set_AND:
        bv.bit_and(bv_tmp, BV::opt_compress);
        break;
    case set_ASSIGN:
        bv.swap(bv_tmp);
        break;
    case set_OR:
        bv.bit_or(bv_tmp);
        break;
    case set_SUB:
        bv.bit_sub(bv_tmp);
        break;
    case set_XOR:
        bv.bit_xor(bv_tmp);
        break;
    case set_COUNT: case set_COUNT_B:
        count = bv_tmp.count();
        break;
    case set_COUNT_A:
        count = bv.count();
        break;
    case set_COUNT_AND:
        count = bm::count_and(bv, bv_tmp);
        break;
    case set_COUNT_XOR:
        count = bm::count_xor(bv, bv_tmp);
        break;
    case set_COUNT_OR:
        count = bm::count_or(bv, bv_tmp);
        break;
    case set_COUNT_SUB_AB:
        count = bm::count_sub(bv, bv_tmp);
        break;
    case set_COUNT_SUB_BA:
        count = bm::count_sub(bv_tmp, bv);
        break;
    case set_END:
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            //throw std::logic_error(err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch

    return count;
}


template<class BV>
typename operation_deserializer<BV>::size_type
operation_deserializer<BV>::deserialize_xor(bvector_type&       bv,
                                            const unsigned char* buf,
                                            set_operation        op,
                                            bool                 /*exit_on_one*/)
{
    if (op == set_OR)
    {
        bm::deserialize(bv, buf, temp_block_, ref_vect_);
        return 0;
    }
    bvector_type bv_tmp;
    bm::deserialize(bv_tmp, buf, temp_block_, ref_vect_);
    return deserialize_xor(bv, bv_tmp, op);
}

template<class BV>
void operation_deserializer<BV>::deserialize_xor_range(
                                bvector_type&       bv,
                                const unsigned char* buf,
                                size_type            idx_from,
                                size_type            idx_to)
{
    bv.clear();

    ByteOrder bo_current = globals<true>::byte_order();

    bm::decoder dec(buf);
    unsigned char header_flag = dec.get_8();
    ByteOrder bo = bo_current;
    if (!(header_flag & BM_HM_NO_BO))
    {
        bo = (bm::ByteOrder) dec.get_8();
    }

    if (bo_current == bo)
    {
        de_.set_ref_vectors(ref_vect_);
        de_.set_range(idx_from, idx_to);
        de_.deserialize(bv, buf);
        de_.reset();
    }
    else
    {
        switch (bo_current)
        {
        case BigEndian:
            {
                deserializer<BV, bm::decoder_big_endian> deserial;
                deserial.set_ref_vectors(ref_vect_);
                deserial.set_range(idx_from, idx_to);
                deserial.deserialize(bv, buf);
            }
            break;
        case LittleEndian:
            {
                deserializer<BV, bm::decoder_little_endian> deserial;
                deserial.set_ref_vectors(ref_vect_);
                deserial.set_range(idx_from, idx_to);
                deserial.deserialize(bv, buf);
            }
            break;
        default:
            BM_ASSERT(0);
        };
    }
    bv.keep_range_no_check(idx_from, idx_to);
}



template<class BV>
typename operation_deserializer<BV>::size_type
operation_deserializer<BV>::deserialize_xor(bvector_type&       bv,
                                            bvector_type&       bv_tmp,
                                            set_operation       op)
{
    size_type count = 0;
    switch (op)
    {
    case set_AND:
        bv.bit_and(bv_tmp, bvector_type::opt_compress);
        break;
    case set_ASSIGN:
        bv.swap(bv_tmp);
        break;
    case set_SUB:
        bv -= bv_tmp;
        break;
    case set_XOR:
        bv ^= bv_tmp;
        break;
    case set_COUNT: case set_COUNT_B:
        count = bv_tmp.count();
        break;
    case set_COUNT_A:
        return bv.count();
    case set_COUNT_AND:
        count += bm::count_and(bv, bv_tmp);
        break;
    case set_COUNT_XOR:
        count += bm::count_xor(bv, bv_tmp);
        break;
    case set_COUNT_OR:
        count += bm::count_or(bv, bv_tmp);
        break;
    case set_COUNT_SUB_AB:
        count += bm::count_sub(bv, bv_tmp);
        break;
    case set_COUNT_SUB_BA:
        count += bm::count_sub(bv_tmp, bv);
        break;
    case set_END:
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error("BM: serialization error");
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch

    return count;
}



template<class BV>
typename operation_deserializer<BV>::size_type
operation_deserializer<BV>::deserialize(bvector_type&        bv,
                                        const unsigned char* buf, 
                                        set_operation        op,
                                        bool                 exit_on_one)
{
    ByteOrder bo_current = globals<true>::byte_order();
    bm::decoder dec(buf);
    unsigned char header_flag = dec.get_8();

    if (header_flag & BM_HM_HXOR) // XOR compression
    {
        BM_ASSERT(ref_vect_); // reference vector must be set
        return deserialize_xor(bv, buf, op, exit_on_one);
    }

    ByteOrder bo = bo_current;
    if (!(header_flag & BM_HM_NO_BO))
        bo = (bm::ByteOrder) dec.get_8();

    if (op == bm::set_ASSIGN)
    {
        bv.clear(true);
        op = bm::set_OR;
    }

    if (header_flag & BM_HM_SPARSE)
    {
        size_type count = 0;
        if (bo_current == bo)
        {
            if (op == bm::set_OR)
            {
                de_.reset();
                de_.deserialize(bv, buf);
            }
            else
            {
                bv_tmp_.clear(true);
                bv_tmp_.set_new_blocks_strat(bm::BM_GAP);
                de_.reset();
                de_.deserialize(bv_tmp_, buf);
                count = bm::process_operation(bv, bv_tmp_, op);
            }
        }
        else
        {
            // TODO: implement byte-order aware sparse deserialization
            BM_ASSERT(0);
        }
        return count;
    }

    if (bo_current == bo)
    {
        serial_stream_current ss(buf);
        return it_d_.deserialize(bv, ss, temp_block_, op, exit_on_one);
    }
    switch (bo_current) 
    {
    case BigEndian:
        {
            serial_stream_be ss(buf);
            return it_d_be_.deserialize(bv, ss, temp_block_, op, exit_on_one);
        }
    case LittleEndian:
        {
            serial_stream_le ss(buf);
            return it_d_le_.deserialize(bv, ss, temp_block_, op, exit_on_one);
        }
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error("BM::platform error: unknown endianness");
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    };
}

template<class BV>
void operation_deserializer<BV>::deserialize_range(
                       bvector_type&        bv,
                       const unsigned char* buf,
                       size_type            idx_from,
                       size_type            idx_to)
{
    ByteOrder bo_current = globals<true>::byte_order();
    bm::decoder dec(buf);
    unsigned char header_flag = dec.get_8();
    ByteOrder bo = bo_current;
    if (!(header_flag & BM_HM_NO_BO))
        bo = (bm::ByteOrder) dec.get_8();

    // check if it is empty fresh vector, set the range then
    //
    blocks_manager_type& bman = bv.get_blocks_manager();
    if (!bman.is_init())
        bv.set_range(idx_from, idx_to);
    else
    {} // assume that the target range set outside the call


    if (header_flag & BM_HM_HXOR) // XOR compression
    {
        BM_ASSERT(ref_vect_);
        bvector_type bv_tmp;
        deserialize_xor_range(bv_tmp, buf, idx_from, idx_to);
        if (bv.any())
        {
            bv.bit_and(bv_tmp, bvector_type::opt_compress);
        }
        else
        {
            bv.swap(bv_tmp);
        }
        return;
    }

    if (header_flag & BM_HM_SPARSE)
    {
        if (bo_current == bo)
        {
            bv_tmp_.clear(true);
            bv_tmp_.set_new_blocks_strat(bm::BM_GAP);
            de_.reset();
            de_.set_range(idx_from, idx_to);
            de_.deserialize(bv_tmp_, buf);
            de_.reset();

            if (bv.any())
            {
                bv.bit_and(bv_tmp_, bvector_type::opt_compress);
            }
            else
            {
                bv.swap(bv_tmp_);
            }

        }
        else
        {
            // TODO: implement byte-order aware sparse deserialization
            BM_ASSERT(0);
        }
        return;
    }

    const bm::set_operation op = bm::set_AND;

    if (bo_current == bo)
    {
        serial_stream_current ss(buf);
        it_d_.set_range(idx_from, idx_to);
        it_d_.deserialize(bv, ss, temp_block_, op, false);
        it_d_.unset_range();
        return;
    }
    switch (bo_current)
    {
    case BigEndian:
        {
            serial_stream_be ss(buf);
            it_d_be_.set_range(idx_from, idx_to);
            it_d_be_.deserialize(bv, ss, temp_block_, op, false);
            it_d_be_.unset_range();
            return;
        }
    case LittleEndian:
        {
            serial_stream_le ss(buf);
            it_d_le_.set_range(idx_from, idx_to);
            it_d_le_.deserialize(bv, ss, temp_block_, op, false);
            it_d_le_.unset_range();
            return;
        }
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error("BM::platform error: unknown endianness");
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    };
    return;
}



// ------------------------------------------------------------------

template<class BV, class SerialIterator>
void iterator_deserializer<BV, SerialIterator>::set_range(
                                        size_type from, size_type to)
{
    is_range_set_ = true;
    nb_range_from_ = (from >> bm::set_block_shift);
    nb_range_to_ = (to >> bm::set_block_shift);
}


template<class BV, class SerialIterator>
void iterator_deserializer<BV, SerialIterator>::load_id_list(
                                            bvector_type&         bv, 
                                            serial_iterator_type& sit,
                                            unsigned              id_count,
                                            bool                  set_clear)
{
    const unsigned win_size = 64;
    bm::id_t id_buffer[win_size+1];

    if (set_clear)  // set bits
    {
        for (unsigned i = 0; i <= id_count;)
        {
            unsigned j;
            for (j = 0; j < win_size && i <= id_count; ++j, ++i) 
            {
                id_buffer[j] = sit.get_id();
                sit.next();
            } // for j
            bm::combine_or(bv, id_buffer, id_buffer + j);
        } // for i
    } 
    else // clear bits
    {
        for (unsigned i = 0; i <= id_count;)
        {
            unsigned j;
            for (j = 0; j < win_size && i <= id_count; ++j, ++i) 
            {
                id_buffer[j] = sit.get_id();
                sit.next();
            } // for j
            bm::combine_sub(bv, id_buffer, id_buffer + j);
        } // for i
    }
}

template<class BV, class SerialIterator>
typename iterator_deserializer<BV, SerialIterator>::size_type
iterator_deserializer<BV, SerialIterator>::finalize_target_vector(
                                        blocks_manager_type& bman,
                                        set_operation        op,
                                        size_type            bv_block_idx)
{
    size_type count = 0;
    switch (op)
    {
    case set_OR:    case set_SUB:     case set_XOR:
    case set_COUNT: case set_COUNT_B: case set_COUNT_AND:
    case set_COUNT_SUB_BA:
        // nothing to do
        break;
    case set_ASSIGN: case set_AND:
        {
            block_idx_type nblock_last = (bm::id_max >> bm::set_block_shift);
            if (bv_block_idx <= nblock_last)
                bman.set_all_zero(bv_block_idx, nblock_last); // clear the tail
        }
        break;
    case set_COUNT_A: case set_COUNT_OR: case set_COUNT_XOR:
    case set_COUNT_SUB_AB:
        // count bits in the target vector
        {
            unsigned i, j;
            bm::get_block_coord(bv_block_idx, i, j);
            bm::word_t*** blk_root = bman.top_blocks_root();
            unsigned top_size = bman.top_block_size();
            for (;i < top_size; ++i)
            {
                bm::word_t** blk_blk = blk_root[i];
                if (blk_blk == 0) 
                {
                    bv_block_idx+=bm::set_sub_array_size-j;
                    j = 0;
                    continue;
                }
                // TODO: optimize for FULL top level
                for (; j < bm::set_sub_array_size; ++j, ++bv_block_idx)
                {
                    if (blk_blk[j])
                        count += bman.block_bitcount(blk_blk[j]);
                } // for j
                j = 0;
            } // for i
        }
        break;
    case set_END:
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    }
    return count;
}

template<class BV, class SerialIterator>
typename iterator_deserializer<BV, SerialIterator>::size_type
iterator_deserializer<BV, SerialIterator>::process_id_list(
                                    bvector_type&         bv, 
                                    serial_iterator_type& sit,
                                    set_operation         op)
{
    size_type count = 0;
    unsigned id_count = sit.get_id_count();
    bool set_clear = true;
    switch (op)
    {
    case set_AND:
        {
            // TODO: use some more optimal algorithm without temp vector
            BV bv_tmp(BM_GAP);
            load_id_list(bv_tmp, sit, id_count, true);
            bv &= bv_tmp;
        }
        break;
    case set_ASSIGN:
        BM_ASSERT(0);
        BM_FALLTHROUGH;
        // fall through
    case set_OR:
        set_clear = true;
        BM_FALLTHROUGH;
        // fall through
    case set_SUB:
        load_id_list(bv, sit, id_count, set_clear);
        break;
    case set_XOR:
        for (unsigned i = 0; i < id_count; ++i)
        {
            bm::id_t id = sit.get_id();
            bv[id] ^= true;
            sit.next();
        } // for
        break;
    case set_COUNT: case set_COUNT_B:
        for (unsigned i = 0; i < id_count; ++i)
        {
            /* bm::id_t id = */ sit.get_id();
            ++count;
            sit.next();
        } // for
        break;
    case set_COUNT_A:
        return bv.count();
    case set_COUNT_AND:
        for (size_type i = 0; i < id_count; ++i)
        {
            bm::id_t id = sit.get_id();
            count += bv.get_bit(id);
            sit.next();
        } // for
        break;
    case set_COUNT_XOR:
        {
            // TODO: get rid of the temp vector
            BV bv_tmp(BM_GAP);
            load_id_list(bv_tmp, sit, id_count, true);
            count += bm::count_xor(bv, bv_tmp);
        }
        break;
    case set_COUNT_OR:
        {
            // TODO: get rid of the temp. vector
            BV bv_tmp(BM_GAP);
            load_id_list(bv_tmp, sit, id_count, true);
            count += bm::count_or(bv, bv_tmp);
        }
        break;
    case set_COUNT_SUB_AB:
        {
            // TODO: get rid of the temp. vector
            BV bv_tmp(bv);
            load_id_list(bv_tmp, sit, id_count, false);
            count += bv_tmp.count();
        }
        break;
    case set_COUNT_SUB_BA:
        {
            BV bv_tmp(BM_GAP);
            load_id_list(bv_tmp, sit, id_count, true);
            count += bm::count_sub(bv_tmp, bv);        
        }
        break;
    case set_END:
    default:
        BM_ASSERT(0);
        #ifndef BM_NO_STL
            throw std::logic_error(err_msg());
        #else
            BM_THROW(BM_ERR_SERIALFORMAT);
        #endif
    } // switch

    return count;
}


template<class BV, class SerialIterator>
typename iterator_deserializer<BV, SerialIterator>::size_type
iterator_deserializer<BV, SerialIterator>::deserialize(
                                       bvector_type&         bv, 
                                       serial_iterator_type& sit, 
                                       bm::word_t*           temp_block,
                                       set_operation         op,
                                       bool                  exit_on_one)
{
    BM_ASSERT(temp_block);

    size_type count = 0;
    gap_word_t   gap_temp_block[bm::gap_equiv_len * 4];
    gap_temp_block[0] = 0;

    blocks_manager_type& bman = bv.get_blocks_manager();
    if (!bman.is_init())
        bman.init_tree();

    if (sit.bv_size() && (sit.bv_size() > bv.size()))
        bv.resize(sit.bv_size());

    typename serial_iterator_type::iterator_state state;
    state = sit.get_state();
    if (state == serial_iterator_type::e_list_ids)
    {
        count = process_id_list(bv, sit, op);
        return count;
    }

    size_type bv_block_idx = 0;
    size_type empty_op_cnt = 0; // counter for empty target operations
    
    for (;1;)
    {
        bm::set_operation sop = op;
        if (sit.is_eof()) // argument stream ended
        {
            count += finalize_target_vector(bman, op, bv_block_idx);
            return count;
        }

        state = sit.state();
        switch (state)
        {
        case serial_iterator_type::e_blocks:
            sit.next(); 

            // try to skip forward
            //
            if (is_range_set_ && (bv_block_idx < nb_range_from_))
            {
                // TODO: use saved bookmark block-idx
                bool skip_flag = sit.try_skip(bv_block_idx, nb_range_from_);
                if (skip_flag)
                {
                    bv_block_idx = sit.block_idx();
                    BM_ASSERT(bv_block_idx <= nb_range_from_);
                    BM_ASSERT(sit.state() == serial_iterator_type::e_blocks);
                }
            }
            continue;
        case serial_iterator_type::e_bit_block:
            {
            BM_ASSERT(sit.block_idx() == bv_block_idx);
            unsigned i0, j0;
            bm::get_block_coord(bv_block_idx, i0, j0);
            bm::word_t* blk = bman.get_block_ptr(i0, j0);
            if (!blk)
            {
                switch (op)
                {
                case set_AND:          case set_SUB: case set_COUNT_AND:
                case set_COUNT_SUB_AB: case set_COUNT_A:
                    // one arg is 0, so the operation gives us zero
                    // all we need to do is to seek the input stream
                    sop = set_ASSIGN;
                    break;

                case set_OR: case set_XOR: case set_ASSIGN:
                    blk = bman.make_bit_block(bv_block_idx);
                    break;

                case set_COUNT:        case set_COUNT_XOR: case set_COUNT_OR:
                case set_COUNT_SUB_BA: case set_COUNT_B:
                    // first arg is not required (should work as is)
                    sop = set_COUNT;
                    break;

                case set_END:
                default:
                    BM_ASSERT(0);
                    #ifndef BM_NO_STL
                        throw std::logic_error(err_msg());
                    #else
                        BM_THROW(BM_ERR_SERIALFORMAT);
                    #endif
                }
            }
            else // block exists
            {
                int is_gap = BM_IS_GAP(blk);
                if (is_gap || IS_FULL_BLOCK(blk))
                {
                    if (IS_FULL_BLOCK(blk) && is_const_set_operation(op))
                    {
                        blk = FULL_BLOCK_REAL_ADDR;
                    }
                    else
                    {
                        // TODO: make sure const operations do not 
                        // deoptimize GAP blocks
                        blk = bman.deoptimize_block(bv_block_idx);
                    }
                }
            }

            // 2 bit-blocks recombination
            unsigned c = sit.get_bit_block(blk, temp_block, sop);
            count += c;
			if (exit_on_one && count) // early exit
				return count;
            switch (op) // target block optimization for non-const operations
            {
            case set_AND: case set_SUB: case set_XOR: case set_OR:
                bman.optimize_bit_block(i0, j0, bvector_type::opt_compress);
                break;
            default: break;
            } // switch

            }
            break;

        case serial_iterator_type::e_zero_blocks:
            {
            BM_ASSERT(bv_block_idx == sit.block_idx());
            
            switch (op)
            {
            case set_ASSIGN: // nothing to do to rewind fwd
            case set_SUB: case set_COUNT_AND:    case set_OR:
            case set_XOR: case set_COUNT_SUB_BA: case set_COUNT_B:
                bv_block_idx = sit.skip_mono_blocks();
                continue;
                
            case set_AND: // clear the range
                {
                    size_type nb_start = bv_block_idx;
                    bv_block_idx = sit.skip_mono_blocks();
                    bman.set_all_zero(nb_start, bv_block_idx-1);
                }
                continue;
            case set_END:
            default:
                break;
            } // switch op

            
            unsigned i0, j0;
            bm::get_block_coord(bv_block_idx, i0, j0);
            bm::word_t* blk = bman.get_block_ptr(i0, j0);

            sit.next();

            if (blk)
            {
                switch (op)
                {
                case set_AND: case set_ASSIGN:
                    // the result is 0
                    //blk =
                    bman.zero_block(bv_block_idx);
                    break;

                case set_SUB: case set_COUNT_AND:    case set_OR:
                case set_XOR: case set_COUNT_SUB_BA: case set_COUNT_B:
                    // nothing to do
                    break;
                
                case set_COUNT_SUB_AB: case set_COUNT_A: case set_COUNT_OR:
                case set_COUNT:        case set_COUNT_XOR:
                    // valid bit block recombined with 0 block
                    // results with same block data
                    // all we need is to bitcount bv block
                    {
                    count += blk ? bman.block_bitcount(blk) : 0;
					if (exit_on_one && count) // early exit
						return count;
                    }
                    break;
                case set_END:
                default:
                    BM_ASSERT(0);
                } // switch op
            } // if blk
            }
            break;

        case serial_iterator_type::e_one_blocks:
            {
            BM_ASSERT(bv_block_idx == sit.block_idx());
            unsigned i0, j0;
            bm::get_block_coord(bv_block_idx, i0, j0);
            bm::word_t* blk = bman.get_block_ptr(i0, j0);

            sit.next();

            switch (op)
            {
            case set_OR: case set_ASSIGN:
                bman.set_block_all_set(bv_block_idx);
                break;
            case set_COUNT_OR: case set_COUNT_B: case set_COUNT:
                // block becomes all set
                count += bm::bits_in_block;
                break;
            case set_SUB:
                //blk =
                bman.zero_block(bv_block_idx);
                break;
            case set_COUNT_SUB_AB: case set_AND:
                // nothing to do, check maybe nothing else to do at all
                if (++empty_op_cnt > 64)
                {
                    size_type last_id;
                    bool b = bv.find_reverse(last_id);
                    if (!b)
                        return count;
                    size_type last_nb = (last_id >> bm::set_block_shift);
                    if (last_nb < bv_block_idx)
                        return count; // early exit, nothing to do here
                    empty_op_cnt = 0;
                }
                break;
            case set_COUNT_AND: case set_COUNT_A:
                count += blk ? bman.block_bitcount(blk) : 0;
                break;
            default:
                if (blk)
                {
                    switch (op)
                    {
                    case set_XOR:
                        blk = bman.deoptimize_block(bv_block_idx);
                        bm::bit_block_xor(blk, FULL_BLOCK_REAL_ADDR);
                        break;
                    case set_COUNT_XOR:
                        {
                        count += 
                            combine_count_operation_with_block(
                                                blk,
                                                FULL_BLOCK_REAL_ADDR,
                                                bm::COUNT_XOR);
                        }
                        break;
                    case set_COUNT_SUB_BA:
                        {
                        count += 
                            combine_count_operation_with_block(
                                                blk,
                                                FULL_BLOCK_REAL_ADDR,
                                                bm::COUNT_SUB_BA);
                        }
                        break;
                    default:
                        BM_ASSERT(0);
                    } // switch
                }
                else // blk == 0 
                {
                    switch (op)
                    {
                    case set_XOR:
                        // 0 XOR 1 = 1
                        bman.set_block_all_set(bv_block_idx);
                        break;
                    case set_COUNT_XOR:
                        count += bm::bits_in_block;
                        break;
                    case set_COUNT_SUB_BA:
                        // 1 - 0 = 1
                        count += bm::bits_in_block;
                        break;
                    default:
                        break;
                    } // switch
                } // else
            } // switch
            if (exit_on_one && count) // early exit
				   return count;
            }
            break;

        case serial_iterator_type::e_gap_block:
            {
            BM_ASSERT(bv_block_idx == sit.block_idx());

            unsigned i0, j0;
            bm::get_block_coord(bv_block_idx, i0, j0);
            const bm::word_t* blk = bman.get_block(i0, j0);

            sit.get_gap_block(gap_temp_block);

            unsigned len = gap_length(gap_temp_block);
            int level = gap_calc_level(len, bman.glen());
            --len;

            bool const_op = is_const_set_operation(op);
            if (const_op)
            {
                distance_metric metric = operation2metric(op);
                bm::word_t* gptr = (bm::word_t*)gap_temp_block;
                BMSET_PTRGAP(gptr);
                unsigned c = 
                    combine_count_operation_with_block(
                                        blk,
                                        gptr,
                                        metric);
                count += c;
                if (exit_on_one && count) // early exit
				    return count;

            }
            else // non-const set operation
            {
                if ((sop == set_ASSIGN) && blk) // target block override
                {
                    bman.zero_block(bv_block_idx);
                    sop = set_OR;
                }
                if (blk == 0) // target block not found
                {
                    switch (sop)
                    {
                    case set_AND: case set_SUB:
                        break;
                    case set_OR: case set_XOR: case set_ASSIGN:
                        bman.set_gap_block(
                            bv_block_idx, gap_temp_block, level);
                        break;
                    
                    default:
                        BM_ASSERT(0);
                    } // switch
                }
                else  // target block exists
                {
                    bm::operation bop = bm::setop2op(op);
                    if (level == -1) // too big to GAP
                    {
                        gap_convert_to_bitset(temp_block, gap_temp_block);
                        bv.combine_operation_with_block(bv_block_idx, 
                                                        temp_block, 
                                                        0, // BIT
                                                        bop);
                    }
                    else // GAP fits
                    {
                        set_gap_level(gap_temp_block, level);
                        bv.combine_operation_with_block(
                                                bv_block_idx, 
                                                (bm::word_t*)gap_temp_block, 
                                                1,  // GAP
                                                bop);
                    }
                }
                if (exit_on_one) 
                {
                    bm::get_block_coord(bv_block_idx, i0, j0);
                    blk = bman.get_block_ptr(i0, j0);
                    if (blk)
                    {
                        bool z = bm::check_block_zero(blk, true/*deep check*/);
                        if (!z) 
                            return 1;
                    } 
                } // if exit_on_one

            } // if else non-const op
            }
            break;

        default:
            BM_ASSERT(0);
            #ifndef BM_NO_STL
                throw std::logic_error(err_msg());
            #else
                BM_THROW(BM_ERR_SERIALFORMAT);
            #endif
        } // switch

        ++bv_block_idx;
        BM_ASSERT(bv_block_idx);

        if (is_range_set_ && (bv_block_idx > nb_range_to_))
            break;

    } // for (deserialization)

    return count;
}



} // namespace bm


#ifdef _MSC_VER
#pragma warning( pop )
#endif


#endif
