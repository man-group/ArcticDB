#ifndef BMTRANS__H__INCLUDED__
#define BMTRANS__H__INCLUDED__
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

/*! \file bmtrans.h
    \brief Utilities for bit transposition (internal) (experimental!)
*/

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4311 4312 4127)
#endif

#include "bmdef.h"

namespace bm
{

/**
    Mini-matrix for bit transposition purposes
    @internal
*/
template<typename T, unsigned ROWS, unsigned COLS>
struct tmatrix
{
    typedef T value_type;
    
    T BM_VECT_ALIGN value[ROWS][COLS] BM_VECT_ALIGN_ATTR;

    enum params
    {
        n_rows = ROWS,
        n_columns = COLS
    };


    /// Row characteristics for transposed matrix
    struct rstat
    {
        unsigned               bit_count;
        unsigned               gap_count;
        bm::set_representation best_rep;
    };

    static unsigned rows() BMNOEXCEPT { return ROWS; }
    static unsigned cols() BMNOEXCEPT { return COLS; }

    const T* row(unsigned row_idx) const BMNOEXCEPT
    {
        BM_ASSERT(row_idx < ROWS);
        return value[row_idx];
    }
    T* row(unsigned row_idx) BMNOEXCEPT
    {
        BM_ASSERT(row_idx < ROWS);
        return value[row_idx];
    }
};


/*!
    Bit array grabber - template specitialization for various basic types    
    @internal
*/
template<typename T, unsigned BPC>
struct bit_grabber
{
    static
    unsigned get(const T*, unsigned)
    {
        BM_ASSERT(0); return 0;
    }
};

template<>
struct bit_grabber<unsigned, 32>
{
    static
    unsigned get(const unsigned* arr, unsigned j)
    {
        return  (((arr[0] >> j) & 1) << 0) |
                (((arr[1] >> j) & 1) << 1) |
                (((arr[2] >> j) & 1) << 2) |
                (((arr[3] >> j) & 1) << 3) |
                (((arr[4] >> j) & 1) << 4) |
                (((arr[5] >> j) & 1) << 5) |
                (((arr[6] >> j) & 1) << 6) |
                (((arr[7] >> j) & 1) << 7) |
                (((arr[8] >> j) & 1) << 8) |
                (((arr[9] >> j) & 1) << 9) |
                (((arr[10]>> j) & 1) << 10)|
                (((arr[11]>> j) & 1) << 11)|
                (((arr[12]>> j) & 1) << 12)|
                (((arr[13]>> j) & 1) << 13)|
                (((arr[14]>> j) & 1) << 14)|
                (((arr[15]>> j) & 1) << 15)|
                (((arr[16]>> j) & 1) << 16)|
                (((arr[17]>> j) & 1) << 17)|
                (((arr[18]>> j) & 1) << 18)|
                (((arr[19]>> j) & 1) << 19)|
                (((arr[20]>> j) & 1) << 20)|
                (((arr[21]>> j) & 1) << 21)|
                (((arr[22]>> j) & 1) << 22)|
                (((arr[23]>> j) & 1) << 23)|
                (((arr[24]>> j) & 1) << 24)|
                (((arr[25]>> j) & 1) << 25)|
                (((arr[26]>> j) & 1) << 26)|
                (((arr[27]>> j) & 1) << 27)|
                (((arr[28]>> j) & 1) << 28)|
                (((arr[29]>> j) & 1) << 29)|
                (((arr[30]>> j) & 1) << 30)|
                (((arr[31]>> j) & 1) << 31);    
    }
};

template<>
struct bit_grabber<unsigned short, 16>
{
    static
    unsigned get(const unsigned short* arr, unsigned j)
    {
        return  (((arr[0] >> j) & 1u) << 0u) |
                (((arr[1] >> j) & 1u) << 1u) |
                (((arr[2] >> j) & 1u) << 2u) |
                (((arr[3] >> j) & 1u) << 3u) |
                (((arr[4] >> j) & 1u) << 4u) |
                (((arr[5] >> j) & 1u) << 5u) |
                (((arr[6] >> j) & 1u) << 6u) |
                (((arr[7] >> j) & 1u) << 7u) |
                (((arr[8] >> j) & 1u) << 8u) |
                (((arr[9] >> j) & 1u) << 9u) |
                (((arr[10]>> j) & 1u) << 10u)|
                (((arr[11]>> j) & 1u) << 11u)|
                (((arr[12]>> j) & 1u) << 12u)|
                (((arr[13]>> j) & 1u) << 13u)|
                (((arr[14]>> j) & 1u) << 14u)|
                (((arr[15]>> j) & 1u) << 15u);
    }
};


template<>
struct bit_grabber<unsigned char, 8>
{
    static
    unsigned get(const unsigned char* arr, unsigned j)
    {
        return  unsigned(
                (((arr[0] >> j) & 1) << 0) |
                (((arr[1] >> j) & 1) << 1) |
                (((arr[2] >> j) & 1) << 2) |
                (((arr[3] >> j) & 1) << 3) |
                (((arr[4] >> j) & 1) << 4) |
                (((arr[5] >> j) & 1) << 5) |
                (((arr[6] >> j) & 1) << 6) |
                (((arr[7] >> j) & 1) << 7));
    }
};


/*!
    Bit transpose matrix grabber - template specitialization for various basic types    
    @internal
*/
template<typename T, unsigned BPC, unsigned BPS>
struct bit_trans_grabber
{
    static
    T get(const T  tmatrix[BPC][BPS], unsigned i, unsigned j)
    {
        T w = 0;
        
            // Next code hopes that compiler will completely
            // eliminate ifs (all conditions are known at compile time)
            //    ( typically C++ compilers are smart to do that)
            
            // 8-bit (minimum)
            w |=
                (((tmatrix[0][i] >> j) & 1) << 0) |
                (((tmatrix[1][i] >> j) & 1) << 1) |
                (((tmatrix[2][i] >> j) & 1) << 2) |
                (((tmatrix[3][i] >> j) & 1) << 3) |
                (((tmatrix[4][i] >> j) & 1) << 4) |
                (((tmatrix[5][i] >> j) & 1) << 5) |
                (((tmatrix[6][i] >> j) & 1) << 6) |
                (((tmatrix[7][i] >> j) & 1) << 7);
                
            // 16-bit
            if (BPC > 8) 
            {
                w |=
                (((tmatrix[8][i] >> j) & 1) << 8) |
                (((tmatrix[9][i] >> j) & 1) << 9) |
                (((tmatrix[10][i] >> j) & 1) << 10) |
                (((tmatrix[11][i] >> j) & 1) << 11) |
                (((tmatrix[12][i] >> j) & 1) << 12) |
                (((tmatrix[13][i] >> j) & 1) << 13) |
                (((tmatrix[14][i] >> j) & 1) << 14) |
                (((tmatrix[15][i] >> j) & 1) << 15);
            }
            
            // 32-bit
            if (BPC > 16)
            {
                w |= 
                (((tmatrix[16][i] >> j) & 1) << 16) |                
                (((tmatrix[17][i] >> j) & 1) << 17) |
                (((tmatrix[18][i] >> j) & 1) << 18) |
                (((tmatrix[19][i] >> j) & 1) << 19) |
                (((tmatrix[20][i] >> j) & 1) << 20) |
                (((tmatrix[21][i] >> j) & 1) << 21) |
                (((tmatrix[22][i] >> j) & 1) << 22) |
                (((tmatrix[23][i] >> j) & 1) << 23) |
                (((tmatrix[24][i] >> j) & 1) << 24) |
                (((tmatrix[25][i] >> j) & 1) << 25) |
                (((tmatrix[26][i] >> j) & 1) << 26) |
                (((tmatrix[27][i] >> j) & 1) << 27) |
                (((tmatrix[28][i] >> j) & 1) << 28) |
                (((tmatrix[29][i] >> j) & 1) << 29) |
                (((tmatrix[30][i] >> j) & 1) << 30) |
                (((tmatrix[31][i] >> j) & 1) << 31); 
            }   
        return w;
    }
};



/**
    Generic bit-array transposition function
    T - array type (any int)
    BPC - bit plane count
    BPS - bit plane size
    
    \param arr      - source array start
    \param arr_size - source array size
    \param tmatrix   - destination bit matrix
        
*/
template<typename T, unsigned BPC, unsigned BPS>
void vect_bit_transpose(const T* arr, 
                        unsigned arr_size,
                        T        tmatrix[BPC][BPS])
{
    BM_ASSERT(sizeof(T)*8 == BPC);

    unsigned col = 0;
    for (unsigned i = 0; i < arr_size; 
                         i+=BPC, arr+=BPC, 
                         ++col)
    {
        for (unsigned j = 0; j < BPC; ++j)
        {
            unsigned w = 
                bm::bit_grabber<T, BPC>::get(arr, j);
            T* row = tmatrix[j];
            row[col] = (T)w;
        } // for j
    } // for i
}


/**
    Restore bit array from the transposition matrix
    T - array type (any int)
    BPC - bit plane count
    BPS - bit plane size
    
    \param arr       - dest array
    \param tmatrix   - source bit-slice matrix
        
*/
template<typename T, unsigned BPC, unsigned BPS>
void vect_bit_trestore(const T  tmatrix[BPC][BPS], 
                             T* arr)
{
    unsigned col = 0;
    for (unsigned i = 0; i < BPS; ++i)
    {
        for (unsigned j = 0; j < BPC; ++j, ++col) 
        {
            arr[col] = 
                bm::bit_trans_grabber<T, BPC, BPS>::get(tmatrix, i, j);
        } // for j
    } // for i    
}



/*!
    \brief Compute pairwise Row x Row Humming distances on planes(rows) of
           the transposed bit block
   \param tmatrix - bit-block transposition matrix (bit-planes)
   \param distance - pairwise NxN Humming distance matrix (diagonal is popcnt)

   @ingroup bitfunc
*/
template<typename T, unsigned BPC, unsigned BPS>
void tmatrix_distance(const T  tmatrix[BPC][BPS], 
                      unsigned distance[BPC][BPC])
{                      
    for (unsigned i = 0; i < BPC; ++i)
    {
        const T* r1 = tmatrix[i];
        distance[i][i] =
            bm::bit_block_count((bm::word_t*)r1);

        for (unsigned j = i + 1; j < BPC; ++j)
        {
            r1 = tmatrix[i];
            unsigned count = 0;

            {
                const T* r2 = tmatrix[i];
                const T* r2_end = r2 + BPS;
                const bm::word_t* r3 = (bm::word_t*)(tmatrix[j]);
                do {
                    count += bm::word_bitcount(r2[0] ^ r3[0]);
                    count += bm::word_bitcount(r2[1] ^ r3[1]);
                    count += bm::word_bitcount(r2[2] ^ r3[2]);
                    count += bm::word_bitcount(r2[3] ^ r3[3]);
                    r2 += 4;
                    r3 += 4;
                } while (r2 < r2_end);
            }
            distance[i][j] = count;
        } // for j
    } // for i
}



const unsigned char ibpc_uncompr = 0; ///!< plane uncompressed
const unsigned char ibpc_all_zero= 1; ///!< plane ALL ZERO
const unsigned char ibpc_all_one = 2; ///!< plane ALL ONE
const unsigned char ibpc_equiv   = 3; ///!< plane is equal to plane M
const unsigned char ibpc_close   = 4; ///!< plane is close to plane M

const unsigned char ibpc_end = 8; ///!< ibpc limiter


/*!
    \brief Make a compression descriptor vector for bit-planes

    \param distance - pairwise distance matrix
    \param pc_vector - OUT compression descriptor vector
    <pre>
        pc_vector[] format:
            each element (pc_vector[i]) describes the plane compression:
                first 3 bits - compression code:
                    0 - plane uncompressed
                    1 - plane is ALL ZERO (000000...)
                    2 - plane is ALL ONE  (111111...)
                    3 - plane is equal to another plane J (5 high bits (max 31))
                    4 - plane is close (but not equal) to plane J
                next 5 bits - number of plane used as a XOR expression
                 ( compression codes: 3,4 )
    </pre>                    
    
    @ingroup bitfunc
*/
template<typename T, unsigned BPC, unsigned BPS>
void bit_iblock_make_pcv(
      const unsigned  distance[BPC][BPC],
      unsigned char*  pc_vector)
{
    BM_ASSERT(pc_vector);

    for (unsigned i = 0; i < BPC; ++i)
    {
        unsigned char pc = ibpc_uncompr; 
        unsigned row_bitcount = distance[i][i];
        
        const unsigned total_possible_max = sizeof(T)*8*BPS;
        switch (row_bitcount)
        {
        case 0:
            pc_vector[i] = ibpc_all_zero; 
            continue;
        case total_possible_max:
            pc_vector[i] = ibpc_all_one; 
            continue;
        default:
            break;
        }
        
        // Dense-populated set, leave it as is
        if (row_bitcount >  total_possible_max/2)
        {
            pc_vector[i] = ibpc_uncompr;
            continue;
        }
        
        // scan for the closest neighbor
        //
        unsigned rmin = ~0u;
        unsigned rmin_idx = 0;
        for (unsigned j = i + 1; j < BPC; ++j)
        {
            unsigned d = distance[i][j];
            if (d < rmin) // new minimum - closest plane
            {
                if (d == 0) // plane is complete duplicate of j
                {
                    pc = (unsigned char)(ibpc_equiv | (j << 3));
                    break;
                }
                rmin = d; rmin_idx = j;
            }
        } // for j
        
        if ((pc == 0) && rmin_idx && (rmin < row_bitcount)) // neighbor found
        {
            pc = (unsigned char)(ibpc_close | (rmin_idx << 3));
        }
        pc_vector[i] = pc;
    } // for i
}


/*!
    \brief Compute number of ibpc codes in pc_vector
*/
inline
void bit_iblock_pcv_stat(const unsigned char* BMRESTRICT pc_vector,
                         const unsigned char* BMRESTRICT pc_vector_end,
                         unsigned* BMRESTRICT pc_vector_stat
                        )
{
    BM_ASSERT(pc_vector_stat);
    // pc_vector_stat MUST be assigned to 0 before 
    do 
    {
        unsigned ibpc = *pc_vector & 7;
        ++(pc_vector_stat[ibpc]);
    } while (++pc_vector < pc_vector_end);
}



/**
    \brief Matrix reduction based on transformation pc vector
*/
inline
void bit_iblock_reduce(
    const unsigned  tmatrix[bm::set_block_plane_cnt][bm::set_block_plane_size],
    const unsigned char* BMRESTRICT pc_vector,
    const unsigned char* BMRESTRICT pc_vector_end,
    unsigned  tmatrix_out[bm::set_block_plane_cnt][bm::set_block_plane_size])
{
    ::memset(tmatrix_out, 0, bm::set_block_plane_cnt * sizeof(*tmatrix_out));
    unsigned row = 0;
    do 
    {
        unsigned ibpc = *pc_vector & 7;
        unsigned n_row = *pc_vector >> 3;
        
        switch(ibpc)
        {
        case bm::ibpc_uncompr:
            {
            const unsigned* r1 = tmatrix[row];
            unsigned* r_out = tmatrix_out[row];
            for (unsigned i = 0; i < bm::set_block_plane_size; ++i)
            {
                r_out[i] = r1[i];
            }
            }
            break;
        case bm::ibpc_all_zero:
            break;
        case bm::ibpc_all_one:
            break;
        case bm::ibpc_equiv:
            break;
        case bm::ibpc_close:
            {
            const unsigned* r1 = tmatrix[row];
            const unsigned* r2 = tmatrix[n_row];
            unsigned* r_out = tmatrix_out[row];
            for (unsigned i = 0; i < bm::set_block_plane_size; ++i)
            {
                r_out[i] = r1[i] ^ r2[i];
            } // for
            }
            break;
        default:
            BM_ASSERT(0);
            break;
        } // switch
        ++row;
    } while (++pc_vector < pc_vector_end);
    
}

/**
    \brief Transposed Matrix reduction based on transformation pc vector
*/
template<class TMatrix>
void tmatrix_reduce(TMatrix& tmatrix, 
                    const unsigned char* pc_vector,
                    const unsigned       effective_cols)
{
    BM_ASSERT(pc_vector);

    typedef typename TMatrix::value_type value_type;

    const unsigned char* pc_vector_end = pc_vector + tmatrix.rows();
    unsigned row = 0;
    unsigned cols = effective_cols ? effective_cols : tmatrix.cols();

    do
    {
        unsigned ibpc = *pc_vector & 7;        
        switch(ibpc)
        {
        case bm::ibpc_uncompr:
        case bm::ibpc_all_zero:
        case bm::ibpc_all_one:
        case bm::ibpc_equiv:
            break;
        case bm::ibpc_close:
            {
            unsigned n_row = *pc_vector >> 3;
            BM_ASSERT(n_row > row);

            value_type* r1 = tmatrix.row(row);
            const value_type* r2 = tmatrix.row(n_row);
            for (unsigned i = 0; i < cols; ++i)
            {
                r1[i] ^= r2[i];
            } // for
            }
            break;
        default:
            BM_ASSERT(0);
            break;
        } // switch
        ++row;
    } while (++pc_vector < pc_vector_end);
}

/**
    \brief Transposed Matrix restore based on transformation pc vector
*/
template<class TMatrix>
void tmatrix_restore(TMatrix& tmatrix, 
                     const unsigned char* pc_vector,
                     const unsigned effective_cols)
{
    BM_ASSERT(pc_vector);

    typedef typename TMatrix::value_type value_type;

    unsigned cols = effective_cols ? effective_cols : tmatrix.cols();
    for (unsigned row = tmatrix.rows()-1u; 1; --row)
    {
        unsigned ibpc = pc_vector[row] & 7u;
        unsigned n_row = pc_vector[row] >> 3u;

        value_type* r1 = tmatrix.row(unsigned(row));

        switch(ibpc)
        {
        case bm::ibpc_uncompr:
            break;
        case bm::ibpc_all_zero:
            for (unsigned i = 0; i < cols; ++i)
                r1[i] = 0;
             break;
        case bm::ibpc_all_one:
            for (unsigned i = 0; i < cols; ++i)
                r1[i] = (value_type)(~0);
            break;
        case bm::ibpc_equiv:
            {
            BM_ASSERT(n_row > row);
            const value_type* r2 = tmatrix.row(n_row);
            for (unsigned i = 0; i < cols; ++i)
                r1[i] = r2[i];
            }
            break;
        case bm::ibpc_close:
            {      
            BM_ASSERT(n_row > row);
            const value_type* r2 = tmatrix.row(n_row);
            for (unsigned i = 0; i < cols; ++i)
                r1[i] ^= r2[i];
            }
            break;
        default:
            BM_ASSERT(0);
            break;
        } // switch
        
        if (row == 0)
            break;
    }  // for

}



/**
    \brief Copy GAP block body to bit block with DGap transformation 
    \internal
*/
template<typename GT, typename BT>
void gap_2_bitblock(const GT* BMRESTRICT gap_buf, 
                          BT* BMRESTRICT block, 
                          unsigned       block_size)
{
    GT* dgap_buf = (GT*) block;
    BT* block_end = block + block_size;

    GT* dgap_end = gap_2_dgap<GT>(gap_buf, dgap_buf, false);
    GT* block_end2 = (GT*) block_end;
    
    // zero the tail memory
    for ( ;dgap_end < block_end2; ++dgap_end)
    {
        *dgap_end = 0;
    }
}

#if 0
/**
    @brief Compute t-matrix rows statistics used for compression

    @param tmatrix - transposed matrix
    @param pc_vector - row content vector
    @param rstat - output row vector
    @param effective_cols - effective columns

    @internal
*/
template<class TMatrix>
void compute_tmatrix_rstat(const TMatrix& tmatrix, 
                           const unsigned char* pc_vector,
                           typename TMatrix::rstat* rstat,
                           unsigned effective_cols)
{
    BM_ASSERT(rstat);
    typedef typename TMatrix::value_type value_type;

    unsigned cols = effective_cols ? effective_cols : tmatrix.cols();
    //unsigned cols = tmatrix.cols();
    unsigned rows = tmatrix.rows();

    for (unsigned i = 0; i < rows; ++i)
    {
        unsigned ibpc = pc_vector[i] & 7;        
        switch(ibpc)
        {
        case bm::ibpc_all_zero:
        case bm::ibpc_all_one:
        case bm::ibpc_equiv:
            rstat[i].bit_count = rstat[i].gap_count = 0;
            rstat[i].best_rep = bm::set_bitset;
            break;
        case bm::ibpc_uncompr:
        case bm::ibpc_close:
            {
            const value_type* r1 = tmatrix.row(i);
            const value_type* r1_end = r1 + cols;
            // TODO: find how to deal with the potentially incorrect type-cast
            bm::bit_count_change32((bm::word_t*)r1, (bm::word_t*)r1_end, 
                                    &rstat[i].bit_count, &rstat[i].gap_count);

            const unsigned bitset_size = unsigned(sizeof(value_type) * cols);
            const unsigned total_possible_max_bits = unsigned(sizeof(value_type)*8*cols);

            rstat[i].best_rep = 
                bm::best_representation(rstat[i].bit_count,
                                        total_possible_max_bits,
                                        rstat[i].gap_count,
                                        bitset_size);

            }
            break;
        default:
            BM_ASSERT(0);
            break;
        } // switch

    } // for 
}
#endif


/**
    \brief Compute effective right column border of the t-matrix
    \internal
*/
template<typename TM>
unsigned find_effective_columns(const TM& tmatrix)
{
    // TODO: need optimization in order not to scan the whole space
    unsigned col = 1;
    for (unsigned i = 0; i < tmatrix.rows(); ++i)
    {
        const typename TM::value_type* row = tmatrix.value[i];
        for (unsigned j = 0; j < tmatrix.cols(); ++j)
        {
            if (row[j] != 0 && j > col)
            {
                col = j;
            }
        }
    }
    return col;
}


/**
    \brief Bit-plane splicing of a GAP block
    
    GT - gap word type
    BT - block word type
    BLOCK_SIZE - bit block size in words (works as a transposition basis)
    
    @internal
*/
template<typename GT, typename BT, unsigned BLOCK_SIZE>
class gap_transpose_engine
{
public:
    /// cryptic calculation of equivalent size for the transpose matrix
    /// based on BLOCK_SIZE and sizeof(GT)(16)
    ///
    /// matrix[size_of_gap*8][(Size_block_in_bytes / size_of_gap) / number_of_planes)] 
    typedef 
    tmatrix<GT, static_cast<unsigned>(sizeof(GT)*8),
            static_cast<unsigned>(((BLOCK_SIZE * sizeof(unsigned)) / (sizeof(GT)))
                                  / (sizeof(GT) * 8))>
                tmatrix_type;
                
    gap_transpose_engine() : eff_cols_(0)
    {}            
    
    /// Transpose GAP block through a temp. block of aligned(!) memory
    /// 
    void transpose(const GT* BMRESTRICT gap_buf, 
                         BT* BMRESTRICT tmp_block)
    {
        const unsigned arr_size = BLOCK_SIZE * sizeof(unsigned) / sizeof(GT);

        BM_ASSERT(sizeof(tmatrix_.value) == tmatrix_type::n_columns * 
                                            tmatrix_type::n_rows * sizeof(GT));
                
        // load all GAP as D-GAP(but not head word) into aligned bit-block
        gap_2_bitblock(gap_buf, tmp_block, BLOCK_SIZE);
        
        // transpose
        vect_bit_transpose<GT, tmatrix_type::n_rows, tmatrix_type::n_columns>
                           ((GT*)tmp_block, arr_size, tmatrix_.value);

        // calculate number of non-zero columns
        eff_cols_ = find_effective_columns(tmatrix_);        
    }

	/// Transpose array of shorts
	///
	void transpose(const GT* BMRESTRICT garr,
		           unsigned garr_size,
				   BT* BMRESTRICT tmp_block)
	{
		BM_ASSERT(garr_size);

		bit_block_set(tmp_block, 0);
		::memcpy(tmp_block, garr, sizeof(GT)*garr_size);

        const unsigned arr_size = BLOCK_SIZE * sizeof(unsigned) / sizeof(GT);
        BM_ASSERT(sizeof(tmatrix_.value) == tmatrix_type::n_columns * 
                                            tmatrix_type::n_rows * sizeof(GT));
        // transpose
        vect_bit_transpose<GT, tmatrix_type::n_rows, tmatrix_type::n_columns>
                           ((GT*)tmp_block, arr_size, tmatrix_.value);

        // calculate number of non-zero columns
        eff_cols_ = find_effective_columns(tmatrix_);        

	}

    void compute_distance_matrix()
    {
        tmatrix_distance<typename tmatrix_type::value_type, 
                         tmatrix_type::n_rows, tmatrix_type::n_columns>
                         (tmatrix_.value, distance_);

        // make compression descriptor vector and statistics vector
        bit_iblock_make_pcv<unsigned char, 
                            tmatrix_type::n_rows, tmatrix_type::n_columns>
                            (distance_, pc_vector_);

        bit_iblock_pcv_stat(pc_vector_, 
                            pc_vector_ + tmatrix_type::n_rows, 
                            pc_vector_stat_);
    }

    void reduce()
    {
        tmatrix_reduce(tmatrix_, pc_vector_, eff_cols_);
        //compute_tmatrix_rstat(tmatrix_, pc_vector_, rstat_vector_, eff_cols_);
    }

    void restore()
    {
        tmatrix_restore(tmatrix_, pc_vector_, eff_cols_);
    }
    
    
    /// Restore GAP block from the transposed matrix
    ///
    void trestore(GT             gap_head, 
                  GT* BMRESTRICT gap_buf, 
                  BT* BMRESTRICT tmp_block)
    {
        BM_ASSERT(sizeof(tmatrix_.value) == tmatrix_type::n_columns * 
                                            tmatrix_type::n_rows * sizeof(GT));
  
        // restore into a temp buffer
        GT* gap_tmp = (GT*)tmp_block;
        //*gap_tmp++ = gap_head;
       
        vect_bit_trestore<GT, tmatrix_type::n_rows, tmatrix_type::n_columns>(tmatrix_.value, gap_tmp);
        
        // D-Gap to GAP block recalculation
        gap_tmp = (GT*)tmp_block;
        dgap_2_gap<GT>(gap_tmp, gap_buf, gap_head);
    }
    
public:
//    GT            gap_head_;
    tmatrix_type                  tmatrix_;    
    unsigned                      eff_cols_;
    unsigned                      distance_[tmatrix_type::n_rows][tmatrix_type::n_rows];
    unsigned char                 pc_vector_[tmatrix_type::n_rows];
    unsigned                      pc_vector_stat_[bm::ibpc_end];
    typename tmatrix_type::rstat  rstat_vector_[tmatrix_type::n_rows];
};


} // namespace bm


#ifdef _MSC_VER
#pragma warning( pop )
#endif


#endif
