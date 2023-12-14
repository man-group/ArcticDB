#ifndef BMVMIN__H__INCLUDED__
#define BMVMIN__H__INCLUDED__
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

/*! \file bmvmin.h
    \brief Mini bitset for testing and utility purposes (internal)
*/


#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4100)
#endif

namespace bm
{


#define BM_MINISET_GAPLEN (bm::gap_len_table<true>::_len[0])
#define BM_MINISET_ARRSIZE(x) ((x / 32) + ( (x % 32) && 1 ))


/*! @defgroup mset Small sets functionality (intrenal)
 *  @internal
 *  @ingroup bmagic
 *  
 */

/*!@{  */
/*!
    @brief Template class implements memory saving set functionality
    @ingroup mset
    @sa bvmini
    @internal
*/
template <class A, size_t N> class miniset
{
public:

    miniset() 
        : m_buf(0),
          m_type(1)
    {}

    miniset(const miniset& mset)
    {
        if (mset.m_buf)
        {
            if (mset.m_type)
                init_gapbuf(mset.m_buf);
            else
                init_bitbuf(mset.m_buf);
        }
        else
        {
            m_type = mset.m_type;
            m_buf = 0;
        }
    }

    ~miniset()
    {
        if (m_buf)
        {
            A::deallocate(m_buf, m_type ? 
                (BM_MINISET_GAPLEN / (sizeof(bm::word_t) / sizeof(bm::gap_word_t)))
                : 
                (BM_MINISET_ARRSIZE(N)));
        }
    }

    /// Checks if bit pos 1 or 0. Returns 0 if 0 and non zero otherwise.
    unsigned test(bm::id_t n) const 
    { 
        return
            !m_buf ? 0 
            :
            m_type ? 
              gap_test((gap_word_t*)m_buf, n)
              : 
              m_buf[n>>bm::set_word_shift] & (1<<(n & bm::set_word_mask));
    }

    void set(bm::id_t n, bool val=true)
    {
        if (m_type == 0)
        {
            if (!m_buf)
            {
                if (!val) return;
                init_bitbuf(0);
            }

            unsigned nword  = n >> bm::set_word_shift; 
            unsigned mask = unsigned(1) << (n & bm::set_word_mask);

            val ? (m_buf[nword] |= mask) : (m_buf[nword] &= ~mask);
        }
        else
        {
            if (!m_buf)
            {
                if (!val) return;
                init_gapbuf(0);
            }
            
            unsigned is_set;
            unsigned new_block_len = 
                gap_set_value(val, (gap_word_t*)m_buf, n, &is_set);

            if (new_block_len > unsigned(BM_MINISET_GAPLEN-4))
            {
                convert_buf();
            }
        }
    }

    unsigned mem_used() const
    {
        return sizeof(*this) +
               (m_buf ?
                 (m_type ? (BM_MINISET_GAPLEN * sizeof(gap_word_t))
                        : (BM_MINISET_ARRSIZE(N) * sizeof(bm::word_t)))
                : 0);
    }

    void swap(miniset& mset)
    {
        bm::word_t* buftmp = m_buf;
        m_buf = mset.m_buf;
        mset.m_buf = buftmp;
        unsigned typetmp = m_type;
        m_type = mset.m_type;
        mset.m_type = typetmp;
    }


private:

    void init_bitbuf(bm::word_t* buf)
    {
        unsigned arr_size = BM_MINISET_ARRSIZE(N);
        m_buf = A::allocate(arr_size, 0);
        if (buf)
        {
            ::memcpy(m_buf, buf, arr_size * sizeof(bm::word_t));
        }
        else
        {
            ::memset(m_buf, 0, arr_size * sizeof(bm::word_t));
        }
        m_type = 0;
    }

    void init_gapbuf(bm::word_t* buf)
    {
        unsigned arr_size = 
            unsigned(BM_MINISET_GAPLEN / (sizeof(bm::word_t) / sizeof(bm::gap_word_t)));
        m_buf = A::allocate(arr_size, 0);
        if (buf)
        {
            ::memcpy(m_buf, buf, arr_size * sizeof(bm::word_t));
        }
        else
        {
            *m_buf = 0;
            gap_set_all((gap_word_t*)m_buf, bm::gap_max_bits, 0);
        }
        m_type = 1;
    }

    void convert_buf()
    {
        unsigned arr_size = BM_MINISET_ARRSIZE(N);
        bm::word_t* buf = A::allocate(arr_size, 0);

        gap_convert_to_bitset(buf, (gap_word_t*) m_buf/*, arr_size*/);
        arr_size = 
            unsigned(BM_MINISET_GAPLEN / (sizeof(bm::word_t) / sizeof(bm::gap_word_t)));
        A::deallocate(m_buf, arr_size);
        m_buf = buf;
        m_type = 0;
    }

private:
    bm::word_t*   m_buf;      //!< Buffer pointer
    unsigned      m_type;     //!< buffer type (0-bit, 1-gap)
};


/*!
    @brief Mini bit-vector for auxiliary purposes
    @ingroup mset
    @sa miniset
    @internal
*/
template<size_t N> class bvmini
{
public:

    bvmini(int = 0)
    {
        ::memset(m_buf, 0, sizeof(m_buf));
    }

    bvmini(const bvmini& mset)
    {
        ::memcpy(m_buf, mset.m_buf, sizeof(m_buf));
    }


    /// Checks if bit pos 1 or 0. Returns 0 if 0 and non zero otherwise.
    unsigned test(bm::id_t n) const 
    { 
        return m_buf[n>>bm::set_word_shift] & (1<<(n & bm::set_word_mask));
    }

    void set(bm::id_t n, bool val=true)
    {
        unsigned nword  = n >> bm::set_word_shift; 
        unsigned mask = unsigned(1) << (n & bm::set_word_mask);

        val ? (m_buf[nword] |= mask) : (m_buf[nword] &= ~mask);
    }

    unsigned mem_used() const
    {
        return sizeof(*this);
    }

    void swap(bvmini& mset)
    {
        for (unsigned i = 0; i < BM_MINISET_ARRSIZE(N); ++i)
        {
            bm::word_t tmp = m_buf[i];
            m_buf[i] = mset.m_buf[i];
            mset.m_buf[i] = tmp;
        }
    }

private:
    bm::word_t   m_buf[BM_MINISET_ARRSIZE(N)];
};


/**@} */

/*!
    @brief Bitvector class with very limited functionality.

    Class implements simple bitset and used for internal 
    and testing purposes.
    @internal
*/
template<class A> class bvector_mini
{
public:
#ifdef BM64ADDR
    typedef bm::id64_t                                   size_type;
#else
    typedef bm::id_t                                     size_type;
#endif

public:
    bvector_mini(size_type size)
      : m_buf(0),
        m_size(size)
    {
        size_type arr_size = (size / 32) + 1;
        m_buf = A::allocate(arr_size, 0);
        ::memset(m_buf, 0, arr_size * sizeof(unsigned));
    }
    bvector_mini(const bvector_mini& bvect)
       : m_size(bvect.m_size)
    {
        size_type arr_size = (m_size / 32) + 1;
        m_buf = A::allocate(arr_size, 0);
        ::memcpy(m_buf, bvect.m_buf, arr_size * sizeof(unsigned));        
    }

    ~bvector_mini()
    {
        A::deallocate(m_buf, (m_size / 32) + 1); 
    }

    /// Checks if bit pos 1 or 0. Returns 0 if 0 and non zero otherwise.
    int is_bit_true(size_type pos) const
    {
        unsigned char  mask = (unsigned char)((char)0x1 << (pos & 7));
        unsigned char* offs = (unsigned char*)m_buf + (pos >> 3);
        return (*offs) & mask;
    }

    /// Sets bit number pos to 1
    void set_bit(size_type pos)
    {
        unsigned char  mask = (unsigned char)(0x1 << (pos & 7));
        unsigned char* offs = (unsigned char*)m_buf + (pos >> 3); 
        *offs |= mask;
    }

    /// Sets bit number pos to 0
    void clear_bit(size_type pos)
    {
        unsigned char  mask = (unsigned char)(0x1 << (pos & 7));
        unsigned char* offs = (unsigned char*)m_buf + (pos >> 3);
        *offs &= (unsigned char)~mask;
    }

    /// Counts number of bits ON 
    size_type bit_count() const
    {
        size_type count = 0;
        const unsigned* end = m_buf + (m_size / 32)+1;
        for (unsigned* start = m_buf; start < end; ++start)
        {
            unsigned value = *start;
            for (count += (value!=0); value &= value - 1; ++count);
        }
        return count;
    }

    /// Comparison.
    int compare(const bvector_mini& bvect)
    {
        size_type cnt1 = bit_count();
        size_type cnt2 = bvect.bit_count();
        if (!cnt1 && !cnt2) return 0;
        size_type cnt_min = cnt1 < cnt2 ? cnt1 : cnt2;
        if (!cnt_min) return cnt1 ? 1 : -1;

        size_type idx1 = get_first();
        size_type idx2 = bvect.get_first();

        for (size_type i = 0; i < cnt_min; ++i)
        {
            if (idx1 != idx2)
            {
                return idx1 < idx2 ? 1 : -1;
            }
            idx1 = get_next(idx1);
            idx2 = bvect.get_next(idx2);
        }
        if (idx1 != idx2)
        {
            if (!idx1) return -1;
            if (!idx2) return  1;
            return idx1 < idx2 ? 1 : -1;
        }
        return 0;
    }


    /// Returns index of the first ON bit
    size_type get_first() const
    {
        size_type pos = 0;
        const unsigned char* ptr = (unsigned char*) m_buf;
        for (unsigned i = 0; i < ((m_size/8)+1); ++i)
        {
            unsigned char w = ptr[i];
            if (w != 0)
            {
                while ((w & 1u) == 0)
                {
                    w = (unsigned char)(w >> 1u);
                    ++pos;
                }
                return pos;
            }
            pos = size_type(pos + sizeof(unsigned char) * 8u);
        }
        return 0;
    }


    /// Returns index of next bit, which is ON
    size_type get_next(size_type idx) const
    {
        size_type i;
        for (i = idx+1; i < m_size; ++i)
        {
            unsigned char* offs = (unsigned char*)m_buf + (i >> 3); 
            if (*offs)
            {
                unsigned char mask = (unsigned char)((char)0x1 << (i & 7));
                if (*offs & mask)
                {
                    return i;
                }
            }
            else
            {
                i += 7;
            }
        }
        return 0;
    }

    void combine_and(const bvector_mini& bvect)
    {
        const unsigned* end = m_buf + (m_size / 32)+1;
        const unsigned* src = bvect.get_buf();
        for (unsigned* start = m_buf; start < end; ++start)
        {
            *start &= *src++;
        }
    }

    void combine_xor(const bvector_mini& bvect)
    {
        const unsigned* end = m_buf + (m_size / 32)+1;
        const unsigned* src = bvect.get_buf();
        for (unsigned* start = m_buf; start < end; ++start)
        {
            *start ^= *src++;
        }
    }

    void combine_or(const bvector_mini& bvect)
    {
        const unsigned* end = m_buf + (m_size / 32)+1;
        const unsigned* src = bvect.get_buf();
        for (unsigned* start = m_buf; start < end; ++start)
        {
            *start |= *src++;
        }
    }

    void combine_sub(const bvector_mini& bvect)
    {
        const unsigned* end = m_buf + (m_size / 32)+1;
        const unsigned* src = bvect.get_buf();
        for (unsigned* start = m_buf; start < end; ++start)
        {
            *start &= ~(*src++);
        }
    }

    const unsigned* get_buf() const { return m_buf; }
    unsigned mem_used() const
    {
        return unsigned(sizeof(bvector_mini) + (m_size / 32) + 1);
    }

    void swap(bvector_mini& bvm)
    {
        bm::word_t* buftmp = m_buf;
        m_buf = bvm.m_buf;
        bvm.m_buf = buftmp;
    }

private:
    bm::word_t*   m_buf;
    size_type     m_size;
};



} // namespace bm

#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif
