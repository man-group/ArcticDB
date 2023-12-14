#ifndef BMALGO_SIMILARITY__H__INCLUDED__
#define BMALGO_SIMILARITY__H__INCLUDED__
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

#include <algorithm>
#include <functional>
#include <vector>

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration 
// #include "bm.h" or "bm64.h" explicitly 
# error missing include (bm.h or bm64.h)
#endif


#include "bmfunc.h"
#include "bmdef.h"

#include "bmalgo_impl.h"

namespace bm
{

/*! Similarity descriptor between two objects (bit vectors, blocks, etc)
    \internal
*/
template <typename SO, unsigned DMD_SZ, typename IDX_VALUE, typename SValue, typename SFunc>
class similarity_descriptor
{
public:
    typedef SO                 similarity_object_type;
    typedef SValue             similarity_value_type;
    typedef SFunc              similarity_functor;
public:
    similarity_descriptor()
     : so1_(0), so2_(0), so1_idx_(0), so2_idx_(0)
    {
		similarity_ = 0;
	}
    
    similarity_descriptor(const SO* so1, const SO* so2,
                          const distance_metric_descriptor* dmd_ptr)
    :so1_(so1),
     so2_(so2),
     so1_idx_(0), so2_idx_(0)
    {
        for (size_t i = 0; i < DMD_SZ; ++i)
            dmd_[i] = dmd_ptr[i];
		similarity_ = 0;
    }

    similarity_descriptor(const SO* so1, IDX_VALUE i1,
                          const SO* so2, IDX_VALUE i2,
                          const distance_metric_descriptor* dmd_ptr)
    :so1_(so1), so2_(so2), so1_idx_(i1), so2_idx_(i2)
    {
        for (size_t i = 0; i < DMD_SZ; ++i)
            dmd_[i] = dmd_ptr[i];
		similarity_ = 0;
    }

    similarity_descriptor(const similarity_descriptor& sd)
    : similarity_(sd.similarity_),
      so1_(sd.so1_),
      so2_(sd.so2_),
      so1_idx_(sd.so1_idx_),
      so2_idx_(sd.so2_idx_)
    {
        for (size_t i = 0; i < DMD_SZ; ++i)
            dmd_[i] = sd.dmd_[i];
    }
    similarity_descriptor& operator=(const similarity_descriptor& sd)
    {
        similarity_ = sd.similarity_;
        so1_ = sd.so1_; so2_ = sd.so2_;
        so1_idx_ = sd.so1_idx_; so2_idx_ = sd.so2_idx_;
        for (size_t i = 0; i < DMD_SZ; ++i)
            dmd_[i] = sd.dmd_[i];
        return *this;
    }
    
    bool operator > (const similarity_descriptor& sd) const
    {
        return similarity_ > sd.similarity_;
    }
    
    SValue similarity() const { return similarity_; }
    void set_similarity(SValue s) { similarity_ = s;}
    
    const SO* get_first() const  { return so1_; }
    const SO* get_second() const { return so2_; }
    
    IDX_VALUE get_first_idx() const { return so1_idx_; }
    IDX_VALUE get_second_idx() const { return so2_idx_; }

    distance_metric_descriptor* distance_begin() { return &dmd_[0]; }
    distance_metric_descriptor* distance_end() { return &dmd_[DMD_SZ]; }
    
    void set_metric(size_t i, distance_metric metric)
    {
        BM_ASSERT(i < DMD_SZ);
        dmd_[i].metric = metric;
    }
    

protected:
    SValue     similarity_; //< final similarity product
    const SO*  so1_;        //< object 1 for similarity comparison
    const SO*  so2_;        //< object 2 for similarity comparison
    IDX_VALUE  so1_idx_;    //< index of object 1
    IDX_VALUE  so2_idx_;    //< index of object 2
    distance_metric_descriptor dmd_[DMD_SZ]; //< array of distance operations defined on objects
};

/*!
     Batch of objects for similarity measurement
     \internal
*/
template<class SDESCR>
struct similarity_batch
{
    typedef SDESCR                                      similaruty_descriptor_type;
    typedef typename SDESCR::similarity_object_type     similarity_object_type;
    typedef typename SDESCR::similarity_value_type      similarity_value_type;
    typedef typename SDESCR::similarity_functor         similarity_functor;
    typedef std::vector<SDESCR>                         vector_type;

    /// run the similarity calculation using distance metrics engine
    void calculate()
    {
        for( size_t i = 0; i < descr_vect_.size(); ++i)
        {
            similaruty_descriptor_type& sdescr = descr_vect_[i];
            
            const similarity_object_type* so1 = sdescr.get_first();
            const similarity_object_type* so2 = sdescr.get_second();
            
            distance_metric_descriptor* dit = sdescr.distance_begin();
            distance_metric_descriptor* dit_end = sdescr.distance_end();
            bm::distance_operation(*so1, *so2, dit, dit_end);
            
            // reduce: use functor to compute final similarity metric
            similarity_functor func;
            similarity_value_type d = func(dit, dit_end);
            sdescr.set_similarity(d);
        } // for i
    }
    
    void sort()
    {
        std::sort(descr_vect_.begin(), descr_vect_.end(), std::greater<SDESCR>());
    }
    
    void reserve(size_t cap)
    {
        descr_vect_.reserve(cap);
    }
    
    void push_back(const similaruty_descriptor_type& sdt)
    {
        descr_vect_.push_back(sdt);
    }
    
public:
    std::vector<SDESCR> descr_vect_;
};


/**
   Utility function to build jaccard similarity batch for sparse_vector<>
   \internal
*/
template<class SIMBATCH, class SV>
void build_jaccard_similarity_batch(SIMBATCH& sbatch, const SV& sv)
{

    size_t planes = sv.get_bmatrix().rows();//slices();
    sbatch.reserve((planes * planes) / 2);

    bm::distance_metric_descriptor dmd[2];
    dmd[0].metric = bm::COUNT_AND;
    dmd[1].metric = bm::COUNT_OR;
    
    // build a batch for triangular distance matrix
    //
    for (unsigned i = 0; i < planes; ++i)
    {
        if (const typename SV::bvector_type* bv1 = sv.get_slice(i))
        {
            for (unsigned j = i+1; j < planes; ++j)
            {
                const typename SV::bvector_type* bv2 = sv.get_slice(j);
                if (bv2 && bv1 != bv2)
                    sbatch.push_back(typename SIMBATCH::similaruty_descriptor_type(bv1, i, bv2, j, &dmd[0]));
            } // for j
        }

    } // for i
}



} // namespace bm


#endif
