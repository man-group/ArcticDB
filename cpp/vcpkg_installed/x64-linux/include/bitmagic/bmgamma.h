#ifndef BMGAMMAENC__H__INCLUDED__
#define BMGAMMAENC__H__INCLUDED__
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

/*! \file bmgamma.h
    \brief Elias Gamma Utils used for compact serialization (internal)
*/


namespace bm
{

/*! 
    \defgroup gammacode Elias Gamma Code (internal)
    Elias Gamma Encoder
    @internal
    \ingroup bvserial
 
 */

/**
    Elias Gamma decoder
    \ingroup gammacode
*/
template<typename T, typename TBitIO>
class gamma_decoder
{
public:
    gamma_decoder(TBitIO& bin) BMNOEXEPT : bin_(bin)
    {}
    
    /**
        Start encoding sequence
    */
    void start() BMNOEXEPT
    {}
    
    /**
        Stop decoding sequence
    */
    void stop() BMNOEXEPT
    {}
    
    /**
        Decode word
    */
    T operator()(void) BMNOEXEPT
    {
        unsigned l = bin_.eat_zero_bits();
        bin_.get_bit(); // get border bit
        T current = 0;
        for (unsigned i = 0; i < l; ++i)
        {
            if (bin_.get_bit())
            {
                current += 1 << i;
            }
        }
        current |= (1 << l);
        return current;
    }
private:
    gamma_decoder(const gamma_decoder&);
    gamma_decoder& operator=(const gamma_decoder&);
private:
    TBitIO&  bin_;
};



} // bm

#endif

