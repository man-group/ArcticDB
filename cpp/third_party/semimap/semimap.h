/*
  semi::static_map and semi::map - associative map containers with compile-time
  lookup!

  ==============================================================================
  Copyright (c) 2018 Fabian Renn-Giles

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
  ==============================================================================

  This associative map container avoids any hashing overhead when the key is
  a literal. It can also fall back to run-time lookup for non-literal keys.

  You use it as follows:

  #include <iostream>
  #include <string>

  #include "semimap.h"

  #define ID(x) []() constexpr { return x; }

  int main()
  {
      semi::map<std::string, std::string> map;

      // Using string literals to access the container is super fast:
      // computational complexity remains constant regardless of the number of key, value pairs!
      map.get(ID("food")) = "pizza";
      map.get(ID("drink")) = "soda";
      std::cout << map.get(ID("drink")) << std::endl;

      // Values can also be looked-up with run-time keys
      // which will then use std::unordered_map as a fallback.
      std::string key;

      std::cin >> key;
      std::cout << map.get(key) << std::endl; // for example: outputs "soda" if key is "drink"

      // there is also a static version of the map where lookup is even faster
      struct Tag {};
      using Map = semi::static_map<std::string, std::string, Tag>;

      // in fact, it is (nearly) as fast as looking up any plain old global variable
      Map::get(ID("food")) = "pizza";
      Map::get(ID("drink")) = "beer";

      return 0;
  }

  See my cppcon talk about this container here:

  Slides: https://goo.gl/igwVxD
  Video:  https://www.youtube.com/watch?v=qNAbGpV1ZkU

  Twitter: @hogliux
*/

#include <cstring>
#include <memory>
#include <new>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

// check C++ version
#if (defined(_MSVC_LANG) && _MSVC_LANG < 201703L) || ((!defined(_MSVC_LANG)) && __cplusplus < 201703L)
#error semi::map and semi::static_map require C++17 support
#endif

#ifdef __GNUC__
#define semi_branch_expect(x, y) __builtin_expect(x, y)
#else
#define semi_branch_expect(x, y) x
#endif

namespace semi {

namespace detail {
    // evaluates to the type returned by a constexpr lambda
    template <typename Identifier>
    using identifier_type = decltype(std::declval<Identifier>()());

    //==============================================================================
    constexpr std::size_t constexpr_strlen(const char* str) { return str[0] == 0 ? 0 : constexpr_strlen(str + 1) + 1; }

    //==============================================================================
    template <auto...>
    struct dummy_t {
    };

    template <typename Identifier, std::enable_if_t<std::is_integral_v<identifier_type<Identifier>>, int> = 0>
    constexpr auto idval2type(Identifier id)
    {
        return dummy_t<id()>{};
    }

    template <typename Identifier, std::size_t... I>
    constexpr auto array_id2type(Identifier id, std::index_sequence<I...>)
    {
        return dummy_t<id()[I]...>{};
    }

    template <typename Identifier, std::enable_if_t<std::is_same_v<identifier_type<Identifier>, const char*>, int> = 0>
    constexpr auto idval2type(Identifier id)
    {
        return array_id2type(id, std::make_index_sequence<constexpr_strlen(id())>{});
    }

    template <typename, typename, bool>
    struct default_tag {
    };

    //==============================================================================
    // super simple flat map implementation
    template <typename Key, typename Value>
    class flat_map {
    public:
        template <typename... Args>
        Value& get(const Key& key, Args&&... args)
        {
            for (auto& pair : storage)
                if (pair.first == key)
                    return pair.second;

            return storage.emplace_back(key, Value(std::forward<Args>(args)...)).second;
        }

        auto size() const
        {
            return storage.size();
        }

        void erase(const Key& key)
        {
            for (auto it = storage.begin(); it != storage.end(); ++it) {
                if (it->first == key) {
                    storage.erase(it);
                    return;
                }
            }
        }

        bool contains(const Key& key) const
        {
            for (auto& pair : storage)
                if (pair.first == key)
                    return true;

            return false;
        }

    private:
        std::vector<std::pair<Key, Value>> storage;
    };

    //==============================================================================
    // some versions of clang do not seem to have std::launder
#if __cpp_lib_launder >= 201606
    template <class T>
    constexpr T* launder(T* p)
    {
        return std::launder(p);
    }
#else
    template <class T>
    constexpr T* launder(T* p)
    {
        return p;
    }
#endif

} // namespace detail

// forward declaration
template <typename, typename, typename>
class map;

//==============================================================================
//==============================================================================
template <typename Key, typename Value, typename Tag = detail::default_tag<Key, Value, true>>
class static_map {
public:
    using KeyType = Key;
    static_map() = delete;

    template <typename Identifier, typename... Args, std::enable_if_t<std::is_invocable_v<Identifier>, int> = 0>
    static Value& get(Identifier identifier, Args&&... args)
    {
        static_assert(std::is_convertible_v<detail::identifier_type<Identifier>, Key>);
        using UniqueTypeForKeyValue = decltype(detail::idval2type(identifier));
        auto* mem = storage<UniqueTypeForKeyValue>;
        auto& i_flag = init_flag<UniqueTypeForKeyValue>;

        if (!semi_branch_expect(i_flag, true)) {
            Key key(identifier());

            auto it = runtime_map.find(key);

            if (it != runtime_map.end())
                it->second = u_ptr(new (mem) Value(std::move(*it->second)), { &i_flag });
            else
                runtime_map.emplace_hint(it, key, u_ptr(new (mem) Value(std::forward<Args>(args)...), { &i_flag }));

            i_flag = true;
        }

        return *detail::launder(reinterpret_cast<Value*>(mem));
    }

    template <typename... Args>
    static Value& get(const Key& key, Args&&... args)
    {
        auto it = runtime_map.find(key);

        if (it != runtime_map.end())
            return *it->second;

        return *runtime_map.emplace_hint(it, key, u_ptr(new Value(std::forward<Args>(args)...), { nullptr }))->second;
    }

    template <typename Identifier, std::enable_if_t<std::is_invocable_v<Identifier>, int> = 0>
    static bool contains(Identifier identifier)
    {
        using UniqueTypeForKeyValue = decltype(detail::idval2type(identifier));

        if (!semi_branch_expect(init_flag<UniqueTypeForKeyValue>, true)) {
            auto key = identifier();
            return contains(key);
        }

        return true;
    }

    static bool contains(const Key& key)
    {
        return (runtime_map.find(key) != runtime_map.end());
    }

    template <typename Identifier, std::enable_if_t<std::is_invocable_v<Identifier>, int> = 0>
    static void erase(Identifier identifier)
    {
        erase(identifier());
    }

    static void erase(const Key& key)
    {
        runtime_map.erase(key);
    }

    static void clear()
    {
        runtime_map.clear();
    }

    static auto begin() {
        return runtime_map.begin();
    }

    static auto end() {
        return runtime_map.end();
    }

private:
    struct value_deleter {
        bool* i_flag = nullptr;

        void operator()(Value* v)
        {
            if (i_flag != nullptr) {
                v->~Value();
                *i_flag = false;
            } else {
                delete v;
            }
        }
    };

    using u_ptr = std::unique_ptr<Value, value_deleter>;

    template <typename, typename, typename>
    friend class map;

    template <typename>
    alignas(Value) static char storage[sizeof(Value)];
    template <typename>
    static bool init_flag;
    static std::unordered_map<Key, std::unique_ptr<Value, value_deleter>> runtime_map;
};


template <typename Key, typename Value, typename Tag>
std::unordered_map<Key, typename static_map<Key, Value, Tag>::u_ptr> static_map<Key, Value, Tag>::runtime_map;

template <typename Key, typename Value, typename Tag>
template <typename>
alignas(Value) char static_map<Key, Value, Tag>::storage[sizeof(Value)];

template <typename Key, typename Value, typename Tag>
template <typename>
bool static_map<Key, Value, Tag>::init_flag = false;

//==============================================================================
//==============================================================================
template <typename Key, typename Value, typename Tag = detail::default_tag<Key, Value, false>>
class map {
public:
    ~map()
    {
        clear();
    }

    template <typename Identifier, typename... Args>
    Value& get(Identifier key, Args&... args)
    {
        return staticmap::get(key).get(this, std::forward<Args>(args)...);
    }

    template <typename Identifier>
    bool contains(Identifier key)
    {
        if (staticmap::contains(key))
            return staticmap::get(key).contains(this);

        return false;
    }

    template <typename Identifier>
    void erase(Identifier key)
    {
        if (staticmap::contains(key)) {
            auto& map = staticmap::get(key);
            map.erase(this);

            if (map.size() == 0)
                staticmap::erase(key);
        }
    }

     auto begin() {
        return staticmap::runtime_map.begin();
    }

    auto end() {
        return staticmap::runtime_map.end();
    }

    void clear()
    {
        auto it = staticmap::runtime_map.begin();

        while (it != staticmap::runtime_map.end()) {
            auto& map = *it->second;

            map.erase(this);

            if (map.size() == 0) {
                it = staticmap::runtime_map.erase(staticmap::runtime_map.find(it->first));
                continue;
            }

            ++it;
        }
    }

private:
    using staticmap = static_map<Key, detail::flat_map<map<Key, Value>*, Value>, Tag>;
};

#undef semi_branch_expect

} // namespace semi