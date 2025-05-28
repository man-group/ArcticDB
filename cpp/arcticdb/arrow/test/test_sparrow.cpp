#include <gtest/gtest.h>

#include <arcticdb/util/test/generators.hpp>
#include <sparrow/sparrow.hpp>
#include <sparrow/record_batch.hpp>
#include <sparrow/layout/primitive_layout/primitive_data_access.hpp>

template <typename T>
sparrow::primitive_array<T> create_primitive_array(
        T* data_ptr,
        size_t data_size,
        std::optional<sparrow::validity_bitmap>& validity_bitmap) {
    sparrow::u8_buffer<T> buffer(data_ptr, data_size);
    if(validity_bitmap) {
        return sparrow::primitive_array<T>{std::move(buffer), data_size, std::move(*validity_bitmap)};
    } else {
        return sparrow::primitive_array<T>{std::move(buffer), data_size};
    }
}

template <>
sparrow::primitive_array<bool> create_primitive_array(
        bool* data_ptr,
        size_t data_size,
        std::optional<sparrow::validity_bitmap>& validity_bitmap) {
    auto buffer = sparrow::details::primitive_data_access<bool>::make_data_buffer(std::span{data_ptr, data_size});
    if(validity_bitmap) {
        return sparrow::primitive_array<bool>{std::move(buffer), data_size, std::move(*validity_bitmap)};
    } else {
        return sparrow::primitive_array<bool>{std::move(buffer), data_size};
    }
}

template <typename T>
sparrow::array create_arrow_array(
        T* data_ptr,
        size_t data_size,
        std::string_view name,
        std::optional<sparrow::validity_bitmap> maybe_bitmap) {
    auto primitive_array = create_primitive_array<T>(data_ptr, data_size, maybe_bitmap);
    auto arr = sparrow::array{std::move(primitive_array)};
    arr.set_name(name);
    return arr;
}

template <typename T>
sparrow::dictionary_encoded_array<T> create_dict_array(
    sparrow::array&& dict_values_array,
    sparrow::u8_buffer<T>&& dict_keys_buffer,
    std::optional<sparrow::validity_bitmap>& validity_bitmap
    ) {
    if(validity_bitmap) {
        return sparrow::dictionary_encoded_array<T>{
            typename sparrow::dictionary_encoded_array<T>::keys_buffer_type(std::move(dict_keys_buffer)),
            std::move(dict_values_array),
            std::move(*validity_bitmap)
        };
    } else {
        return sparrow::dictionary_encoded_array<T>{
            typename sparrow::dictionary_encoded_array<T>::keys_buffer_type(std::move(dict_keys_buffer)),
            std::move(dict_values_array),
        };
    }
}

sparrow::array create_string_dict_array(
        int32_t* string_offsets_ptr,
        size_t string_offsets_size,
        char* string_buffer_ptr,
        size_t string_buffer_size,
        int32_t* keys_ptr,
        size_t keys_size,
        std::string_view name,
        std::optional<sparrow::validity_bitmap> maybe_bitmap) {

    sparrow::u8_buffer<int32_t> offset_buffer(string_offsets_ptr, string_offsets_size);
    sparrow::u8_buffer<char> strings_buffer(string_buffer_ptr, string_buffer_size);

    sparrow::u8_buffer<int32_t> dict_keys_buffer{keys_ptr, keys_size};

    sparrow::string_array dict_values_array(
        std::move(strings_buffer),
        std::move(offset_buffer)
    );

    auto dict_encoded = create_dict_array<int32_t>(
        sparrow::array{std::move(dict_values_array)},
        std::move(dict_keys_buffer),
        maybe_bitmap
    );

    sparrow::array arr{std::move(dict_encoded)};
    arr.set_name(name);
    return arr;
}


TEST(Sparrow, BasicIntArray) {
    const size_t data_size = 20;
    uint16_t* data = new uint16_t[data_size];
    for (auto i = 0u; i < data_size; ++i)
    {
        data[i] = static_cast<uint16_t>(i);
    }
    auto arr = create_arrow_array<uint16_t>(data, data_size, "arr", std::optional<sparrow::validity_bitmap>{});
    EXPECT_EQ(arr.size(), data_size);
}

TEST(Sparrow, BasicStringArray) {
    const size_t string_size = 20;
    char* data = new char[string_size];
    for (auto i = 0u; i < string_size; ++i)
    {
        data[i] = static_cast<char>(i);
    }

    const size_t num_strings = 4;
    int32_t* string_offsets = new int32_t[num_strings + 1];
    string_offsets[0] = 0;
    string_offsets[1] = 7;
    string_offsets[2] = 11;
    string_offsets[3] = 12;
    string_offsets[4] = 20;

    const size_t num_rows = 15;
    int32_t* row_keys = new int32_t[num_rows];
    for (auto i = 0u; i < num_rows; ++i)
    {
        row_keys[i] = static_cast<int32_t>(i % num_strings);
    }

    auto arr = create_string_dict_array(
        string_offsets,
        num_strings+1,
        data,
        string_size,
        row_keys,
        num_rows,
        "arr",
        std::optional<sparrow::validity_bitmap>{});

    EXPECT_EQ(arr.size(), num_rows);
}
