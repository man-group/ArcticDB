#pragma once
#include <fmt/format.h>

#include <arcticdb/column_store/memory_segment.hpp>

namespace fmt {
template<>
struct formatter<arcticdb::SegmentInMemory> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format([[maybe_unused]] const arcticdb::SegmentInMemory& segment, FormatContext& ctx) const {
#if ARCTICDB_ENABLE_SEGMENT_PRETTY_PRINT
        const StreamDescriptor& desc = segment.descriptor();
        auto out = fmt::format_to(ctx.out(), "Segment\n");
        for (unsigned i = 0; i < desc.field_count(); ++i) {
            out = fmt::format_to(out, "\nColumn[{}]: {}\n", i, desc.field(i));
            const arcticdb::Column& column = segment.column(i);
            details::visit_type(column.type().data_type(), [&](auto tag) {
                using type_info = ScalarTypeInfo<decltype(tag)>;
                auto input_data = column.data();
                auto it = input_data.begin<typename type_info::TDT>();
                while (it != input_data.end<typename type_info::TDT>()) {
                    if constexpr (is_sequence_type(type_info::data_type)) {
                        const std::string_view str = [&]() -> std::string_view {
                            if (arcticdb::is_a_string(*it)) {
                                return segment.string_at_offset(*it);
                            } else if (arcticdb::not_a_string() == *it) {
                                return "\"None\"";
                            }
                            return "\"NaN\"";
                        }();
                        fmt::format_to(out, "{} ", str);
                    } else if constexpr (std::same_as<char, typename type_info::RawType>) {
                        // This is to prevent printing int8_t values as ASCII characters
                        fmt::format_to(out, "{} ", static_cast<int>(*it));
                    } else {
                        fmt::format_to(out, "{} ", *it);
                    }
                    ++it;
                }
            });
        }
        return out;
#else
        return ctx.out();
#endif
    }
};
} // namespace fmt
