#pragma once

#include <arcticdb/storage/storage.hpp>

namespace arcticdb::storage {

struct KeyData {
    uint64_t key_offset_;
    uint64_t key_size_;
};

class SingleFileStorage : public Storage {
  public:
    SingleFileStorage(const LibraryPath& lib, OpenMode mode) : Storage(lib, mode) {}

    std::string name() const = 0;

    void write_raw(const uint8_t* data, size_t bytes) { do_write_raw(data, bytes); }

    uint8_t* read_raw(size_t offset, size_t bytes) { return do_read_raw(offset, bytes); }

    size_t get_offset() const { return do_get_offset(); }

    size_t get_bytes() const { return do_get_bytes(); }

    void finalize(KeyData key_data) { do_finalize(key_data); }

    void load_header(size_t header_offset, size_t header_size) { return do_load_header(header_offset, header_size); }

  private:
    virtual uint8_t* do_read_raw(size_t offset, size_t bytes) = 0;

    virtual size_t do_get_bytes() const = 0;

    virtual void do_write_raw(const uint8_t* data, size_t bytes) = 0;

    virtual size_t do_get_offset() const = 0;

    virtual void do_finalize(KeyData) = 0;

    virtual void do_load_header(size_t header_offset, size_t header_size) = 0;
};
} // namespace arcticdb::storage

namespace fmt {
template<>
struct formatter<arcticdb::storage::KeyData> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::storage::KeyData& k, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}:{}", k.key_offset_, k.key_size_);
    }
};

} // namespace fmt
