#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/log/log.hpp>

#ifdef WIN32

#include "Windows.h"
#include "winerror.h"

namespace arcticdb {

class MemoryMappedFile {
  private:
    HANDLE file_ = INVALID_HANDLE_VALUE;
    HANDLE map_ = nullptr;
    uint64_t* length_ = nullptr;
    uint8_t* base_ = nullptr;
    uint8_t* data_ = nullptr;
    bool writeable_ = false;
    static constexpr size_t header_size = sizeof(uint64_t) + sizeof(uint64_t);

  public:
    MemoryMappedFile() = default;

    size_t get_file_size(const std::string& file_path) {
        LARGE_INTEGER size;
        HANDLE h = CreateFile(
                file_path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr
        );
        if (h == INVALID_HANDLE_VALUE) {
            util::raise_rte("Failed to get file size");
        }
        if (!GetFileSizeEx(h, &size)) {
            CloseHandle(h);
            util::raise_rte("Failed to get file size");
        }
        CloseHandle(h);
        return static_cast<size_t>(size.QuadPart);
    }

    void open_file(const std::string& filepath) {
        size_t total_size = get_file_size(filepath);
        util::check(total_size >= header_size, "File size too small");
        file_ = CreateFile(
                filepath.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr
        );
        if (file_ == INVALID_HANDLE_VALUE) {
            util::raise_rte("Error opening file for reading");
        }

        map_ = CreateFileMapping(file_, nullptr, PAGE_READONLY, 0, 0, nullptr);
        if (!map_) {
            CloseHandle(file_);
            util::raise_rte("Error creating file mapping");
        }

        base_ = static_cast<uint8_t*>(MapViewOfFile(map_, FILE_MAP_READ, 0, 0, total_size));
        if (!base_) {
            CloseHandle(map_);
            CloseHandle(file_);
            util::raise_rte("Error mapping view of file");
        }

        auto header = reinterpret_cast<arcticdb::util::MagicNum<'A', 'r', 'c', 't'>*>(base_);
        header->check();
        length_ = reinterpret_cast<uint64_t*>(base_ + sizeof(uint64_t));
        data_ = base_ + header_size;
    }

    void create_file(const std::string& filepath, size_t size) {
        size_t total_size = header_size + size;
        file_ = CreateFile(
                filepath.c_str(),
                GENERIC_READ | GENERIC_WRITE,
                0,
                nullptr,
                CREATE_ALWAYS,
                FILE_ATTRIBUTE_NORMAL,
                nullptr
        );
        if (file_ == INVALID_HANDLE_VALUE) {
            util::raise_rte("Error opening file for writing");
        }

        map_ = CreateFileMapping(file_, nullptr, PAGE_READWRITE, 0, static_cast<DWORD>(total_size), nullptr);
        if (!map_) {
            CloseHandle(file_);
            util::raise_rte("Error creating file mapping");
        }

        base_ = static_cast<uint8_t*>(MapViewOfFile(map_, FILE_MAP_WRITE, 0, 0, total_size));
        if (!base_) {
            CloseHandle(map_);
            CloseHandle(file_);
            util::raise_rte("Error mapping view of file");
        }

        new (base_) arcticdb::util::MagicNum<'A', 'r', 'c', 't'>();
        *reinterpret_cast<size_t*>(base_ + sizeof(uint64_t)) = size;
        data_ = base_ + header_size;
        length_ = reinterpret_cast<uint64_t*>(base_ + sizeof(uint64_t));
        *length_ = size;
        writeable_ = true;
        ARCTICDB_DEBUG(log::storage(), "Created memory mapped file at {} with size {}", filepath, *length_);
    }

    void unmap() {
        if (base_ != nullptr) {
            if (writeable_ && !FlushViewOfFile(base_, header_size + *length_)) {
                log::storage().warn("Failed to flush view of file");
            }
            if (!UnmapViewOfFile(base_)) {
                log::storage().warn("Failed to unmap view of file");
            }
            base_ = nullptr;
        }
        if (map_) {
            CloseHandle(map_);
            map_ = nullptr;
        }
    }

    void truncate(size_t new_size) {
        if (length_ != nullptr) {
            *length_ = new_size;
        }
        unmap();
        if (file_ != INVALID_HANDLE_VALUE) {
            LARGE_INTEGER li;
            li.QuadPart = header_size + new_size;
            li.LowPart = SetFilePointer(file_, li.LowPart, &li.HighPart, FILE_BEGIN);
            if (li.LowPart == INVALID_SET_FILE_POINTER && GetLastError() != 0UL) {
                util::raise_rte("Error setting file pointer");
            }
            if (!SetEndOfFile(file_)) {
                util::raise_rte("Error truncating file");
            }
        }
    }

    ~MemoryMappedFile() {
        unmap();
        if (file_ != INVALID_HANDLE_VALUE) {
            CloseHandle(file_);
        }
    }

    [[nodiscard]] uint8_t* data() const { return data_; }

    [[nodiscard]] size_t bytes() const { return length_ ? *length_ : 0; }
};

} // namespace arcticdb

#else
#include <iostream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace arcticdb {
class MemoryMappedFile {
  private:
    int fd_ = -1;
    uint64_t* length_ = nullptr;
    uint8_t* base_ = nullptr;
    uint8_t* data_ = nullptr;
    static constexpr size_t header_size = sizeof(uint64_t) + sizeof(uint64_t);

  public:
    ARCTICDB_NO_MOVE_OR_COPY(MemoryMappedFile)

    MemoryMappedFile() = default;

    static size_t get_file_size(const std::string& file_path) {
        struct stat sb{};
        auto result = stat(file_path.c_str(), &sb);
        util::check(result != -1, "Failed to stat file");
        return static_cast<size_t>(sb.st_size);
    }

    void open_file(const std::string& filepath) {
        size_t total_size = get_file_size(filepath);
        util::check(total_size >= header_size, "File size too small");
        fd_ = open(filepath.c_str(), O_RDONLY);
        util::check(fd_ != -1, "Error opening file for reading");
        base_ = static_cast<uint8_t*>(mmap(nullptr, total_size, PROT_READ, MAP_SHARED, fd_, 0));
        if (base_ == MAP_FAILED) {
            close(fd_);
            util::raise_rte("Error memory mapping the file");
        }
        auto header = reinterpret_cast<arcticdb::util::MagicNum<'A', 'r', 'c', 't'>*>(base_);
        header->check();
        length_ = reinterpret_cast<uint64_t*>(base_ + sizeof(uint64_t));
        data_ = base_ + header_size;
    }

    void create_file(const std::string& filepath, size_t size) {
        size_t total_size = header_size + size;
        fd_ = open(filepath.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        util::check(fd_ != -1, "Error opening file for writing");
        auto result = lseek(fd_, static_cast<long>(total_size - 1), SEEK_SET);
        if (result == -1) {
            close(fd_);
            util::raise_rte("Failed to extend memory mapped file");
        }
        result = write(fd_, "", 1);
        if (result == -1) {
            close(fd_);
            util::raise_rte("Error writing last byte of the file");
        }
        base_ = static_cast<uint8_t*>(mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (base_ == MAP_FAILED) {
            close(fd_);
            util::raise_rte("Error memory mapping the file");
        }
        new (base_) arcticdb::util::MagicNum<'A', 'r', 'c', 't'>();
        *reinterpret_cast<uint64_t*>(base_ + sizeof(uint64_t)) = size;
        data_ = base_ + header_size;
        length_ = reinterpret_cast<uint64_t*>(base_ + sizeof(uint64_t));
        *length_ = size;
        ARCTICDB_DEBUG(log::storage(), "Created memory mapped file at {} with size {}", filepath, *length_);
    }

    void unmap() {
        if (base_ != nullptr) {
            auto result = msync(base_, header_size + *length_, MS_SYNC);
            if (result == -1) {
                log::storage().warn("Could not sync the file to disk: {}", result);
            } else {
                result = munmap(base_, header_size + *length_);
                if (result == -1)
                    log::storage().warn("Error un-memory mapping the file");
            }
        }
    }

    void truncate(size_t new_size) {
        *length_ = new_size;
        unmap();
        auto result = ftruncate(fd_, static_cast<long>(header_size + new_size));
        util::check(result != -1, "Error truncating file");
        base_ = nullptr;
        ARCTICDB_DEBUG(log::storage(), "Truncated memory-mapped file to size {}", new_size);
    }

    ~MemoryMappedFile() {
        unmap();
        ARCTICDB_DEBUG(log::storage(), "Closing memory-mapped file");
        if (fd_ != -1)
            close(fd_);
    }

    [[nodiscard]] uint8_t* data() const { return data_; }

    [[nodiscard]] size_t bytes() const { return *length_; }
};

} // namespace arcticdb

#endif