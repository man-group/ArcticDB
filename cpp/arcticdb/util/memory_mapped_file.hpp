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
        size_t length_ = 0;
        uint8_t* data_ = nullptr;

    public:
        MemoryMappedFile() = default;

        size_t get_file_size(const std::string& file_path) {
            LARGE_INTEGER size;
            HANDLE h = CreateFile(file_path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
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
            length_ = get_file_size(filepath);
            file_ = CreateFile(filepath.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (file_ == INVALID_HANDLE_VALUE) {
                util::raise_rte("Error opening file for reading");
            }

            map_ = CreateFileMapping(file_, nullptr, PAGE_READONLY, 0, 0, nullptr);
            if (!map_) {
                CloseHandle(file_);
                util::raise_rte("Error creating file mapping");
            }

            data_ = static_cast<uint8_t*>(MapViewOfFile(map_, FILE_MAP_READ, 0, 0, length_));
            if (!data_) {
                CloseHandle(map_);
                CloseHandle(file_);
                util::raise_rte("Error mapping view of file");
            }
        }

        void create_file(const std::string& filepath, size_t size) {
            length_ = size;
            file_ = CreateFile(filepath.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (file_ == INVALID_HANDLE_VALUE) {
                util::raise_rte("Error opening file for writing");
            }

            map_ = CreateFileMapping(file_, nullptr, PAGE_READWRITE, 0, static_cast<DWORD>(size), nullptr);
            if (!map_) {
                CloseHandle(file_);
                util::raise_rte("Error creating file mapping");
            }

            data_ = static_cast<uint8_t*>(MapViewOfFile(map_, FILE_MAP_WRITE, 0, 0, size));
            if (!data_) {
                CloseHandle(map_);
                CloseHandle(file_);
                util::raise_rte("Error mapping view of file");
            }
        }

        void unmap() {
            if (data_) {
                if (!UnmapViewOfFile(data_)) {
                    log::storage().warn("Failed to unmap view of file");
                }
                data_ = nullptr;
            }
            if (map_) {
                CloseHandle(map_);
                map_ = nullptr;
            }
        }

        void truncate(size_t new_size) {
            unmap();
            if (file_ != INVALID_HANDLE_VALUE) {
                // Set file pointer to the new truncated size
                LARGE_INTEGER li;
                li.QuadPart = new_size;
                li.LowPart = SetFilePointer(file_, li.LowPart, &li.HighPart, FILE_BEGIN);
                if (li.LowPart == INVALID_SET_FILE_POINTER && GetLastError() != 0UL) {
                    util::raise_rte("Error setting file pointer");
                }
                // Truncate the file
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

        [[nodiscard]] uint8_t* data() const {
            return data_;
        }

        [[nodiscard]] size_t bytes() const {
            return length_;
        }
    };

}//namespace arcticdb

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
    size_t length_ = 0;
    uint8_t *data_ = nullptr;

public:
    ARCTICDB_NO_MOVE_OR_COPY(MemoryMappedFile)

    MemoryMappedFile() = default;

    size_t get_file_size(const std::string& file_path) {
        struct stat sb;
        auto result = stat(file_path.c_str(), &sb);
        util::check(result != -1, "Failed to stat file");
        return static_cast<size_t>(sb.st_size);
    }

    void open_file(const std::string &filepath) {
        length_ = get_file_size(filepath);
        // Open file
        fd_ = open(filepath.c_str(), O_RDONLY);
        util::check(fd_ != -1, "Error opening file for reading");

        // Map file into memory
        ARCTICDB_DEBUG(log::storage(), "Memory-mapped file at path {} with length {}", filepath, length_);
        data_ = static_cast<uint8_t *>(mmap(nullptr, length_, PROT_READ, MAP_SHARED, fd_, 0));
        if (data_ == MAP_FAILED) {
            close(fd_);
            util::raise_rte("Error mmapping the file");
        }
    }

    void create_file(const std::string &filepath, size_t size) {
        length_ = size;
        // Open file
        fd_ = open(filepath.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        util::check(fd_ != -1, "Error opening file for writing");

        auto result = lseek(fd_, static_cast<long>(length_ - 1), SEEK_SET);
        if (result == -1) {
            close(fd_);
            util::raise_rte("Failed to extend memory mapped file: {}", result);
        }

        result = write(fd_, "", 1);
        if (result == -1) {
            close(fd_);
            util::raise_rte("Error writing last byte of the file: {}", result);
        }

        // Map file into memory
        data_ = static_cast<uint8_t *>(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (data_ == MAP_FAILED) {
            close(fd_);
            util::raise_rte("Error mmapping the file");
        }
        ARCTICDB_DEBUG(log::storage(), "Created memory mapped file at {} with size {}", filepath, length_);
    }

    void unmap() {
        if (data_ != nullptr) {
            auto result = msync(data_, length_, MS_SYNC);
            if(result == -1) {
                log::storage().warn("Could not sync the file to disk: {}", result);
            } else {
                result = munmap(data_, length_);
                if (result == -1)
                    log::storage().warn("Error un-mmapping the file");
            }
        }
    }

    void truncate(size_t new_size) {
        length_ = new_size;
        unmap();

        auto result = ftruncate(fd_, new_size);
        util::check(result != -1, "Error truncating file");

        data_ = nullptr;
        ARCTICDB_DEBUG(log::storage(), "Truncated memory-mapped file to size {}", length_);
    }


    ~MemoryMappedFile() {
        unmap();

        ARCTICDB_DEBUG(log::storage(), "Closing memory-mapped file of length {}", length_);
        if (fd_ != -1)
            close(fd_);

    }

    [[nodiscard]] uint8_t *data() const {
        return data_;
    }

    [[nodiscard]] size_t bytes() const {
        return length_;
    }
};

} //namespace arcticdb

#endif