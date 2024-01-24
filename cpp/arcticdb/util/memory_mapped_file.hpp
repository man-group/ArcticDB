#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/log/log.hpp>

#ifdef WIN32

#include <windows.h>
#include <iostream>

class MemoryMappedFile {
private:
    HANDLE file_handle_;
    HANDLE map_handle_;
    LPVOID map_view_;
    size_t length_;

public:
    MemoryMappedFile(const char* filepath, size_t size)
        : file_handle_(INVALID_HANDLE_VALUE),
          map_handle_(nullptr),
          map_view_(nullptr),
          length_(size) {
        file_handle_ = CreateFileA(filepath,
                                 GENERIC_READ | GENERIC_WRITE,
                                 0,
                                 nullptr,
                                 OPEN_ALWAYS,
                                 FILE_ATTRIBUTE_NORMAL,
                                 nullptr);
        util::check(file_handle_ != INVALID_HANDLE_VALUE, "Error opening file");

        LARGE_INTEGER largeSize;
        largeSize.QuadPart = size;
        if (!SetFilePointerEx(file_handle_, largeSize, nullptr, FILE_BEGIN)) {
            CloseHandle(file_handle_);
            util::raise_rte("Error moving file pointer");
        }
        
        if (!SetEndOfFile(file_handle_)) {
            CloseHandle(file_handle_);
            util::raise_rte("Error setting end of file");
        }

        map_handle_ = CreateFileMappingA(file_handle_,
                                       nullptr,
                                       PAGE_READWRITE,
                                       0,
                                       size,
                                       nullptr);
        if (map_handle_ == nullptr) {
            CloseHandle(file_handle_);
            util::raise("Error opening map handle");
        }

        // Map view of file
        map_view_ = MapViewOfFile(map_handle_,
                                FILE_MAP_ALL_ACCESS,
                                0,
                                0,
                                size);
        if (map_view_ == nullptr) {
            CloseHandle(map_handle_);
            CloseHandle(file_handle_);
            util::raise("Error mapping view of file");
        }
    }

    ~MemoryMappedFile() {
        if (map_view_ != nullptr) 
            UnmapViewOfFile(map_view_);
        
        if (map_handle_ != nullptr) 
            CloseHandle(map_handle_);
        
        if (file_handle_ != INVALID_HANDLE_VALUE) 
            CloseHandle(file_handle_);
    }

    uint8_t* data() const {
        return static_cast<uint8_t*>(map_view_);
    }

    size_t size() const {
        return length_;
    }
};

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
    
    MemoryMappedFile(const std::string &filepath, size_t size)
        : length_(size) {
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
    }

    void truncate(size_t new_size) {
        auto result = munmap(data_, length_);
        util::check(result != -1, "Error un-mapping file");

        result = ftruncate(fd_, new_size);
        util::check(result != -1, "Error truncating file");

        length_ = new_size;
        data_ = nullptr;
    }


    ~MemoryMappedFile() {
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

        if (fd_ != -1) {
            close(fd_);
        }
    }

    [[nodiscard]] uint8_t *data() const {
        return data_;
    }

    [[nodiscard]] size_t bytes() const {
        return length_;
    }

    // Other methods to interact with the memory-mapped file could be added here
};

} //namespace arcticdb

#endif