#include <gtest/gtest.h>


#include <fstream>
#include <vector>

#include <arcticdb/log/log.hpp>
#include <lz4.h>
#include <arcticdb/util/timer.hpp>
#include <arcticdb/codec/run_length_encoding.hpp>
#include <arcticdb/codec/null_encoding.hpp>
#include <arcticdb/codec/bitpacking.hpp>

#include <arcticdb/codec/thirdparty/fastpforlib/pfor.hpp>

#include <arcticdb/codec/thirdparty/middleout/middleout.hpp>

#include <random>

std::vector<double> random_double(size_t size) {
    std::vector<double> vec(size);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    for (auto &val: vec) {
        val = dis(gen);
    }

    return vec;
}

std::vector<double> stock_prices_double() {
    std::ifstream infile("/opt/arcticdb/arcticdb_link/cpp/arcticdb/codec/test/data/ibm.data");

    std::vector<double> data;

    std::string line;
    size_t count = 0;
    while (std::getline(infile, line)) {
        // scale to integer - with no decimal fraction
        double price = std::stod(line);
        data.push_back(price);
        if(++count == 100000)
            break;
    }
   // std::cout << std::endl << "13 MB of stock market data - IBM" << std::endl;
    return data;
}

std::vector<double> get_double() {
    return stock_prices_double();
    //return random_double(100000);
}

std::vector<int64_t> stock_prices_int() {
    std::ifstream infile("/opt/arcticdb/arcticdb_link/cpp/arcticdb/codec/test/data/ibm.data");

    std::vector<int64_t> data;

    std::string line;
    size_t count = 0;
    while (std::getline(infile, line)) {
        // scale to integer - with no decimal fraction
        int64_t price = std::stod(line);
        data.push_back(price);
        if(++count == 100000)
            break;
    }

    // std::cout << std::endl << "13 MB of stock market data - IBM" << std::endl;
    return data;
}

std::vector<uint32_t> stock_prices_uint32() {
    std::ifstream infile("/opt/arcticdb/arcticdb_link/cpp/arcticdb/codec/test/data/ibm.data");

    std::vector<uint32_t> data;

    std::string line;
    size_t count = 0;
    while (std::getline(infile, line)) {
        // scale to integer - with no decimal fraction
        uint32_t price = std::stod(line);
        data.push_back(price);
        if(++count == 100000)
            break;
    }

    // std::cout << std::endl << "13 MB of stock market data - IBM" << std::endl;
    return data;
}

//    std::vector<char> compressedData(middleout::maxCompressedSize(data.size()));
//
//    std::vector<double> outData(data.size());

TEST(CompressionBenchmark, LZ4) {
    using namespace arcticdb;
    auto data = get_double();
    const auto bytes = data.size() * sizeof(double);
    auto size = LZ4_compressBound(static_cast<int>(bytes));

    interval_timer timer;
    std::vector<uint8_t> compressed(size);
    timer.start_timer("Compress");
    int compressed_bytes = LZ4_compress_default(
            reinterpret_cast<const char *>(data.data()),
            reinterpret_cast<char *>(compressed.data()),
            int(bytes),
            int(compressed.size()));

    timer.stop_timer("Compress");

    std::vector<double> uncompressed(data.size());
    timer.start_timer("Decompress");
    (void)LZ4_decompress_safe(
            reinterpret_cast<const char*>(compressed.data()),
            reinterpret_cast<char*>(uncompressed.data()),
            int(compressed_bytes),
            int(bytes)
    );
    timer.stop_timer("Decompress");
    log::version().info("LZ4 {} bytes: {}", compressed_bytes, timer.display_all());
}

TEST(CompressionBenchmark, LZ4Int) {
    using namespace arcticdb;
    auto data = stock_prices_int();
    const auto bytes = data.size() * sizeof(int64_t);
    auto size = LZ4_compressBound(static_cast<int>(bytes));

    interval_timer timer;
    std::vector<uint8_t> compressed(size);
    timer.start_timer("Compress");
    int compressed_bytes = LZ4_compress_default(
            reinterpret_cast<const char *>(data.data()),
            reinterpret_cast<char *>(compressed.data()),
            int(bytes),
            int(compressed.size()));

    timer.stop_timer("Compress");

    std::vector<int64_t> uncompressed(data.size());
    timer.start_timer("Decompress");
    (void)LZ4_decompress_safe(
            reinterpret_cast<const char*>(compressed.data()),
            reinterpret_cast<char*>(uncompressed.data()),
            int(compressed_bytes),
            int(bytes)
    );
    timer.stop_timer("Decompress");
    log::version().info("LZ4 integer {} bytes: {}", compressed_bytes, timer.display_all());
}

TEST(PFOR, LZ4UInt32) {
    using namespace arcticdb;
    auto data = stock_prices_uint32();
    const auto bytes = data.size() * sizeof(uint32_t);
    auto size = LZ4_compressBound(static_cast<int>(bytes));

    interval_timer timer;
    std::vector<uint8_t> compressed(size);
    timer.start_timer("Compress");
    int compressed_bytes = LZ4_compress_default(
        reinterpret_cast<const char *>(data.data()),
        reinterpret_cast<char *>(compressed.data()),
        int(bytes),
        int(compressed.size()));

    timer.stop_timer("Compress");

    std::vector<uint32_t> uncompressed(data.size());
    timer.start_timer("Decompress");
    (void)LZ4_decompress_safe(
        reinterpret_cast<const char*>(compressed.data()),
        reinterpret_cast<char*>(uncompressed.data()),
        int(compressed_bytes),
        int(bytes)
    );
    timer.stop_timer("Decompress");
    log::version().info("LZ4 uint32 {} bytes: {}", compressed_bytes, timer.display_all());
}

/*
TEST(CompressionBenchmark, RLE) {
    using namespace arcticdb;
    auto data = get_double();
    const auto bytes = data.size() * sizeof(double);
    RunLengthEncoding<double> encoding;
    auto size = encoding.max_required_bytes(data.data(), bytes);

    interval_timer timer;
    std::vector<uint8_t> compressed(*size);
    timer.start_timer("Compress");
    int compressed_bytes = encoding.encode(data.data(), data.size(), compressed.data());

    timer.stop_timer("Compress");

    std::vector<double> uncompressed(data.size());
    timer.start_timer("Decompress");
    encoding.decode(compressed.data(), compressed_bytes, uncompressed.data());
    timer.stop_timer("Decompress");
    log::version().info("RLE {} bytes: {}", compressed_bytes, timer.display_all());
}
*/

TEST(CompressionBenchmark, Null) {
    using namespace arcticdb;
    auto data = get_double();
    const auto bytes = data.size() * sizeof(double);
    NullEncoding<double> encoding;
    auto size = encoding.max_required_bytes(data.data(), bytes);

    interval_timer timer;
    std::vector<uint8_t> compressed(*size);
    timer.start_timer("Compress");
    int compressed_bytes = encoding.encode(data.data(), data.size(), compressed.data());

    timer.stop_timer("Compress");

    std::vector<double> uncompressed(data.size());
    timer.start_timer("Decompress");
    encoding.decode(compressed.data(), compressed_bytes, uncompressed.data());
    timer.stop_timer("Decompress");
    log::version().info("Null {} bytes: {}", compressed_bytes, timer.display_all());
}

/*
TEST(CompressionBenchmark, RLEInt) {
    using namespace arcticdb;
    auto data = stock_prices_int();
    const auto bytes = data.size() * sizeof(int64_t);
    RunLengthEncoding<int64_t> encoding;
    auto size = encoding.max_required_bytes(data.data(), bytes);

    interval_timer timer;
    std::vector<uint8_t> compressed(*size);
    timer.start_timer("Compress");
    int compressed_bytes = encoding.encode(data.data(), data.size(), compressed.data());

    timer.stop_timer("Compress");

    std::vector<int64_t> uncompressed(data.size());
    timer.start_timer("Decompress");
    encoding.decode(compressed.data(), compressed_bytes, uncompressed.data());
    timer.stop_timer("Decompress");
    log::version().info("RLE integer {} bytes: {}", compressed_bytes, timer.display_all());
}
*/
TEST(CompressionBenchmark, MiddleOut) {
    using namespace arcticdb;
    auto data = get_double();
    auto size = middleout::maxCompressedSize(data.size());

    interval_timer timer;
    std::vector<char> compressed(size);
    timer.start_timer("Compress");
    int compressed_bytes = middleout::compress(data, compressed);

    timer.stop_timer("Compress");

    std::vector<double> uncompressed(data.size());
    timer.start_timer("Decompress");
    middleout::decompress(compressed, data.size(), uncompressed);
    timer.stop_timer("Decompress");
    log::version().info("Middleout {} bytes: {}", compressed_bytes, timer.display_all());
}


TEST(CompressionBenchmark, MiddleOutInt) {
    using namespace arcticdb;
    auto data = stock_prices_int();
    auto size = middleout::maxCompressedSize(data.size());

    interval_timer timer;
    std::vector<char> compressed(size);
    timer.start_timer("Compress");
    int compressed_bytes = middleout::compress(data, compressed);

    timer.stop_timer("Compress");

    std::vector<int64_t> uncompressed(data.size());
    timer.start_timer("Decompress");
    middleout::decompress(compressed, data.size(), uncompressed);
    timer.stop_timer("Decompress");
    log::version().info("Middleout integer {} bytes: {}", compressed_bytes, timer.display_all());
}


TEST(CompressionBenchmark, BitpackingInt) {
    using namespace arcticdb;
    auto data = stock_prices_int();
    const auto bytes = data.size() * sizeof(int64_t);
    auto size = BitpackingPrimitives::GetRequiredSize(bytes, 8);

    interval_timer timer;
    std::vector<uint8_t> compressed(size);
    timer.start_timer("Compress");
    BitpackingPrimitives::PackBuffer(compressed.data(), data.data(), data.size(), 8);

    timer.stop_timer("Compress");

    std::vector<int64_t> uncompressed(data.size());
    timer.start_timer("Decompress");
    BitpackingPrimitives::UnPackBuffer<uint64_t>(reinterpret_cast<uint8_t*>(uncompressed.data()), compressed.data(), data.size(), 8);

    timer.stop_timer("Decompress");
    log::version().info("Bitpack integer: {}", timer.display_all());
}

TEST(PFOR, PFORUInt32) {
    using namespace arcticdb;
    auto data = stock_prices_uint32();
    data.resize(99968);
    interval_timer timer;
    std::vector<uint32_t> compressed(data.size());
    timer.start_timer("Compress");
    arcticdb_pforlib::PFor pfor;
    size_t nvalue = compressed.size();
    pfor.encodeArray(data.data(), data.size(), compressed.data(), nvalue);

    timer.stop_timer("Compress");

    std::vector<uint32_t> uncompressed(data.size());
    timer.start_timer("uncompressed");
    size_t recoveredsize = uncompressed.size();
    pfor.decodeArray(compressed.data(), compressed.size(), uncompressed.data(), recoveredsize);
    timer.stop_timer("Decompress");
    log::version().info("PFOR uint32_t: {} bytes {}", nvalue, timer.display_all());
}