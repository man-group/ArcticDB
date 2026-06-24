/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

/**
 * Tests that verify the CVE-2019-16224..16228 patches to LMDB reject
 * crafted data.mdb files instead of crashing.
 *
 * NVD descriptions (all CRITICAL/HIGH, all require attacker-supplied data.mdb):
 *   CVE-16224 (CRITICAL 9.8) — "certain values of md_flags, mdb_node_add does not
 *     properly set up a memcpy destination, leading to an invalid write operation"
 *   CVE-16225 (CRITICAL 9.8) — "certain values of mp_flags, mdb_page_touch does not
 *     properly set up mc->mc_pg[mc->top], leading to an invalid write operation"
 *   CVE-16226 (HIGH 7.5)     — "mdb_node_del does not validate a memmove in the case
 *     of an unexpected node->mn_hi, leading to an invalid write operation"
 *   CVE-16227 (CRITICAL 9.8) — "certain values of mn_flags, mdb_cursor_set triggers a
 *     memcpy with an invalid write operation within mdb_xcursor_init1"
 *   CVE-16228 (HIGH 7.5)     — "divide-by-zero error in mdb_env_open2 if
 *     mdb_env_read_header obtains a zero value for a certain size field"
 *
 * Each test mirrors the repro scenario from jnwatson/py-lmdb PR#429
 * (misc/cve-2019-*-repro.c): create a valid database, corrupt specific bytes,
 * re-open and attempt the same operations as the repro, then assert the patched
 * behaviour (clean error return) rather than the pre-patch behaviour (crash).
 *
 * Where each patch check fires (important for understanding the assertions):
 *   CVE-16224: mdb_txn_renew0 (called by mdb_txn_begin) — mdb_env_open succeeds
 *   CVE-16228: mdb_env_open2  (called by mdb_env_open)  — mdb_env_open fails
 *   CVE-16225: mdb_page_get early-rejects P_DIRTY pages — mdb_put/del returns MDB_CORRUPTED
 *              (vulnerable function per NVD is mdb_page_touch; our patch is the guard in mdb_page_get)
 *   CVE-16226: mdb_node_del sets MDB_TXN_ERROR          — mdb_txn_commit returns MDB_BAD_TXN
 *   CVE-16227: mdb_cursor_first (and other cursor ops)  — returns MDB_CORRUPTED
 *              (NVD names mdb_cursor_set as the trigger; mdb_xcursor_init1 is where the write occurs)
 *
 * data.mdb binary layout used by the corruption helpers (64-bit little-endian):
 *
 *   Page N starts at file offset N * mm_psize.
 *   Page 0 and 1 are meta pages; data pages start at page 2.
 *
 *   Meta page layout:
 *     +0  .. +7    mp_pgno      (uint64)
 *     +8  .. +9    mp_pad       (uint16)
 *     +10 .. +11   mp_flags     (uint16: P_META=0x08)
 *     +12 .. +15   mp_lower / mp_upper
 *     -- MDB_meta starts at +16 --
 *     +40 .. +43   mm_dbs[FREE_DBI].md_pad  = mm_psize  (uint32)
 *     +44 .. +45   mm_dbs[FREE_DBI].md_flags             (uint16)
 *     +92 .. +93   mm_dbs[MAIN_DBI].md_flags             (uint16)
 *
 *   Leaf page layout:
 *     +10 .. +11   mp_flags  (P_LEAF=0x02, P_DIRTY=0x10)
 *     +16 .. +17   mp_ptrs[0] — byte offset of first MDB_node within page
 *
 *   MDB_node layout (at page_base + mp_ptrs[i]):
 *     +0  .. +1    mn_lo    (low 16 bits of data size)
 *     +2  .. +3    mn_hi    (high 16 bits of data size)       CVE-16226
 *     +4  .. +5    mn_flags (F_DUPDATA=0x04)                 CVE-16227
 *     +6  .. +7    mn_ksize (key size)
 *     +8  ..       key bytes, then value bytes
 */

#include <gtest/gtest.h>

#include <lmdb.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

// ---- Binary I/O helpers ------------------------------------------------

template<typename T>
void patch(const fs::path& db, long offset, T val) {
    std::fstream f(db, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(f.is_open()) << "Cannot open " << db;
    f.seekp(offset);
    f.write(reinterpret_cast<const char*>(&val), sizeof(val));
}

uint32_t read_u32(const fs::path& db, long offset) {
    std::ifstream f(db, std::ios::binary);
    uint32_t val = 0;
    f.seekg(offset);
    f.read(reinterpret_cast<char*>(&val), sizeof(val));
    return val;
}

std::vector<uint8_t> read_file(const fs::path& path) {
    std::ifstream f(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

void write_file(const fs::path& path, const std::vector<uint8_t>& data) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    f.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
}

// ---- Page layout constants ---------------------------------------------

constexpr long PSIZE_OFFSET = 40;      // mm_dbs[FREE_DBI].md_pad = mm_psize (uint32)
constexpr long FREE_FLAGS_OFFSET = 44; // mm_dbs[FREE_DBI].md_flags (uint16)
constexpr long MAIN_FLAGS_OFFSET = 92; // mm_dbs[MAIN_DBI].md_flags (uint16)
constexpr int MP_FLAGS_OFFSET = 10;    // mp_flags within page (uint16)
constexpr int MP_PTRS0_OFFSET = 16;    // mp_ptrs[0] within page (uint16)
// These three flags are internal to mdb.c and not exported by lmdb.h.
// Values are taken directly from mdb.c and must be kept in sync if LMDB is upgraded.
constexpr uint16_t P_LEAF = 0x02;         // mp_flags: leaf page (mdb.c: P_LEAF)
constexpr uint16_t P_DIRTY = 0x10;        // mp_flags: in-memory dirty marker (mdb.c: P_DIRTY)
constexpr uint16_t F_DUPDATA_FLAG = 0x04; // mn_flags: node has dup data (mdb.c: F_DUPDATA)

// ---- LMDB environment helper -------------------------------------------

struct TempLmdbEnv {
    fs::path dir;
    fs::path db_path;
    uint32_t psize = 0;

    explicit TempLmdbEnv(std::string_view name) {
        dir = fs::temp_directory_path() / ("arcticdb_lmdb_cve_" + std::string(name));
        fs::remove_all(dir);
        fs::create_directories(dir);
        db_path = dir / "data.mdb";
    }

    ~TempLmdbEnv() { fs::remove_all(dir); }

    // Write n_keys entries ("key0".."keyN-1", each value_size bytes of 'x').
    // mapsize defaults to 1 MiB; pass 4 MiB when n_keys * value_size is large.
    bool populate(int n_keys, size_t value_size = 3, uint64_t mapsize = 1ULL << 20) {
        MDB_env* env = nullptr;
        if (mdb_env_create(&env) != MDB_SUCCESS)
            return false;
        mdb_env_set_mapsize(env, mapsize);
        if (mdb_env_open(env, dir.string().c_str(), 0, 0644) != MDB_SUCCESS) {
            mdb_env_close(env);
            return false;
        }
        MDB_txn* txn = nullptr;
        MDB_dbi dbi;
        mdb_txn_begin(env, nullptr, 0, &txn);
        mdb_dbi_open(txn, nullptr, MDB_CREATE, &dbi);
        const std::string val_str(value_size, 'x');
        for (int i = 0; i < n_keys; ++i) {
            std::string k = "key" + std::to_string(i);
            MDB_val key{k.size(), k.data()};
            MDB_val val{val_str.size(), const_cast<char*>(val_str.data())};
            mdb_put(txn, dbi, &key, &val, 0);
        }
        mdb_txn_commit(txn);
        mdb_env_close(env);
        psize = read_u32(db_path, PSIZE_OFFSET);
        return psize > 0;
    }

    // Find the file offset of the first leaf page (after the two meta pages).
    // Returns -1 if not found.
    long first_leaf_page_offset() const {
        auto data = read_file(db_path);
        for (long off = 2 * static_cast<long>(psize); off + static_cast<long>(psize) <= static_cast<long>(data.size());
             off += psize) {
            uint16_t flags = 0;
            std::memcpy(&flags, data.data() + off + MP_FLAGS_OFFSET, 2);
            if (flags & P_LEAF)
                return off;
        }
        return -1;
    }
};

// ---- CVE-2019-16224 -------------------------------------------------------
//
// Heap buffer overflow via crafted md_flags.
//
// When FREE_DBI md_flags has MDB_DUPFIXED (0x10) set without MDB_DUPSORT,
// mdb_cursor_put sets P_LEAF2 on new freelist pages. mdb_node_add's IS_LEAF2
// path uses md_pad (4096) as memcpy size → heap buffer overflow in commit.
//
// Repro: write 50 entries, corrupt FREE_DBI flags to MDB_INTEGERKEY|MDB_DUPFIXED, reopen, delete all 50,
// commit → crash without patch.
// Patch: mdb_txn_renew0 detects BAD_DB_FLAGS and returns MDB_INVALID, so
// mdb_txn_begin fails immediately before any deletes can happen.

TEST(LmdbCveTest, CVE_2019_16224_FreeDbiDupfixedWithoutDupsortIsInvalid) {
    TempLmdbEnv e{"16224_free"};
    ASSERT_TRUE(e.populate(50, 200, 4ULL << 20));

    // Corrupt FREE_DBI md_flags: normal value is MDB_INTEGERKEY alone; adding
    // MDB_DUPFIXED without MDB_DUPSORT is the invalid combination.
    for (uint32_t meta = 0; meta < 2; ++meta)
        patch<uint16_t>(
                e.db_path,
                static_cast<long>(meta * e.psize) + FREE_FLAGS_OFFSET,
                uint16_t{MDB_INTEGERKEY | MDB_DUPFIXED}
        );

    // mdb_env_open does not inspect md_flags, so it succeeds.
    // The BAD_DB_FLAGS check is in mdb_txn_renew0, called by mdb_txn_begin.
    MDB_env* env = nullptr;
    mdb_env_create(&env);
    mdb_env_set_mapsize(env, 4ULL << 20);
    ASSERT_EQ(mdb_env_open(env, e.dir.string().c_str(), 0, 0644), MDB_SUCCESS)
            << "mdb_env_open must succeed — flag check fires in mdb_txn_renew0 later";

    MDB_txn* txn = nullptr;
    const int rc = mdb_txn_begin(env, nullptr, 0, &txn);
    if (txn)
        mdb_txn_abort(txn);
    mdb_env_close(env);

    EXPECT_EQ(rc, MDB_INVALID) << "CVE-2019-16224: mdb_txn_begin must return MDB_INVALID for FREE_DBI "
                                  "with MDB_DUPFIXED set without MDB_DUPSORT";
}

TEST(LmdbCveTest, CVE_2019_16224_MainDbiIntegerdupWithoutDupsortIsInvalid) {
    TempLmdbEnv e{"16224_main"};
    ASSERT_TRUE(e.populate(50, 200, 4ULL << 20));

    // Corrupt MAIN_DBI md_flags: MDB_INTEGERDUP without MDB_DUPSORT
    for (uint32_t meta = 0; meta < 2; ++meta)
        patch<uint16_t>(e.db_path, static_cast<long>(meta * e.psize) + MAIN_FLAGS_OFFSET, uint16_t{MDB_INTEGERDUP});

    MDB_env* env = nullptr;
    mdb_env_create(&env);
    mdb_env_set_mapsize(env, 4ULL << 20);
    ASSERT_EQ(mdb_env_open(env, e.dir.string().c_str(), 0, 0644), MDB_SUCCESS);

    MDB_txn* txn = nullptr;
    const int rc = mdb_txn_begin(env, nullptr, 0, &txn);
    if (txn)
        mdb_txn_abort(txn);
    mdb_env_close(env);

    EXPECT_EQ(rc, MDB_INVALID) << "CVE-2019-16224: mdb_txn_begin must return MDB_INVALID for MAIN_DBI "
                                  "with MDB_INTEGERDUP set without MDB_DUPSORT";
}

// Sanity check: MDB_DUPSORT | MDB_DUPFIXED together is valid and must not be
// rejected by the bad-flags guard.
TEST(LmdbCveTest, CVE_2019_16224_ValidDupsortDupfixedCombinationIsAccepted) {
    TempLmdbEnv e{"16224_valid"};

    MDB_env* env = nullptr;
    MDB_txn* txn = nullptr;
    MDB_dbi dbi;

    ASSERT_EQ(mdb_env_create(&env), MDB_SUCCESS);
    mdb_env_set_maxdbs(env, 4);
    mdb_env_set_mapsize(env, 1ULL << 20);
    ASSERT_EQ(mdb_env_open(env, e.dir.string().c_str(), 0, 0644), MDB_SUCCESS);
    ASSERT_EQ(mdb_txn_begin(env, nullptr, 0, &txn), MDB_SUCCESS);
    ASSERT_EQ(mdb_dbi_open(txn, "dup", MDB_CREATE | MDB_DUPSORT | MDB_DUPFIXED, &dbi), MDB_SUCCESS);

    const char k[] = "key";
    char v1[] = "val1";
    char v2[] = "val2";
    MDB_val key{3, const_cast<char*>(k)};
    MDB_val val1{4, v1};
    MDB_val val2{4, v2};

    EXPECT_EQ(mdb_put(txn, dbi, &key, &val1, 0), MDB_SUCCESS);
    EXPECT_EQ(mdb_put(txn, dbi, &key, &val2, 0), MDB_SUCCESS);
    mdb_txn_commit(txn);
    mdb_env_close(env);
}

// ---- CVE-2019-16228 -------------------------------------------------------
//
// NVD: "divide-by-zero error in mdb_env_open2 if mdb_env_read_header
// obtains a zero value for a certain size field."
//
// mdb_env_open2 computes me_maxpg = me_mapsize / me_psize.  A zero mm_psize
// causes an integer divide-by-zero.  A non-power-of-two or oversized psize
// causes misaligned page accesses (all page addresses are computed as
// me_map + me_psize * pgno; alignment relies on me_psize being a power of two).
// The patch validates all three cases in mdb_env_open2 and returns MDB_INVALID.

TEST(LmdbCveTest, CVE_2019_16228_ZeroPageSizeIsInvalid) {
    TempLmdbEnv e{"16228_zero"};
    ASSERT_TRUE(e.populate(1));

    // Zero mm_psize in both meta pages (mirrors repro zero_psize())
    patch<uint32_t>(e.db_path, PSIZE_OFFSET, 0);
    patch<uint32_t>(e.db_path, static_cast<long>(e.psize) + PSIZE_OFFSET, 0);

    MDB_env* env = nullptr;
    mdb_env_create(&env);
    const int rc = mdb_env_open(env, e.dir.string().c_str(), 0, 0644);
    mdb_env_close(env);

    EXPECT_EQ(rc, MDB_INVALID) << "CVE-2019-16228: expected MDB_INVALID for mm_psize=0";
}

TEST(LmdbCveTest, CVE_2019_16228_NonPowerOfTwoPageSizeIsInvalid) {
    TempLmdbEnv e{"16228_npo2"};
    ASSERT_TRUE(e.populate(1));

    patch<uint32_t>(e.db_path, PSIZE_OFFSET, 4000);
    patch<uint32_t>(e.db_path, static_cast<long>(e.psize) + PSIZE_OFFSET, 4000);

    MDB_env* env = nullptr;
    mdb_env_create(&env);
    const int rc = mdb_env_open(env, e.dir.string().c_str(), 0, 0644);
    mdb_env_close(env);

    EXPECT_EQ(rc, MDB_INVALID) << "CVE-2019-16228: expected MDB_INVALID for mm_psize=4000 (not a power of two)";
}

TEST(LmdbCveTest, CVE_2019_16228_OversizedPageSizeIsInvalid) {
    TempLmdbEnv e{"16228_big"};
    ASSERT_TRUE(e.populate(1));

    // 0x20000 (131072) exceeds MAX_PAGESIZE on all supported platforms
    patch<uint32_t>(e.db_path, PSIZE_OFFSET, 0x20000);
    patch<uint32_t>(e.db_path, static_cast<long>(e.psize) + PSIZE_OFFSET, 0x20000);

    MDB_env* env = nullptr;
    mdb_env_create(&env);
    const int rc = mdb_env_open(env, e.dir.string().c_str(), 0, 0644);
    mdb_env_close(env);

    EXPECT_EQ(rc, MDB_INVALID) << "CVE-2019-16228: expected MDB_INVALID for mm_psize=0x20000 (too large)";
}

// ---- CVE-2019-16225 -------------------------------------------------------
//
// NVD: "certain values of mp_flags, mdb_page_touch does not properly set up
// mc->mc_pg[mc->top], leading to an invalid write operation."
//
// mdb_page_touch() sees P_DIRTY already set on the mmap'd page and skips the
// copy-on-write allocation (it assumes the page was already dirtied by the
// current transaction). The cursor then points at the underlying read-only
// mmap region; subsequent writes crash with SIGSEGV.
//
// Our patch adds an early guard in mdb_page_get (called before mdb_page_touch)
// that rejects any on-disk page with P_DIRTY set in non-WRITEMAP mode.
//
// Repro: write "1"/"aaa", "2"/"bbb", "3"/"ccc"; corrupt P_DIRTY on leaf
// pages; reopen; del("1"), put("3","ddd") → crash without patch.
// Patch: mdb_page_get returns MDB_CORRUPTED before mdb_page_touch is reached.

TEST(LmdbCveTest, CVE_2019_16225_DirtyFlagOnLeafPageIsCorrupted) {
    TempLmdbEnv e{"16225"};
    ASSERT_TRUE(e.populate(3));

    // Set P_DIRTY (0x10) on every leaf page, mirroring corrupt_leaf_pages() in repro
    auto data = read_file(e.db_path);
    bool patched = false;
    for (long off = 2 * static_cast<long>(e.psize); off + static_cast<long>(e.psize) <= static_cast<long>(data.size());
         off += e.psize) {
        uint16_t flags = 0;
        std::memcpy(&flags, data.data() + off + MP_FLAGS_OFFSET, 2);
        if ((flags & P_LEAF) && !(flags & P_DIRTY)) {
            flags |= P_DIRTY;
            std::memcpy(data.data() + off + MP_FLAGS_OFFSET, &flags, 2);
            patched = true;
        }
    }
    ASSERT_TRUE(patched) << "No leaf page found to corrupt";
    write_file(e.db_path, data);

    MDB_env* env = nullptr;
    MDB_txn* txn = nullptr;
    MDB_dbi dbi;

    ASSERT_EQ(mdb_env_create(&env), MDB_SUCCESS);
    mdb_env_set_mapsize(env, 1ULL << 20);
    ASSERT_EQ(mdb_env_open(env, e.dir.string().c_str(), 0, 0644), MDB_SUCCESS);
    ASSERT_EQ(mdb_txn_begin(env, nullptr, 0, &txn), MDB_SUCCESS);
    ASSERT_EQ(mdb_dbi_open(txn, nullptr, 0, &dbi), MDB_SUCCESS);

    // del("key0") then put("key2","xxx") — mirrors the repro scenario.
    // The write traversal hits mdb_page_get which detects P_DIRTY.
    std::string k1_str = "key0";
    MDB_val k1{k1_str.size(), k1_str.data()};
    int del_rc = mdb_del(txn, dbi, &k1, nullptr);

    std::string k3_str = "key2";
    std::string v_str = "xxx";
    MDB_val k3{k3_str.size(), k3_str.data()};
    MDB_val v{v_str.size(), v_str.data()};
    int put_rc = mdb_put(txn, dbi, &k3, &v, 0);

    mdb_txn_abort(txn);
    mdb_env_close(env);

    // At least one operation must have been rejected with MDB_CORRUPTED
    EXPECT_TRUE(del_rc == MDB_CORRUPTED || put_rc == MDB_CORRUPTED)
            << "CVE-2019-16225: expected MDB_CORRUPTED from del or put when P_DIRTY "
               "is set on a disk page (del_rc="
            << del_rc << ", put_rc=" << put_rc << ")";
}

// ---- CVE-2019-16226 -------------------------------------------------------
//
// Out-of-bounds memmove in mdb_node_del via corrupt mn_hi.
//
// NODEDSZ() = mn_lo | (mn_hi << 16). Setting mn_hi = 0x0100 yields a value
// far exceeding me_psize, causing memmove to write out of bounds.
//
// Repro: write "1"/"aaa", "2"/"bbb", "3"/"ccc"; corrupt mn_hi of first node
// to 0x0100; reopen; del("1"), put("3","ddd"), commit → crash without patch.
// Patch: mdb_node_del validates sz against me_psize, sets MDB_TXN_ERROR, and
// returns early. mdb_txn_commit then returns MDB_BAD_TXN.

TEST(LmdbCveTest, CVE_2019_16226_CorruptNodeSizePreventsHeapOverflow) {
    TempLmdbEnv e{"16226"};
    ASSERT_TRUE(e.populate(3));

    const long leaf_off = e.first_leaf_page_offset();
    ASSERT_GE(leaf_off, 0) << "No leaf page found";

    auto data = read_file(e.db_path);

    // mp_ptrs[0]: offset within the page of the first (lowest-key) node
    uint16_t ptr0 = 0;
    std::memcpy(&ptr0, data.data() + leaf_off + MP_PTRS0_OFFSET, 2);
    ASSERT_GT(ptr0, 0u) << "ptr0 is zero";
    ASSERT_LT(ptr0, e.psize) << "ptr0 out of range";

    // mn_hi is at bytes 2-3 of MDB_node; only corrupt when currently 0
    // (small values have mn_hi == 0 since their size fits in mn_lo alone)
    const long mn_hi_off = leaf_off + ptr0 + 2;
    uint16_t mn_hi = 0;
    std::memcpy(&mn_hi, data.data() + mn_hi_off, 2);
    ASSERT_EQ(mn_hi, 0u) << "mn_hi is not zero — node has unexpectedly large data";

    // Set mn_hi = 0x0100: NODEDSZ() = mn_lo | (0x0100 << 16) >> me_psize
    const uint16_t corrupt_hi = 0x0100;
    std::memcpy(data.data() + mn_hi_off, &corrupt_hi, 2);
    write_file(e.db_path, data);

    MDB_env* env = nullptr;
    MDB_txn* txn = nullptr;
    MDB_dbi dbi;

    ASSERT_EQ(mdb_env_create(&env), MDB_SUCCESS);
    mdb_env_set_mapsize(env, 1ULL << 20);
    ASSERT_EQ(mdb_env_open(env, e.dir.string().c_str(), 0, 0644), MDB_SUCCESS);
    ASSERT_EQ(mdb_txn_begin(env, nullptr, 0, &txn), MDB_SUCCESS);
    ASSERT_EQ(mdb_dbi_open(txn, nullptr, 0, &dbi), MDB_SUCCESS);

    // Mirrors the repro scenario: del("key0"), put("key2","xxx"), commit.
    // mdb_node_del detects sz > me_psize, sets MDB_TXN_ERROR on the txn.
    // mdb_txn_commit then inspects mt_flags and returns MDB_BAD_TXN.
    std::string k1_str = "key0";
    MDB_val k1{k1_str.size(), k1_str.data()};
    mdb_del(txn, dbi, &k1, nullptr);

    std::string k3_str = "key2";
    std::string v_str = "xxx";
    MDB_val k3{k3_str.size(), k3_str.data()};
    MDB_val v{v_str.size(), v_str.data()};
    mdb_put(txn, dbi, &k3, &v, 0);

    const int commit_rc = mdb_txn_commit(txn);
    mdb_env_close(env);

    EXPECT_NE(commit_rc, MDB_SUCCESS) << "CVE-2019-16226: mdb_txn_commit must fail after mdb_node_del detected "
                                         "a corrupt node size (MDB_TXN_ERROR should have been set)";
}

// ---- CVE-2019-16227 -------------------------------------------------------
//
// NVD: "certain values of mn_flags, mdb_cursor_set triggers a memcpy with an
// invalid write operation within mdb_xcursor_init1."
//
// When mn_flags has F_DUPDATA set on a node in a non-DUPSORT DB,
// mdb_xcursor_init1 is called with mc->mc_xcursor == NULL (xcursors are only
// allocated for DUPSORT databases). The first thing mdb_xcursor_init1 does is
// dereference mc_xcursor to write into mx->mx_db via memcpy — writing to a
// NULL-derived address (invalid write, typically SIGSEGV).
//
// The NVD names mdb_cursor_set as the trigger; mdb_cursor_first (and other
// cursor operations that fetch both key and data) also trigger it when the
// found node has F_DUPDATA.
//
// Repro: write "1"/"aaa", "2"/"bbb", "3"/"ccc"; set F_DUPDATA on first node's
// mn_flags; reopen; del("1"), put("3","ddd"), abort → crash without patch.
// Patch: NULL guard added before every mdb_xcursor_init1 call site that follows
// an F_DUPDATA check; returns MDB_CORRUPTED instead of dereferencing NULL.
//
// The most direct trigger is mdb_cursor_get(MDB_FIRST) with data != NULL:
// mdb_cursor_first checks F_DUPDATA, finds mc_xcursor==NULL, and returns
// MDB_CORRUPTED before calling mdb_xcursor_init1.

TEST(LmdbCveTest, CVE_2019_16227_DupDataFlagOnNonDupsortNodeIsCorrupted) {
    TempLmdbEnv e{"16227"};
    ASSERT_TRUE(e.populate(3));

    const long leaf_off = e.first_leaf_page_offset();
    ASSERT_GE(leaf_off, 0) << "No leaf page found";

    auto data = read_file(e.db_path);
    uint16_t ptr0 = 0;
    std::memcpy(&ptr0, data.data() + leaf_off + MP_PTRS0_OFFSET, 2);
    ASSERT_GT(ptr0, 0u);
    ASSERT_LT(ptr0, e.psize);

    // mn_flags is at bytes 4-5 of MDB_node; set F_DUPDATA (0x04)
    const long mn_flags_off = leaf_off + ptr0 + 4;
    uint16_t mn_flags = 0;
    std::memcpy(&mn_flags, data.data() + mn_flags_off, 2);
    mn_flags |= F_DUPDATA_FLAG;
    std::memcpy(data.data() + mn_flags_off, &mn_flags, 2);
    write_file(e.db_path, data);

    MDB_env* env = nullptr;
    MDB_txn* txn = nullptr;
    MDB_dbi dbi;
    MDB_cursor* cursor = nullptr;

    ASSERT_EQ(mdb_env_create(&env), MDB_SUCCESS);
    mdb_env_set_mapsize(env, 1ULL << 20);
    ASSERT_EQ(mdb_env_open(env, e.dir.string().c_str(), 0, 0644), MDB_SUCCESS);
    ASSERT_EQ(mdb_txn_begin(env, nullptr, 0, &txn), MDB_SUCCESS);
    ASSERT_EQ(mdb_dbi_open(txn, nullptr, 0, &dbi), MDB_SUCCESS);
    ASSERT_EQ(mdb_cursor_open(txn, dbi, &cursor), MDB_SUCCESS);

    // mdb_cursor_get(MDB_FIRST) with data != NULL is the direct trigger:
    // mdb_cursor_first checks F_DUPDATA on the first node, finds mc_xcursor==NULL,
    // and returns MDB_CORRUPTED instead of the invalid write via mdb_xcursor_init1.
    MDB_val k{}, v{};
    const int cursor_rc = mdb_cursor_get(cursor, &k, &v, MDB_FIRST);
    mdb_cursor_close(cursor);

    // Also mirror the repro: del("key0") then put("key2","xxx") then abort.
    std::string k1_str = "key0";
    MDB_val k1{k1_str.size(), k1_str.data()};
    const int del_rc = mdb_del(txn, dbi, &k1, nullptr);

    std::string k3_str = "key2";
    std::string v3_str = "xxx";
    MDB_val k3{k3_str.size(), k3_str.data()};
    MDB_val v3{v3_str.size(), v3_str.data()};
    const int put_rc = mdb_put(txn, dbi, &k3, &v3, 0);

    mdb_txn_abort(txn);
    mdb_env_close(env);

    // The cursor_get(MDB_FIRST) must return MDB_CORRUPTED — this is the direct
    // test of the mdb_cursor_first guard added by the patch.
    EXPECT_EQ(cursor_rc, MDB_CORRUPTED) << "CVE-2019-16227: mdb_cursor_get(MDB_FIRST) must return MDB_CORRUPTED "
                                           "when F_DUPDATA is set on a node in a non-DUPSORT database "
                                           "(del_rc="
                                        << del_rc << ", put_rc=" << put_rc << ")";
}

} // namespace
