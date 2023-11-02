// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <arrow/c/bridge.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/key_value_metadata.h>

#include "nanoarrow/nanoarrow.h"

using namespace arrow;

// Helper to avoid the verbosity of ArrowSchemaToStdString
std::string ArrowSchemaToStdString(struct ArrowSchema* schema, bool recursive = true) {
  char result[1024];
  int64_t n = ArrowSchemaToString(schema, result, sizeof(result), recursive);
  std::string out(result, n);
  return out;
}

// Explicitly copy bytes to create the literal {'\1', '\0', '\0', '\0', '\3', '\0',
// '\0', '\0', 'k', 'e', 'y',  '\5', '\0', '\0', '\0', 'v',  'a',  'l',  'u', 'e'} so
// that it also works on big endian
std::string SimpleMetadata() {
  char simple_metadata[20];
  int32_t one = 1;
  memcpy(simple_metadata, &one, sizeof(int32_t));
  int32_t three = 3;
  memcpy(simple_metadata + 4, &three, sizeof(int32_t));
  memcpy(simple_metadata + 8, "key", 3);
  int32_t five = 5;
  memcpy(simple_metadata + 11, &five, sizeof(int32_t));
  memcpy(simple_metadata + 15, "value", 5);

  return std::string(simple_metadata, sizeof(simple_metadata));
}

TEST(SchemaTest, SchemaInit) {
  struct ArrowSchema schema;
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_UNINITIALIZED), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&schema, 2), NANOARROW_OK);

  ASSERT_NE(schema.release, nullptr);
  EXPECT_EQ(schema.format, nullptr);
  EXPECT_EQ(schema.name, nullptr);
  EXPECT_EQ(schema.metadata, nullptr);
  EXPECT_EQ(schema.n_children, 2);
  EXPECT_EQ(schema.children[0]->release, nullptr);
  EXPECT_EQ(schema.children[1]->release, nullptr);

  schema.release(&schema);
  EXPECT_EQ(schema.release, nullptr);

  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_UNINITIALIZED), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaAllocateChildren(
                &schema, std::numeric_limits<int64_t>::max() / sizeof(void*)),
            ENOMEM);
  schema.release(&schema);
}

static void ExpectSchemaInitOk(enum ArrowType type,
                               std::shared_ptr<DataType> expected_arrow_type) {
  struct ArrowSchema schema;
  EXPECT_EQ(ArrowSchemaInitFromType(&schema, type), NANOARROW_OK);
  auto arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(expected_arrow_type));
}

TEST(SchemaTest, SchemaInitSimple) {
  ExpectSchemaInitOk(NANOARROW_TYPE_NA, null());
  ExpectSchemaInitOk(NANOARROW_TYPE_BOOL, boolean());
  ExpectSchemaInitOk(NANOARROW_TYPE_UINT8, uint8());
  ExpectSchemaInitOk(NANOARROW_TYPE_INT8, int8());
  ExpectSchemaInitOk(NANOARROW_TYPE_UINT16, uint16());
  ExpectSchemaInitOk(NANOARROW_TYPE_INT16, int16());
  ExpectSchemaInitOk(NANOARROW_TYPE_UINT32, uint32());
  ExpectSchemaInitOk(NANOARROW_TYPE_INT32, int32());
  ExpectSchemaInitOk(NANOARROW_TYPE_UINT64, uint64());
  ExpectSchemaInitOk(NANOARROW_TYPE_INT64, int64());
  ExpectSchemaInitOk(NANOARROW_TYPE_HALF_FLOAT, float16());
  ExpectSchemaInitOk(NANOARROW_TYPE_FLOAT, float32());
  ExpectSchemaInitOk(NANOARROW_TYPE_DOUBLE, float64());
  ExpectSchemaInitOk(NANOARROW_TYPE_STRING, utf8());
  ExpectSchemaInitOk(NANOARROW_TYPE_LARGE_STRING, large_utf8());
  ExpectSchemaInitOk(NANOARROW_TYPE_BINARY, binary());
  ExpectSchemaInitOk(NANOARROW_TYPE_LARGE_BINARY, large_binary());
  ExpectSchemaInitOk(NANOARROW_TYPE_DATE32, date32());
  ExpectSchemaInitOk(NANOARROW_TYPE_DATE64, date64());
  ExpectSchemaInitOk(NANOARROW_TYPE_INTERVAL_MONTHS, month_interval());
  ExpectSchemaInitOk(NANOARROW_TYPE_INTERVAL_DAY_TIME, day_time_interval());
  ExpectSchemaInitOk(NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO, month_day_nano_interval());
}

TEST(SchemaTest, SchemaInitSimpleError) {
  struct ArrowSchema schema;
  EXPECT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_DECIMAL128), EINVAL);
  EXPECT_EQ(schema.release, nullptr);
}

TEST(SchemaTest, SchemaTestInitNestedList) {
  struct ArrowSchema schema;

  EXPECT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_LIST), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+l");
  ASSERT_EQ(ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);

  auto arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(list(int32())));

  EXPECT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_LARGE_LIST), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+L");
  ASSERT_EQ(ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(large_list(int32())));
}

TEST(SchemaTest, SchemaTestInitNestedStruct) {
  struct ArrowSchema schema;

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeStruct(&schema, 1), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+s");
  ASSERT_EQ(ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema.children[0], "item"), NANOARROW_OK);

  auto arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(struct_({field("item", int32())})));
}

TEST(SchemaTest, SchemaTestInitNestedMap) {
  struct ArrowSchema schema;

  EXPECT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_MAP), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+m");
  EXPECT_EQ(ArrowSchemaSetType(schema.children[0]->children[0], NANOARROW_TYPE_INT32),
            NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetType(schema.children[0]->children[1], NANOARROW_TYPE_STRING),
            NANOARROW_OK);
  EXPECT_STREQ(schema.children[0]->name, "entries");
  EXPECT_STREQ(schema.children[0]->children[0]->name, "key");
  EXPECT_STREQ(schema.children[0]->children[1]->name, "value");

  EXPECT_FALSE(schema.children[0]->flags & ARROW_FLAG_NULLABLE);
  EXPECT_FALSE(schema.children[0]->children[0]->flags & ARROW_FLAG_NULLABLE);

  auto arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(map(int32(), utf8())));
}

TEST(SchemaTest, SchemaInitFixedSize) {
  struct ArrowSchema schema;
  ArrowSchemaInit(&schema);

  EXPECT_EQ(ArrowSchemaSetTypeFixedSize(&schema, NANOARROW_TYPE_DOUBLE, 1), EINVAL);
  EXPECT_EQ(ArrowSchemaSetTypeFixedSize(&schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, 0),
            EINVAL);

  EXPECT_EQ(ArrowSchemaSetTypeFixedSize(&schema, NANOARROW_TYPE_FIXED_SIZE_BINARY, 45),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "w:45");

  auto arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(fixed_size_binary(45)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeFixedSize(&schema, NANOARROW_TYPE_FIXED_SIZE_LIST, 12),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+w:12");
  ASSERT_EQ(ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(fixed_size_list(int32(), 12)));
}

TEST(SchemaTest, SchemaInitDecimal) {
  struct ArrowSchema schema;

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDecimal(&schema, NANOARROW_TYPE_DECIMAL128, -1, 1), EINVAL);
  EXPECT_EQ(ArrowSchemaSetTypeDecimal(&schema, NANOARROW_TYPE_DOUBLE, 1, 2), EINVAL);

  EXPECT_EQ(ArrowSchemaSetTypeDecimal(&schema, NANOARROW_TYPE_DECIMAL128, 1, 2),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "d:1,2");

  auto arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(decimal128(1, 2)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDecimal(&schema, NANOARROW_TYPE_DECIMAL256, 3, 4),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "d:3,4,256");
  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(decimal256(3, 4)));
}

TEST(SchemaTest, SchemaInitDateTime) {
  struct ArrowSchema schema;

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_DOUBLE,
                                       NANOARROW_TIME_UNIT_SECOND, nullptr),
            EINVAL);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_TIME32,
                                       NANOARROW_TIME_UNIT_SECOND, "non-null timezone"),
            EINVAL);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_DURATION,
                                       NANOARROW_TIME_UNIT_SECOND, "non-null timezone"),
            EINVAL);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(
                &schema, NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_SECOND,
                "a really really really really really really really really really really "
                "long timezone that causes a buffer overflow on snprintf"),
            ERANGE);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_TIME32,
                                       NANOARROW_TIME_UNIT_SECOND, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tts");

  auto arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(time32(TimeUnit::SECOND)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_TIME64,
                                       NANOARROW_TIME_UNIT_NANO, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "ttn");

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(time64(TimeUnit::NANO)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_DURATION,
                                       NANOARROW_TIME_UNIT_SECOND, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tDs");

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(duration(TimeUnit::SECOND)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_SECOND, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tss:");

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(timestamp(TimeUnit::SECOND)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MILLI, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tsm:");

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(timestamp(TimeUnit::MILLI)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tsu:");

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(timestamp(TimeUnit::MICRO)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_NANO, NULL),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tsn:");

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(timestamp(TimeUnit::NANO)));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeDateTime(&schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_SECOND, "America/Halifax"),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "tss:America/Halifax");

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(
      arrow_type.ValueUnsafe()->Equals(timestamp(TimeUnit::SECOND, "America/Halifax")));
}

TEST(SchemaTest, SchemaInitUnion) {
  struct ArrowSchema schema;

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeUnion(&schema, NANOARROW_TYPE_NA, 1), EINVAL);
  EXPECT_EQ(ArrowSchemaSetTypeUnion(&schema, NANOARROW_TYPE_SPARSE_UNION, -1), EINVAL);
  EXPECT_EQ(ArrowSchemaSetTypeUnion(&schema, NANOARROW_TYPE_SPARSE_UNION, 128), EINVAL);
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeUnion(&schema, NANOARROW_TYPE_SPARSE_UNION, 0),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+us:");
  EXPECT_EQ(schema.n_children, 0);
  // The zero-case union isn't supported by Arrow C++'s C data interface implementation
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeUnion(&schema, NANOARROW_TYPE_SPARSE_UNION, 1),
            NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetName(schema.children[0], "u1"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+us:0");
  EXPECT_EQ(schema.n_children, 1);

  auto arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(sparse_union({field("u1", int32())})));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeUnion(&schema, NANOARROW_TYPE_SPARSE_UNION, 2),
            NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetName(schema.children[0], "u1"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetName(schema.children[1], "u2"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+us:0,1");
  EXPECT_EQ(schema.n_children, 2);

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(
      sparse_union({field("u1", int32()), field("u2", utf8())})));

  ArrowSchemaInit(&schema);
  EXPECT_EQ(ArrowSchemaSetTypeUnion(&schema, NANOARROW_TYPE_DENSE_UNION, 2),
            NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetName(schema.children[0], "u1"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT32), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetName(schema.children[1], "u2"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetType(schema.children[1], NANOARROW_TYPE_STRING), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+ud:0,1");
  EXPECT_EQ(schema.n_children, 2);

  arrow_type = ImportType(&schema);
  ARROW_EXPECT_OK(arrow_type);
  EXPECT_TRUE(arrow_type.ValueUnsafe()->Equals(
      dense_union({field("u1", int32()), field("u2", utf8())})));
}

TEST(SchemaTest, SchemaSetFormat) {
  struct ArrowSchema schema;
  ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_UNINITIALIZED);

  EXPECT_EQ(ArrowSchemaSetFormat(&schema, "i"), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "i");

  EXPECT_EQ(ArrowSchemaSetFormat(&schema, nullptr), NANOARROW_OK);
  EXPECT_EQ(schema.format, nullptr);

  schema.release(&schema);
}

TEST(SchemaTest, SchemaSetName) {
  struct ArrowSchema schema;
  ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_UNINITIALIZED);

  EXPECT_EQ(ArrowSchemaSetName(&schema, "a_name"), NANOARROW_OK);
  EXPECT_STREQ(schema.name, "a_name");

  EXPECT_EQ(ArrowSchemaSetName(&schema, nullptr), NANOARROW_OK);
  EXPECT_EQ(schema.name, nullptr);

  schema.release(&schema);
}

TEST(SchemaTest, SchemaSetMetadata) {
  struct ArrowSchema schema;
  ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_UNINITIALIZED);

  // Encoded metadata string for "key": "value"
  std::string simple_metadata = SimpleMetadata();

  EXPECT_EQ(ArrowSchemaSetMetadata(&schema, simple_metadata.data()), NANOARROW_OK);
  EXPECT_EQ(memcmp(schema.metadata, simple_metadata.data(), simple_metadata.size()), 0);

  EXPECT_EQ(ArrowSchemaSetMetadata(&schema, nullptr), NANOARROW_OK);
  EXPECT_EQ(schema.metadata, nullptr);

  schema.release(&schema);
}

TEST(SchemaTest, SchemaAllocateDictionary) {
  struct ArrowSchema schema;
  ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_UNINITIALIZED);

  EXPECT_EQ(ArrowSchemaAllocateDictionary(&schema), NANOARROW_OK);
  EXPECT_EQ(schema.dictionary->release, nullptr);
  EXPECT_EQ(ArrowSchemaAllocateDictionary(&schema), EEXIST);
  schema.release(&schema);
}

TEST(SchemaTest, SchemaCopySimpleType) {
  struct ArrowSchema schema;
  ARROW_EXPECT_OK(ExportType(*int32(), &schema));

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_NE(schema_copy.release, nullptr);
  EXPECT_STREQ(schema.format, "i");

  schema.release(&schema);
  schema_copy.release(&schema_copy);
}

TEST(SchemaTest, SchemaCopyNestedType) {
  struct ArrowSchema schema;
  auto struct_type = struct_({field("col1", int32())});
  ARROW_EXPECT_OK(ExportType(*struct_type, &schema));

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_NE(schema_copy.release, nullptr);
  EXPECT_STREQ(schema_copy.format, "+s");
  EXPECT_EQ(schema_copy.n_children, 1);
  EXPECT_STREQ(schema_copy.children[0]->format, "i");
  EXPECT_STREQ(schema_copy.children[0]->name, "col1");

  schema.release(&schema);
  schema_copy.release(&schema_copy);
}

TEST(SchemaTest, SchemaCopyDictType) {
  struct ArrowSchema schema;
  auto struct_type = dictionary(int32(), int64());
  ARROW_EXPECT_OK(ExportType(*struct_type, &schema));

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_STREQ(schema_copy.format, "i");
  ASSERT_NE(schema_copy.dictionary, nullptr);
  EXPECT_STREQ(schema_copy.dictionary->format, "l");

  schema.release(&schema);
  schema_copy.release(&schema_copy);
}

TEST(SchemaTest, SchemaCopyFlags) {
  struct ArrowSchema schema;
  ARROW_EXPECT_OK(ExportType(*int32(), &schema));
  ASSERT_TRUE(schema.flags & ARROW_FLAG_NULLABLE);
  schema.flags &= ~ARROW_FLAG_NULLABLE;
  ASSERT_FALSE(schema.flags & ARROW_FLAG_NULLABLE);

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_NE(schema_copy.release, nullptr);
  ASSERT_EQ(schema.flags, schema_copy.flags);
  ASSERT_FALSE(schema_copy.flags & ARROW_FLAG_NULLABLE);

  schema.release(&schema);
  schema_copy.release(&schema_copy);
}

TEST(SchemaTest, SchemaCopyMetadata) {
  struct ArrowSchema schema;
  auto arrow_meta = std::make_shared<KeyValueMetadata>();
  arrow_meta->Append("some_key", "some_value");

  auto int_field = field("field_name", int32(), arrow_meta);
  ARROW_EXPECT_OK(ExportField(*int_field, &schema));

  struct ArrowSchema schema_copy;
  ArrowSchemaDeepCopy(&schema, &schema_copy);

  ASSERT_NE(schema_copy.release, nullptr);
  EXPECT_STREQ(schema_copy.name, "field_name");
  EXPECT_NE(schema_copy.metadata, nullptr);

  auto int_field_roundtrip = ImportField(&schema_copy).ValueOrDie();
  EXPECT_EQ(int_field->name(), int_field_roundtrip->name());
  EXPECT_EQ(int_field_roundtrip->metadata()->Get("some_key").ValueOrDie(), "some_value");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, nullptr, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Expected non-NULL schema");

  schema.release = nullptr;
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Expected non-released schema");

  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_UNINITIALIZED), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(
      ArrowErrorMessage(&error),
      "Error parsing schema->format: Expected a null-terminated string but found NULL");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, ""), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected a string with size > 0");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Unknown format: '*'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "n*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format 'n*': parsed 1/2 characters");

  schema.release(&schema);
}

void ExpectSimpleTypeOk(std::shared_ptr<DataType> arrow_t, enum ArrowType nanoarrow_t,
                        int bitwidth, const char* formatted) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*arrow_t, &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, nanoarrow_t);
  EXPECT_EQ(schema_view.storage_type, nanoarrow_t);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], nanoarrow_t);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], bitwidth);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);

  EXPECT_EQ(ArrowSchemaToStdString(&schema), formatted);

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitSimple) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*null(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_NA);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_NA);
  EXPECT_EQ(schema_view.extension_name.data, nullptr);
  EXPECT_EQ(schema_view.extension_metadata.data, nullptr);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "na");
  schema.release(&schema);

  ExpectSimpleTypeOk(boolean(), NANOARROW_TYPE_BOOL, 1, "bool");
  ExpectSimpleTypeOk(int8(), NANOARROW_TYPE_INT8, 8, "int8");
  ExpectSimpleTypeOk(uint8(), NANOARROW_TYPE_UINT8, 8, "uint8");
  ExpectSimpleTypeOk(int16(), NANOARROW_TYPE_INT16, 16, "int16");
  ExpectSimpleTypeOk(uint16(), NANOARROW_TYPE_UINT16, 16, "uint16");
  ExpectSimpleTypeOk(int32(), NANOARROW_TYPE_INT32, 32, "int32");
  ExpectSimpleTypeOk(uint32(), NANOARROW_TYPE_UINT32, 32, "uint32");
  ExpectSimpleTypeOk(int64(), NANOARROW_TYPE_INT64, 64, "int64");
  ExpectSimpleTypeOk(uint64(), NANOARROW_TYPE_UINT64, 64, "uint64");
  ExpectSimpleTypeOk(float16(), NANOARROW_TYPE_HALF_FLOAT, 16, "half_float");
  ExpectSimpleTypeOk(float64(), NANOARROW_TYPE_DOUBLE, 64, "double");
  ExpectSimpleTypeOk(float32(), NANOARROW_TYPE_FLOAT, 32, "float");
}

TEST(SchemaViewTest, SchemaViewInitSimpleErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_NA), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&schema, 2), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected schema with 0 children but found 2 children");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitDecimal) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*decimal128(5, 6), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DECIMAL128);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_DECIMAL128);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_DECIMAL128);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 128);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(schema_view.decimal_bitwidth, 128);
  EXPECT_EQ(schema_view.decimal_precision, 5);
  EXPECT_EQ(schema_view.decimal_scale, 6);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "decimal128(5, 6)");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*decimal256(5, 6), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DECIMAL256);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_DECIMAL256);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_DECIMAL256);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 256);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(schema_view.decimal_bitwidth, 256);
  EXPECT_EQ(schema_view.decimal_precision, 5);
  EXPECT_EQ(schema_view.decimal_scale, 6);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "decimal256(5, 6)");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitDecimalErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_NA), NANOARROW_OK);

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "d"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':precision,scale[,bitwidth]' "
               "following 'd'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "d:"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':precision,scale[,bitwidth]' "
               "following 'd'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "d:5"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected 'precision,scale[,bitwidth]' "
               "following 'd:'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "d:5,"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected 'scale[,bitwidth]' following "
               "'d:precision,'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "d:5,6,"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(
      ArrowErrorMessage(&error),
      "Error parsing schema->format: Expected precision following 'd:precision,scale,'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "d:5,6,127"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected decimal bitwidth of 128 or 256 "
               "but found 127");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitBinaryAndString) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*fixed_size_binary(123), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_FIXED_SIZE_BINARY);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_FIXED_SIZE_BINARY);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_BINARY);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 123 * 8);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(schema_view.fixed_size, 123);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "fixed_size_binary(123)");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*utf8(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_STRING);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_STRING);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA_OFFSET);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_DATA);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_INT32);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_STRING);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 32);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "string");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*binary(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_BINARY);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_BINARY);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA_OFFSET);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_DATA);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_INT32);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_BINARY);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 32);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "binary");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*large_binary(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_LARGE_BINARY);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_LARGE_BINARY);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA_OFFSET);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_DATA);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_BINARY);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 64);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "large_binary");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*large_utf8(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_LARGE_STRING);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_LARGE_STRING);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA_OFFSET);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_DATA);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_STRING);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 64);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "large_string");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitBinaryAndStringErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_NA), NANOARROW_OK);

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "w"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':<width>' following 'w'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "w:"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':<width>' following 'w'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "w:abc"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format 'w:abc': parsed 2/5 characters");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "w:0"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected size > 0 for fixed size binary but found size 0");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeDate) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*date32(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DATE32);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT32);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "date32");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*date64(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DATE64);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "date64");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeTime) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*time32(TimeUnit::SECOND), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_TIME32);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT32);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_SECOND);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "time32('s')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*time32(TimeUnit::MILLI), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_TIME32);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT32);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_MILLI);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "time32('ms')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*time64(TimeUnit::MICRO), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_TIME64);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_MICRO);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "time64('us')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*time64(TimeUnit::NANO), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_TIME64);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_NANO);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "time64('ns')");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeTimestamp) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*timestamp(TimeUnit::SECOND, "America/Halifax"), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_TIMESTAMP);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_SECOND);
  EXPECT_STREQ(schema_view.timezone, "America/Halifax");
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "timestamp('s', 'America/Halifax')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*timestamp(TimeUnit::MILLI, "America/Halifax"), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_TIMESTAMP);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_MILLI);
  EXPECT_STREQ(schema_view.timezone, "America/Halifax");
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "timestamp('ms', 'America/Halifax')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*timestamp(TimeUnit::MICRO, "America/Halifax"), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_TIMESTAMP);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_MICRO);
  EXPECT_STREQ(schema_view.timezone, "America/Halifax");
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "timestamp('us', 'America/Halifax')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*timestamp(TimeUnit::NANO, "America/Halifax"), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_TIMESTAMP);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_NANO);
  EXPECT_STREQ(schema_view.timezone, "America/Halifax");
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "timestamp('ns', 'America/Halifax')");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeDuration) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*duration(TimeUnit::SECOND), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DURATION);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_SECOND);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "duration('s')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*duration(TimeUnit::MILLI), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DURATION);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_MILLI);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "duration('ms')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*duration(TimeUnit::MICRO), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DURATION);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_MICRO);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "duration('us')");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*duration(TimeUnit::NANO), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DURATION);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.time_unit, NANOARROW_TIME_UNIT_NANO);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "duration('ns')");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeInterval) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*month_interval(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_INTERVAL_MONTHS);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INTERVAL_MONTHS);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "interval_months");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*day_time_interval(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_INTERVAL_DAY_TIME);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INTERVAL_DAY_TIME);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "interval_day_time");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*month_day_nano_interval(), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "interval_month_day_nano");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitTimeErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_NA), NANOARROW_OK);

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "t*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected 'd', 't', 's', 'D', or 'i' "
               "following 't' but found '*'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "td*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(
      ArrowErrorMessage(&error),
      "Error parsing schema->format: Expected 'D' or 'm' following 'td' but found '*'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "tt*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected 's', 'm', 'u', or 'n' following "
               "'tt' but found '*'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "ts*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected 's', 'm', 'u', or 'n' following "
               "'ts' but found '*'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "tD*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected 's', 'm', u', or 'n' following "
               "'tD' but found '*'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "ti*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected 'M', 'D', or 'n' following 'ti' "
               "but found '*'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "tss"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':' following 'tss' but found ''");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedList) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*list(int32()), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_LIST);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_LIST);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA_OFFSET);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_INT32);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 32);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "list<item: int32>");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*large_list(int32()), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_LARGE_LIST);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_LARGE_LIST);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA_OFFSET);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_INT64);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 64);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "large_list<item: int32>");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*fixed_size_list(int32(), 123), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_FIXED_SIZE_LIST);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_FIXED_SIZE_LIST);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 0);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(schema_view.fixed_size, 123);
  EXPECT_EQ(schema_view.layout.child_size_elements, 123);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "fixed_size_list(123)<item: int32>");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewNestedListErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_NA), NANOARROW_OK);

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+w"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':<width>' following '+w'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+w:"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected ':<width>' following '+w'");

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+w:1"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected schema with 1 children but found 0 children");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedStruct) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(
      ExportType(*struct_({field("col1", int32()), field("col2", int64())}), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_STRUCT);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_STRUCT);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 0);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "struct<col1: int32, col2: int64>");

  // Make sure children validate
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, schema.children[0], &error), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, schema.children[1], &error), NANOARROW_OK);

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedStructErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&schema, 1), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(
      ArrowErrorMessage(&error),
      "Expected valid schema at schema->children[0] but found a released schema");

  // Make sure validation passes even with an inspectable but invalid child
  ASSERT_EQ(ArrowSchemaInitFromType(schema.children[0], NANOARROW_TYPE_UNINITIALIZED),
            NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, schema.children[0], &error), EINVAL);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);

  ArrowFree(schema.children[0]);
  schema.children[0] = NULL;
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected valid schema at schema->children[0] but found NULL");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedMap) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*map(int32(), int32()), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_VALIDITY);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_DATA_OFFSET);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_BOOL);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_INT32);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 1);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 32);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_EQ(ArrowSchemaToStdString(&schema),
            "map<entries: struct<key: int32, value: int32>>");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedMapErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ArrowSchemaInit(&schema);
  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+m"), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&schema, 2), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected schema with 1 children but found 2 children");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+m"), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&schema, 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(schema.children[0], NANOARROW_TYPE_UNINITIALIZED),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetFormat(schema.children[0], "n"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected child of map type to have 2 children but found 0");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+m"), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&schema, 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(schema.children[0], NANOARROW_TYPE_UNINITIALIZED),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(schema.children[0], 2), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetFormat(schema.children[0], "+us:0,1"), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(schema.children[0]->children[0], NANOARROW_TYPE_NA),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(schema.children[0]->children[1], NANOARROW_TYPE_NA),
            NANOARROW_OK);

  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected format of child of map type to be '+s' but found '+us:0,1'");
  schema.release(&schema);

  EXPECT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_MAP), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetType(schema.children[0]->children[0], NANOARROW_TYPE_INT32),
            NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaSetType(schema.children[0]->children[1], NANOARROW_TYPE_STRING),
            NANOARROW_OK);

  schema.children[0]->flags |= ARROW_FLAG_NULLABLE;
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected child of map type to be non-nullable but was nullable");
  schema.children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  schema.children[0]->children[0]->flags |= ARROW_FLAG_NULLABLE;
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Expected key of map type to be non-nullable but was nullable");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedUnion) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*dense_union({field("col", int32())}), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DENSE_UNION);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_DENSE_UNION);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_TYPE_ID);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_UNION_OFFSET);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_INT8);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_INT32);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 8);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 32);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_STREQ(schema_view.union_type_ids, "0");
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "dense_union([0])<col: int32>");
  schema.release(&schema);

  ARROW_EXPECT_OK(ExportType(*sparse_union({field("col", int32())}), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_SPARSE_UNION);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_SPARSE_UNION);
  EXPECT_EQ(schema_view.layout.buffer_type[0], NANOARROW_BUFFER_TYPE_TYPE_ID);
  EXPECT_EQ(schema_view.layout.buffer_type[1], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_type[2], NANOARROW_BUFFER_TYPE_NONE);
  EXPECT_EQ(schema_view.layout.buffer_data_type[0], NANOARROW_TYPE_INT8);
  EXPECT_EQ(schema_view.layout.buffer_data_type[1], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.buffer_data_type[2], NANOARROW_TYPE_UNINITIALIZED);
  EXPECT_EQ(schema_view.layout.element_size_bits[0], 8);
  EXPECT_EQ(schema_view.layout.element_size_bits[1], 0);
  EXPECT_EQ(schema_view.layout.element_size_bits[2], 0);
  EXPECT_STREQ(schema_view.union_type_ids, "0");
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "sparse_union([0])<col: int32>");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitNestedUnionErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_NA), NANOARROW_OK);

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+u*"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected union format string "
               "+us:<type_ids> or +ud:<type_ids> but found '+u*'");

  // missing colon
  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+us"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected union format string "
               "+us:<type_ids> or +ud:<type_ids> but found '+us'");

  // bad type_ids (wrong number of children)
  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+us:0"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected union type_ids parameter to be a "
               "comma-separated list of 0 values between 0 and 127 but found '0'");

  // bad type_ids (not comma separated integers)
  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+us:,"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected union type_ids parameter to be a "
               "comma-separated list of 0 values between 0 and 127 but found ','");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitInvalidSpecErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_NA), NANOARROW_OK);

  ASSERT_EQ(ArrowSchemaSetFormat(&schema, "+Z"), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error),
               "Error parsing schema->format: Expected nested type "
               "format string but found '+Z'");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitDictionary) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ARROW_EXPECT_OK(ExportType(*dictionary(int32(), utf8()), &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT32);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DICTIONARY);
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "dictionary(int32)<string>");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitDictionaryErrors) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_INT32), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateDictionary(&schema), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Expected non-released schema");
  schema.release(&schema);

  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateDictionary(&schema), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(schema.dictionary, NANOARROW_TYPE_STRING),
            NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), EINVAL);
  EXPECT_STREQ(
      ArrowErrorMessage(&error),
      "Expected dictionary schema index type to be an integral type but found '+s'");
  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitExtension) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  auto arrow_meta = std::make_shared<KeyValueMetadata>();
  arrow_meta->Append("ARROW:extension:name", "arrow.test.ext_name");
  arrow_meta->Append("ARROW:extension:metadata", "test metadata");

  auto int_field = field("field_name", int32(), arrow_meta);
  ARROW_EXPECT_OK(ExportField(*int_field, &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(
      std::string(schema_view.extension_name.data, schema_view.extension_name.size_bytes),
      "arrow.test.ext_name");
  EXPECT_EQ(std::string(schema_view.extension_metadata.data,
                        schema_view.extension_metadata.size_bytes),
            "test metadata");
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "arrow.test.ext_name{int32}");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaViewInitExtensionDictionary) {
  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  struct ArrowError error;

  auto arrow_meta = std::make_shared<KeyValueMetadata>();
  arrow_meta->Append("ARROW:extension:name", "arrow.test.ext_name");
  arrow_meta->Append("ARROW:extension:metadata", "test metadata");

  auto int_field = field("field_name", dictionary(int32(), utf8()), arrow_meta);
  ARROW_EXPECT_OK(ExportField(*int_field, &schema));
  EXPECT_EQ(ArrowSchemaViewInit(&schema_view, &schema, &error), NANOARROW_OK);
  EXPECT_EQ(schema_view.type, NANOARROW_TYPE_DICTIONARY);
  EXPECT_EQ(schema_view.storage_type, NANOARROW_TYPE_INT32);
  EXPECT_EQ(
      std::string(schema_view.extension_name.data, schema_view.extension_name.size_bytes),
      "arrow.test.ext_name");
  EXPECT_EQ(std::string(schema_view.extension_metadata.data,
                        schema_view.extension_metadata.size_bytes),
            "test metadata");
  EXPECT_EQ(ArrowSchemaToStdString(&schema),
            "arrow.test.ext_name{dictionary(int32)<string>}");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaFormatNotRecursive) {
  struct ArrowSchema schema;
  ARROW_EXPECT_OK(
      ExportType(*struct_({field("col1", int32()), field("col2", int64())}), &schema));
  EXPECT_EQ(ArrowSchemaToStdString(&schema, false), "struct");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaFormatEmptyNested) {
  struct ArrowSchema schema;
  ARROW_EXPECT_OK(ExportType(*struct_({}), &schema));
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "struct<>");

  schema.release(&schema);
}

TEST(SchemaViewTest, SchemaFormatInvalid) {
  EXPECT_EQ(ArrowSchemaToStdString(nullptr), "[invalid: pointer is null]");

  struct ArrowSchema schema;
  schema.release = nullptr;
  EXPECT_EQ(ArrowSchemaToStdString(&schema), "[invalid: schema is released]");

  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_UNINITIALIZED), NANOARROW_OK);
  EXPECT_EQ(ArrowSchemaToStdString(&schema),
            "[invalid: Error parsing schema->format: Expected a null-terminated string "
            "but found NULL]");

  schema.release(&schema);
}

TEST(MetadataTest, Metadata) {
  // Encoded metadata string for "key": "value"
  std::string simple_metadata = SimpleMetadata();

  EXPECT_EQ(ArrowMetadataSizeOf(nullptr), 0);
  EXPECT_EQ(ArrowMetadataSizeOf(simple_metadata.data()), simple_metadata.size());

  EXPECT_EQ(ArrowMetadataHasKey(simple_metadata.data(), ArrowCharView("key")), 1);
  EXPECT_EQ(ArrowMetadataHasKey(simple_metadata.data(), ArrowCharView("not_a_key")), 0);

  struct ArrowStringView value = ArrowCharView("default_val");
  EXPECT_EQ(ArrowMetadataGetValue(simple_metadata.data(), ArrowCharView("key"), &value),
            NANOARROW_OK);
  EXPECT_EQ(std::string(value.data, value.size_bytes), "value");

  value = ArrowCharView("default_val");
  EXPECT_EQ(
      ArrowMetadataGetValue(simple_metadata.data(), ArrowCharView("not_a_key"), &value),
      NANOARROW_OK);
  EXPECT_EQ(std::string(value.data, value.size_bytes), "default_val");
}

TEST(MetadataTest, MetadataBuild) {
  // Encoded metadata string for "key": "value"
  std::string simple_metadata = SimpleMetadata();

  // Metadata builder from copy
  struct ArrowBuffer metadata_builder;
  ASSERT_EQ(ArrowMetadataBuilderInit(&metadata_builder, simple_metadata.data()),
            NANOARROW_OK);
  EXPECT_EQ(metadata_builder.size_bytes, simple_metadata.size());
  EXPECT_EQ(
      memcmp(metadata_builder.data, simple_metadata.data(), metadata_builder.size_bytes),
      0);
  ArrowBufferReset(&metadata_builder);

  // Empty metadata
  ASSERT_EQ(ArrowMetadataBuilderInit(&metadata_builder, nullptr), NANOARROW_OK);
  EXPECT_EQ(metadata_builder.size_bytes, 0);
  EXPECT_EQ(metadata_builder.data, nullptr);

  // Recreate simple_metadata
  ASSERT_EQ(ArrowMetadataBuilderAppend(&metadata_builder, ArrowCharView("key"),
                                       ArrowCharView("value")),
            NANOARROW_OK);
  ASSERT_EQ(metadata_builder.size_bytes, simple_metadata.size());
  EXPECT_EQ(memcmp(metadata_builder.data, simple_metadata.data(), simple_metadata.size()),
            0);

  // Remove a key that doesn't exist
  ASSERT_EQ(ArrowMetadataBuilderRemove(&metadata_builder, ArrowCharView("key2")),
            NANOARROW_OK);
  ASSERT_EQ(metadata_builder.size_bytes, simple_metadata.size());
  EXPECT_EQ(
      memcmp(metadata_builder.data, simple_metadata.data(), metadata_builder.size_bytes),
      0);

  // Add a new key
  ASSERT_EQ(ArrowMetadataBuilderSet(&metadata_builder, ArrowCharView("key2"),
                                    ArrowCharView("value2")),
            NANOARROW_OK);
  EXPECT_EQ(metadata_builder.size_bytes,
            simple_metadata.size() + sizeof(int32_t) + 4 + sizeof(int32_t) + 6);

  struct ArrowStringView value = ArrowCharView(nullptr);
  ASSERT_EQ(ArrowMetadataGetValue((const char*)metadata_builder.data,
                                  ArrowCharView("key2"), &value),
            NANOARROW_OK);
  EXPECT_EQ(std::string(value.data, value.size_bytes), "value2");

  // Set an existing key
  ASSERT_EQ(ArrowMetadataBuilderSet(&metadata_builder, ArrowCharView("key"),
                                    ArrowCharView("value3")),
            NANOARROW_OK);
  value = ArrowCharView(nullptr);
  ASSERT_EQ(ArrowMetadataGetValue((const char*)metadata_builder.data,
                                  ArrowCharView("key"), &value),
            NANOARROW_OK);
  EXPECT_EQ(std::string(value.data, value.size_bytes), "value3");
  value = ArrowCharView(nullptr);
  ASSERT_EQ(ArrowMetadataGetValue((const char*)metadata_builder.data,
                                  ArrowCharView("key2"), &value),
            NANOARROW_OK);
  EXPECT_EQ(std::string(value.data, value.size_bytes), "value2");

  // Remove a key that does exist
  ASSERT_EQ(ArrowMetadataBuilderRemove(&metadata_builder, ArrowCharView("key")),
            NANOARROW_OK);
  EXPECT_EQ(ArrowMetadataHasKey((const char*)metadata_builder.data, ArrowCharView("key")),
            false);
  value = ArrowCharView(nullptr);
  ASSERT_EQ(ArrowMetadataGetValue((const char*)metadata_builder.data,
                                  ArrowCharView("key2"), &value),
            NANOARROW_OK);
  EXPECT_EQ(std::string(value.data, value.size_bytes), "value2");

  ArrowBufferReset(&metadata_builder);
}
