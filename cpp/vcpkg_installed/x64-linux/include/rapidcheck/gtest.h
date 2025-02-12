#pragma once

#include <rapidcheck.h>

#include "rapidcheck/detail/ExecFixture.h"

namespace rc {
namespace detail {

template <typename Testable>
void checkGTest(Testable &&testable) {
  const auto testInfo = ::testing::UnitTest::GetInstance()->current_test_info();
  TestMetadata metadata;
  metadata.id = std::string(testInfo->test_case_name()) + "/" +
      std::string(testInfo->name());
  metadata.description = std::string(testInfo->name());

  const auto result = checkTestable(std::forward<Testable>(testable), metadata);

  if (result.template is<SuccessResult>()) {
    const auto success = result.template get<SuccessResult>();
    if (!success.distribution.empty()) {
      printResultMessage(result, std::cout);
      std::cout << std::endl;
    }
  } else {
    std::ostringstream ss;
    printResultMessage(result, ss);
    FAIL() << ss.str() << std::endl;
  }
}

} // namespace detail
} // namespace rc

/// Defines a RapidCheck property as a Google Test.
#define RC_GTEST_PROP(TestCase, Name, ArgList)                                 \
  void rapidCheck_propImpl_##TestCase##_##Name ArgList;                        \
                                                                               \
  TEST(TestCase, Name) {                                                       \
    ::rc::detail::checkGTest(&rapidCheck_propImpl_##TestCase##_##Name);        \
  }                                                                            \
                                                                               \
  void rapidCheck_propImpl_##TestCase##_##Name ArgList

/// Defines a RapidCheck property as a Google Test fixture based test. The
/// fixture is reinstantiated for each test case of the property.
#define RC_GTEST_FIXTURE_PROP(Fixture, Name, ArgList)                          \
  class RapidCheckPropImpl_##Fixture##_##Name : public Fixture {               \
  public:                                                                      \
    void rapidCheck_fixtureSetUp() { SetUp(); }                                \
    void TestBody() override {}                                                \
    void operator() ArgList;                                                   \
    void rapidCheck_fixtureTearDown() { TearDown(); }                          \
  };                                                                           \
                                                                               \
  TEST(Fixture##_RapidCheck, Name) {                                           \
    ::rc::detail::checkGTest(&rc::detail::ExecFixture<                         \
                             RapidCheckPropImpl_##Fixture##_##Name>::exec);    \
  }                                                                            \
                                                                               \
  void RapidCheckPropImpl_##Fixture##_##Name::operator() ArgList

/// Defines a RapidCheck property as a typed Google Test.
///
/// TypedFixture is a Google Test typed (or type-parameterized) test
/// fixture. The test type can be accessed from ArgList or from the
/// property body as TypeParam (similarly to a TYPED_TEST).
///
/// The fixture is reinstantiated for each test case of the property.
#define RC_GTEST_TYPED_FIXTURE_PROP(Fixture, Name, ArgList)                    \
  template <typename TypeParam>                                                \
  class RapidCheckPropImpl_##Fixture##_##Name : public Fixture<TypeParam> {    \
  public:                                                                      \
    void rapidCheck_fixtureSetUp() { this->SetUp(); }                          \
    void TestBody() override {}                                                \
    void operator() ArgList;                                                   \
    void rapidCheck_fixtureTearDown() { this->TearDown(); }                    \
  };                                                                           \
                                                                               \
  TYPED_TEST(Fixture, Name) {                                                  \
    ::rc::detail::checkGTest(                                                  \
        &rc::detail::ExecFixture<                                              \
            RapidCheckPropImpl_##Fixture##_##Name<TypeParam>>::exec);          \
  }                                                                            \
                                                                               \
  template <typename TypeParam>                                                \
  void RapidCheckPropImpl_##Fixture##_##Name<TypeParam>::operator() ArgList
