#pragma once

#include <rapidcheck.h>

#include "rapidcheck/detail/ExecFixture.h"

#include <boost/test/unit_test.hpp>

namespace rc {
namespace detail {

template <typename Testable>
void checkBoostTest(Testable &&testable) {
  const auto &testCase = boost::unit_test::framework::current_test_case();
  TestMetadata metadata;
  metadata.id = testCase.full_name();
  metadata.description = testCase.p_name;

  const auto result = checkTestable(std::forward<Testable>(testable), metadata);

  // Without this boost.test will complain about the test case having no assertions when the
  // rapidcheck test passes
  BOOST_CHECK (true);

  if (result.template is<SuccessResult>()) {
    const auto success = result.template get<SuccessResult>();
    if (!success.distribution.empty()) {
      std::cout << "- " << metadata.description << std::endl;
      printResultMessage(result, std::cout);
      std::cout << std::endl;
    }
  } else {
    std::ostringstream ss;
    ss << std::endl;
    printResultMessage(result, ss);
    BOOST_FAIL(ss.str());
  }
}

} // namespace detail
} // namespace rc

/// Defines a RapidCheck property as a Boost test.
#define RC_BOOST_PROP(Name, ArgList)                                           \
  void rapidCheck_propImpl_##Name ArgList;                                     \
                                                                               \
  BOOST_AUTO_TEST_CASE(Name) {                                                 \
    ::rc::detail::checkBoostTest(&rapidCheck_propImpl_##Name);                 \
  }                                                                            \
                                                                               \
  void rapidCheck_propImpl_##Name ArgList

/// Defines a RapidCheck property as a Boost Test fixture based test. The
/// fixture is reinstantiated for each test case of the property.
#define RC_BOOST_FIXTURE_PROP(Name, Fixture, ArgList)                          \
  class RapidCheckPropImpl_##Fixture##_##Name : public Fixture {               \
  public:                                                                      \
    void rapidCheck_fixtureSetUp() {}                                          \
    void operator() ArgList;                                                   \
    void rapidCheck_fixtureTearDown() {}                                       \
  };                                                                           \
                                                                               \
  BOOST_AUTO_TEST_CASE(Name) {                                                 \
     ::rc::detail::checkBoostTest(                                             \
        &rc::detail::ExecFixture<                                              \
            RapidCheckPropImpl_##Fixture##_##Name>::exec);                     \
  }                                                                            \
                                                                               \
  void RapidCheckPropImpl_##Fixture##_##Name::operator() ArgList
