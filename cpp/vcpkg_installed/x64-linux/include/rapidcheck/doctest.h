#pragma once

#include <sstream>
#include <source_location>

#include <rapidcheck.h>

namespace rc::doctest {

/**
 * Checks the given predicate by applying it to randomly generated arguments.
 *
 * Quotes the given description string if the predicate can be falsified.
 *
 * Traces a progress message to 'stdout' if the flag 'v' is true.
 *
 * Like the function 'rc::check', but integrates with 'doctest' to include its
 * result in the statistics that are gathered for a test run.
 *
 * For example:
 *
 *  TEST_CASE("addition is commutative")
 *  {
 *    wol::test::check("a+b == b+a", [](int a, int b) { return a+b == b+a; });
 *  }
 *
 * @param  d  A description of the predicate being checked.
 * @param  t  A predicate to check.
 * @param  v  A flag requesting verbose output.
 *
 * @see    https://github.com/emil-e/rapidcheck/blob/master/doc/properties.md
 *         for more on 'rc::check', on which this function is modeled.
 *
 * @see    https://github.com/emil-e/rapidcheck/blob/master/doc/catch.md
 *         for more on the integration of 'rapidcheck' and 'catch', on which
 *         this implementation is based.
 */
template <class testable>
void check(const char*          d,
           testable&&           t,
           bool                 v = false,
           std::source_location s = std::source_location::current())
{
  using namespace rc::detail;
  using namespace doctest::detail;

  DOCTEST_SUBCASE(d)
  {
    auto r = checkTestable(std::forward<testable>(t));

    if (r.template is<SuccessResult>())
    {
      if (!r.template get<SuccessResult>().distribution.empty() || v)
      {
        std::cout << "- " << d << std::endl;
        printResultMessage(r, std::cout);
        std::cout << std::endl;
      }

      REQUIRE(true);
    }
    else
    {
      std::ostringstream o;
      printResultMessage(r, o << '\n');
      DOCTEST_INFO(o.str());
      ResultBuilder b(doctest::assertType::DT_CHECK, s.file_name(), s.line(), s.function_name());
      DOCTEST_ASSERT_LOG_REACT_RETURN(b);
    }
  }
}

/**
 * Checks the given predicate by applying it to randomly generated arguments.
 *
 * Quotes the given description string if the predicate can be falsified.
 *
 * Traces a progress message to 'stdout' if the flag 'v' is true.
 *
 * Like the function 'rc::check', but integrates with 'doctest' to include its
 * result in the statitics that are gathered for a test run.
 *
 * For example:
 *
 *  TEST_CASE("addition is commutative")
 *  {
 *    wol::test::check("a+b == b+a", [](int a, int b) { return a+b == b+a; });
 *  }
 *
 * @param  t  A predicate to check.
 * @param  v  A flag requesting verbose output.
 *
 * @see    https://github.com/emil-e/rapidcheck/blob/master/doc/properties.md
 *         for more on 'rc::check', on which this function is modeled.
 *
 * @see    https://github.com/emil-e/rapidcheck/blob/master/doc/catch.md
 *         for more on the integration of 'rapidcheck' and 'catch', on which
 *         this implementation is based.
 */
template <class testable>
inline
void check(testable&&           t,
           bool                 v = false,
           std::source_location s = std::source_location::current())
{
  check("", t, v ,s);
}

} // namespace rc::doctest
