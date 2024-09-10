#pragma once

#include <rapidcheck.h>
#include <gmock/gmock.h>

namespace rc {
namespace gmock {

/// Integrates RapidCheck with GMock by allowing GMock failures to become
/// RapidCheck property failures. This class also maintains the old
/// `TestEventListener` (or any other one, for that matter) so that it can
/// delegate to it when not in a property. This way, you can have
/// GMock/GTest/whatever tests side by side with RapidCheck properties and
/// RapidCheck only grabs the errors when it is actually active.
class RapidCheckListener : public ::testing::TestEventListener {
public:
  /// Constructs a new `RapidCheckListener`.
  ///
  /// @param delegate Will be delegated to when not in a property.
  explicit RapidCheckListener(
      std::unique_ptr<::testing::TestEventListener> &&delegate = nullptr)
      : m_delegate(std::move(delegate)) {}

  void OnTestPartResult(const ::testing::TestPartResult &result) override {
    using namespace rc::detail;
    using namespace ::testing;

    std::string msg;
    if (result.file_name()) {
      msg += result.file_name();
      msg += ':';
      msg += std::to_string(result.line_number());
      msg += ":\n";
    }
    msg += result.message();

    // No point in reporting fatal failures since we will be aborted anyway.
    if (result.type() != TestPartResult::kFatalFailure) {
      const auto context = ImplicitParam<param::CurrentPropertyContext>::value();

      const auto type =
        result.failed() ? CaseResult::Type::Failure : CaseResult::Type::Success;
      if (context->reportResult(CaseResult(type, msg))) {
        return;
      }
    }

    // Not handled, delegate if possible, otherwise print to stderr
    if (m_delegate) {
      m_delegate->OnTestPartResult(result);
    } else {
      std::cerr << msg << std::endl;
    }
  }

  /// Installs a new listener with gmock. The old default listener will be set
  /// as the delegate for the new listener so that the old can receive events
  /// when not in a property.
  static void install() {
    auto &listeners = ::testing::UnitTest::GetInstance()->listeners();
    std::unique_ptr<::testing::TestEventListener> delegate(
        listeners.Release(listeners.default_result_printer()));
    listeners.Append(new rc::gmock::RapidCheckListener(std::move(delegate)));
  }

  void OnTestProgramStart(const ::testing::UnitTest &unit_test) override {
    if (m_delegate) {
      m_delegate->OnTestProgramStart(unit_test);
    }
  }

  void OnTestIterationStart(const ::testing::UnitTest &unit_test,
                            int iteration) override {
    if (m_delegate) {
      m_delegate->OnTestIterationStart(unit_test, iteration);
    }
  }

  void OnEnvironmentsSetUpStart(const ::testing::UnitTest &unit_test) override {
    if (m_delegate) {
      m_delegate->OnEnvironmentsSetUpStart(unit_test);
    }
  }

  void OnEnvironmentsSetUpEnd(const ::testing::UnitTest &unit_test) override {
    if (m_delegate) {
      m_delegate->OnEnvironmentsSetUpEnd(unit_test);
    }
  }

  void OnTestCaseStart(const ::testing::TestCase &test_case) override {
    if (m_delegate) {
      m_delegate->OnTestCaseStart(test_case);
    }
  }

  void OnTestStart(const ::testing::TestInfo &test_info) override {
    if (m_delegate) {
      m_delegate->OnTestStart(test_info);
    }
  }

  void OnTestEnd(const ::testing::TestInfo &test_info) override {
    if (m_delegate) {
      m_delegate->OnTestEnd(test_info);
    }
  }

  void OnTestCaseEnd(const ::testing::TestCase &test_case) override {
    if (m_delegate) {
      m_delegate->OnTestCaseEnd(test_case);
    }
  }

  void
  OnEnvironmentsTearDownStart(const ::testing::UnitTest &unit_test) override {
    if (m_delegate) {
      m_delegate->OnEnvironmentsTearDownStart(unit_test);
    }
  }

  void
  OnEnvironmentsTearDownEnd(const ::testing::UnitTest &unit_test) override {
    if (m_delegate) {
      m_delegate->OnEnvironmentsTearDownEnd(unit_test);
    }
  }

  void OnTestIterationEnd(const ::testing::UnitTest &unit_test,
                          int iteration) override {
    if (m_delegate) {
      m_delegate->OnTestIterationEnd(unit_test, iteration);
    }
  }

  void OnTestProgramEnd(const ::testing::UnitTest &unit_test) override {
    if (m_delegate) {
      m_delegate->OnTestProgramEnd(unit_test);
    }
  }

private:
  std::unique_ptr<::testing::TestEventListener> m_delegate;
};

} // namespace gmock
} // namespace rc
