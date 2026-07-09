// windows_env_probe.cpp -- the Windows-specific crux, standalone (no ArcticDB).
//
// ArcticDB's configure_s3_checksum_validation() sets the checksum env vars with
// _putenv_s() on Windows. _putenv_s writes the CRT environment. The AWS C++ SDK
// reads env vars via Aws::Environment::GetEnv, whose Windows implementation calls
// the Win32 GetEnvironmentVariableA against the *process* environment block --
// a separate store. If _putenv_s does not propagate to it (CRT-dependent), the
// SDK never sees "when_required", stays at the 1.11.820 default "when_supported",
// and rejects checksum-less endpoints. This is Windows-only; on Linux setenv()
// mutates the single environ the SDK reads, so the mitigation always sticks.
//
// This probe reproduces just that question -- no S3, no ArcticDB build needed.
//
//   Windows:  cl /EHsc windows_env_probe.cpp && windows_env_probe.exe
//   (For the definitive check, replace GetEnvironmentVariableA below with a call
//    to Aws::Environment::GetEnv linked against the vendored aws-sdk-cpp.)
//
// PASS  => both reads return "when_required"  -> mitigation reaches the SDK.
// FAIL  => Win32 read is empty/stale          -> mitigation silently no-ops. BUG.

#include <cstdlib>
#include <cstdio>

#ifdef _WIN32
#  include <windows.h>
#endif

int main() {
    const char* name = "AWS_RESPONSE_CHECKSUM_VALIDATION";

#ifdef _WIN32
    _putenv_s(name, "when_required");                 // what ArcticDB does

    const char* crt = getenv(name);                   // CRT view
    char win32[256] = {0};
    GetEnvironmentVariableA(name, win32, sizeof(win32)); // what the AWS SDK reads

    printf("CRT   getenv()               = %s\n", crt ? crt : "(null)");
    printf("Win32 GetEnvironmentVariableA= %s\n", win32[0] ? win32 : "(empty)");

    bool ok = win32[0] && std::string_view(win32) == "when_required";
    printf(ok ? "PASS: SDK will see when_required\n"
              : "FAIL: SDK does NOT see when_required -> checksum mitigation no-ops\n");
    return ok ? 0 : 1;
#else
    setenv(name, "when_required", 1);
    printf("getenv() = %s  (Linux: single environ, always consistent)\n", getenv(name));
    return 0;
#endif
}
