#ifdef ARCTICDB_PERFORMANCE_SAMPLING
#include <perfetto.h>
#include <memory>

namespace arcticdb::tracing {

struct TracingSessionCofig {
    std::vector<std::string> disabled_categories;
    std::vector<std::string> enabled_categories = {"*"};
    size_t buffer_size_kb = 1024;
};

class TracingSession {
  public:
    TracingSession(const TracingSessionCofig& cfg);
    void start();
    void stop();
    void stop_and_flush(std::string_view file_dest);
    void flush(std::string_view file_dest);

  private:
    std::unique_ptr<perfetto::TracingSession> session_;
};

} // namespace arcticdb::tracing

#endif
