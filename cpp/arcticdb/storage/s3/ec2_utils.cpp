#include <aws/core/platform/Environment.h>
#include <curl/curl.h>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/configs_map.hpp>

namespace arcticdb::storage::s3 {
// We only care about the response codes, so we just pass a dummy write function to libcurl to not print the responses.
size_t write_callback([[maybe_unused]] void* buffer, size_t size, size_t nmemb, [[maybe_unused]] void* userp) {
    return size * nmemb;
}

// A fast check to identify whether we're running on AWS EC2.
// According to AWS docs a reliable way to identify if we're in EC2 is to query the instance metadata service:
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/identify_ec2_instances.html
//
// Since there are two versions IMDSv1 and IMDSv2 we first try to connect to v2 and if we fail then attempt the legacy
// v1 connection. If both fail we're most likely running outside of EC2 (unless IMDS is under heavy load and takes more
// than 100ms to respond)
bool has_connection_to_ec2_imds() {
    CURL* curl = curl_easy_init();
    if (!curl) {
        return false;
    }
    CURLcode res;

    // We allow overriding the default 169.254.169.254 endpoint for tests.
    auto imds_endpoint = ConfigsMap::instance()->get_string("EC2.TestIMDSEndpointOverride", "http://169.254.169.254");
    // Suggested approach by aws docs for IMDSv2
    // (https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html): curl -X PUT
    // "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" The below libcurl
    // options should mimic the command above.
    curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "X-aws-ec2-metadata-token-ttl-seconds: 21600");
    curl_easy_setopt(curl, CURLOPT_URL, fmt::format("{}/latest/api/token", imds_endpoint).c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);

    // We use a default timeout of 100ms because it should be enough for IMDS to respond. Be wary of increasing the
    // timeout since we need to wait the whole 100ms every time we init the aws sdk outside of aws ec2.
    long timeout = ConfigsMap::instance()->get_int("EC2.IMDSQueryTimeoutMs", 100);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout);

    res = curl_easy_perform(curl);
    curl_slist_free_all(headers);

    if (res == CURLE_OK) {
        curl_easy_cleanup(curl);
        return true;
    }

    // If attempting to connect via IMDSv2 fails we want to attempt a connection to IMDSv1:
    // curl http://169.254.169.254/latest/dynamic/instance-identity/document
    curl_easy_reset(curl);
    curl_easy_setopt(
            curl, CURLOPT_URL, fmt::format("{}/latest/dynamic/instance-identity/document", imds_endpoint).c_str()
    );
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout);

    res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res == CURLE_OK) {
        return true;
    }
    return false;
}

bool is_running_inside_aws_fast() {
    // If any of the below env vars are set we are likely running inside AWS.
    for (auto name : std::initializer_list<const char*>{
                 "AWS_EC2_METADATA_DISABLED", "AWS_DEFAULT_REGION", "AWS_REGION", "AWS_EC2_METADATA_SERVICE_ENDPOINT"
         }) {
        if (!Aws::Environment::GetEnv(name).empty()) {
            ARCTICDB_RUNTIME_DEBUG(
                    log::storage(), "Fast check determined we're running inside AWS because env var {} is set.", name
            );
            return true;
        }
    }
    if (has_connection_to_ec2_imds()) {
        ARCTICDB_RUNTIME_DEBUG(
                log::storage(),
                "Fast check determined we're running inside AWS because we managed to connect to the instance metadata "
                "service."
        );
        return true;
    } else {
        ARCTICDB_RUNTIME_DEBUG(
                log::storage(),
                "Fast check determined we're NOT running inside AWS because we didn't find aws env vars and couldn't "
                "connect to instance metadata service"
        );
        return false;
    }
}
} // namespace arcticdb::storage::s3
