namespace arcticdb::storage::s3 {
// A faster check than aws-sdk's attempt to connect with retries to ec2 imds
bool is_running_inside_aws_fast();
} // namespace arcticdb::storage::s3
