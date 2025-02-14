// Copyright 2023 MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <mongocxx/bulk_write-fwd.hpp>
#include <mongocxx/change_stream-fwd.hpp>
#include <mongocxx/client-fwd.hpp>
#include <mongocxx/client_encryption-fwd.hpp>
#include <mongocxx/client_session-fwd.hpp>
#include <mongocxx/collection-fwd.hpp>
#include <mongocxx/cursor-fwd.hpp>
#include <mongocxx/database-fwd.hpp>
#include <mongocxx/events/command_failed_event-fwd.hpp>
#include <mongocxx/events/command_started_event-fwd.hpp>
#include <mongocxx/events/command_succeeded_event-fwd.hpp>
#include <mongocxx/events/heartbeat_failed_event-fwd.hpp>
#include <mongocxx/events/heartbeat_started_event-fwd.hpp>
#include <mongocxx/events/heartbeat_succeeded_event-fwd.hpp>
#include <mongocxx/events/server_changed_event-fwd.hpp>
#include <mongocxx/events/server_closed_event-fwd.hpp>
#include <mongocxx/events/server_description-fwd.hpp>
#include <mongocxx/events/server_opening_event-fwd.hpp>
#include <mongocxx/events/topology_changed_event-fwd.hpp>
#include <mongocxx/events/topology_closed_event-fwd.hpp>
#include <mongocxx/events/topology_description-fwd.hpp>
#include <mongocxx/events/topology_opening_event-fwd.hpp>
#include <mongocxx/exception/authentication_exception-fwd.hpp>
#include <mongocxx/exception/bulk_write_exception-fwd.hpp>
#include <mongocxx/exception/error_code-fwd.hpp>
#include <mongocxx/exception/exception-fwd.hpp>
#include <mongocxx/exception/gridfs_exception-fwd.hpp>
#include <mongocxx/exception/logic_error-fwd.hpp>
#include <mongocxx/exception/operation_exception-fwd.hpp>
#include <mongocxx/exception/query_exception-fwd.hpp>
#include <mongocxx/exception/server_error_code-fwd.hpp>
#include <mongocxx/exception/write_exception-fwd.hpp>
#include <mongocxx/gridfs/bucket-fwd.hpp>
#include <mongocxx/gridfs/downloader-fwd.hpp>
#include <mongocxx/gridfs/uploader-fwd.hpp>
#include <mongocxx/hint-fwd.hpp>
#include <mongocxx/index_model-fwd.hpp>
#include <mongocxx/index_view-fwd.hpp>
#include <mongocxx/instance-fwd.hpp>
#include <mongocxx/logger-fwd.hpp>
#include <mongocxx/model/delete_many-fwd.hpp>
#include <mongocxx/model/delete_one-fwd.hpp>
#include <mongocxx/model/insert_one-fwd.hpp>
#include <mongocxx/model/replace_one-fwd.hpp>
#include <mongocxx/model/update_many-fwd.hpp>
#include <mongocxx/model/update_one-fwd.hpp>
#include <mongocxx/model/write-fwd.hpp>
#include <mongocxx/options/aggregate-fwd.hpp>
#include <mongocxx/options/apm-fwd.hpp>
#include <mongocxx/options/auto_encryption-fwd.hpp>
#include <mongocxx/options/bulk_write-fwd.hpp>
#include <mongocxx/options/change_stream-fwd.hpp>
#include <mongocxx/options/client-fwd.hpp>
#include <mongocxx/options/client_encryption-fwd.hpp>
#include <mongocxx/options/client_session-fwd.hpp>
#include <mongocxx/options/count-fwd.hpp>
#include <mongocxx/options/data_key-fwd.hpp>
#include <mongocxx/options/delete-fwd.hpp>
#include <mongocxx/options/distinct-fwd.hpp>
#include <mongocxx/options/encrypt-fwd.hpp>
#include <mongocxx/options/estimated_document_count-fwd.hpp>
#include <mongocxx/options/find-fwd.hpp>
#include <mongocxx/options/find_one_and_delete-fwd.hpp>
#include <mongocxx/options/find_one_and_replace-fwd.hpp>
#include <mongocxx/options/find_one_and_update-fwd.hpp>
#include <mongocxx/options/find_one_common_options-fwd.hpp>
#include <mongocxx/options/gridfs/bucket-fwd.hpp>
#include <mongocxx/options/gridfs/upload-fwd.hpp>
#include <mongocxx/options/index-fwd.hpp>
#include <mongocxx/options/index_view-fwd.hpp>
#include <mongocxx/options/insert-fwd.hpp>
#include <mongocxx/options/pool-fwd.hpp>
#include <mongocxx/options/range-fwd.hpp>
#include <mongocxx/options/replace-fwd.hpp>
#include <mongocxx/options/rewrap_many_datakey-fwd.hpp>
#include <mongocxx/options/server_api-fwd.hpp>
#include <mongocxx/options/tls-fwd.hpp>
#include <mongocxx/options/transaction-fwd.hpp>
#include <mongocxx/options/update-fwd.hpp>
#include <mongocxx/pipeline-fwd.hpp>
#include <mongocxx/pool-fwd.hpp>
#include <mongocxx/read_concern-fwd.hpp>
#include <mongocxx/read_preference-fwd.hpp>
#include <mongocxx/result/bulk_write-fwd.hpp>
#include <mongocxx/result/delete-fwd.hpp>
#include <mongocxx/result/gridfs/upload-fwd.hpp>
#include <mongocxx/result/insert_many-fwd.hpp>
#include <mongocxx/result/insert_one-fwd.hpp>
#include <mongocxx/result/replace_one-fwd.hpp>
#include <mongocxx/result/rewrap_many_datakey-fwd.hpp>
#include <mongocxx/result/update-fwd.hpp>
#include <mongocxx/search_index_model-fwd.hpp>
#include <mongocxx/search_index_view-fwd.hpp>
#include <mongocxx/uri-fwd.hpp>
#include <mongocxx/validation_criteria-fwd.hpp>
#include <mongocxx/write_concern-fwd.hpp>
#include <mongocxx/write_type-fwd.hpp>
