#pragma once

#include <string>
#include <vector>

#include <mongocxx/collection-fwd.hpp>
#include <mongocxx/search_index_view-fwd.hpp>

#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/search_index_model.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {

///
/// Class representing a MongoDB search index view.
///
class search_index_view {
   public:
    search_index_view(search_index_view&&) noexcept;
    search_index_view& operator=(search_index_view&&) noexcept;

    search_index_view(const search_index_view&);
    search_index_view& operator=(const search_index_view&);

    ~search_index_view();

    ///
    /// @{
    ///
    /// Returns a cursor over all the search indexes.
    ///
    /// @param options
    ///   Options included in the aggregate operation.
    ///
    /// @return A cursor to the list of the search indexes returned.
    ///
    cursor list(const options::aggregate& options = options::aggregate());

    ///
    /// Returns a cursor over all the search indexes.
    ///
    /// @param session
    ///   The mongocxx::v_noabi::client_session with which to perform the list operation.
    /// @param options
    ///   Options included in the aggregate operation.
    ///
    /// @return A cursor to the list of the search indexes returned.
    ///
    cursor list(const client_session& session,
                const options::aggregate& options = options::aggregate());

    ///
    /// Returns a cursor over all the search indexes.
    ///
    /// @param name
    ///   The name of the search index to find.
    /// @param options
    ///   Options included in the aggregate operation.
    ///
    /// @return A cursor to the list of the search indexes returned.
    ///
    cursor list(bsoncxx::v_noabi::string::view_or_value name,
                const options::aggregate& options = options::aggregate());

    ///
    /// Returns a cursor over all the search indexes.
    ///
    /// @param session
    ///   The mongocxx::v_noabi::client_session with which to perform the list operation.
    /// @param name
    ///   The name of the search index to find.
    /// @param options
    ///   Options included in the aggregate operation.
    ///
    /// @return A cursor to the list of the search indexes returned.
    ///
    cursor list(const client_session& session,
                bsoncxx::v_noabi::string::view_or_value name,
                const options::aggregate& options = options::aggregate());

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// This is a convenience method for creating a single search index with a default name.
    ///
    /// @param definition
    ///    The document describing the search index to be created.
    ///
    /// @return The name of the created search index.
    ///
    std::string create_one(bsoncxx::v_noabi::document::view_or_value definition);

    ///
    /// This is a convenience method for creating a single search index with a default name.
    ///
    /// @param session
    ///   The mongocxx::v_noabi::client_session with which to perform the operation.
    /// @param definition
    ///    The document describing the search index to be created.
    ///
    /// @return The name of the created search index.
    ///
    std::string create_one(const client_session& session,
                           bsoncxx::v_noabi::document::view_or_value definition);

    ///
    /// This is a convenience method for creating a single search index.
    ///
    /// @param name
    ///    The name of the search index to create.
    /// @param definition
    ///    The document describing the search index to be created.
    ///
    /// @return The name of the created search index.
    ///
    std::string create_one(bsoncxx::v_noabi::string::view_or_value name,
                           bsoncxx::v_noabi::document::view_or_value definition);

    ///
    /// This is a convenience method for creating a single search index.
    ///
    /// @param session
    ///   The mongocxx::v_noabi::client_session with which to perform the operation.
    /// @param name
    ///   The name of the search index to create.
    /// @param definition
    ///   The document describing the search index to be created.
    ///
    /// @return The name of the created search index.
    ///
    std::string create_one(const client_session& session,
                           bsoncxx::v_noabi::string::view_or_value name,
                           bsoncxx::v_noabi::document::view_or_value definition);

    ///
    /// This is a convenience method for creating a single search index.
    ///
    /// @param model
    ///   The search index model to create.
    ///
    /// @return The name of the created index.
    ///
    std::string create_one(const search_index_model& model);

    ///
    /// This is a convenience method for creating a single search index.
    ///
    /// @param session
    ///   The mongocxx::v_noabi::client_session with which to perform the operation.
    /// @param model
    ///   The search index model to create.
    ///
    /// @return The name of the created index.
    ///
    std::string create_one(const client_session& session, const search_index_model& model);

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Creates multiple search indexes in the collection.
    ///
    /// @param models
    ///   The search index models to create.
    ///
    /// @return The names of the created indexes.
    ///
    std::vector<std::string> create_many(const std::vector<search_index_model>& models);

    ///
    /// Creates multiple search indexes in the collection.
    ///
    /// @param session
    ///   The mongocxx::v_noabi::client_session with which to perform the operation.
    /// @param models
    ///   The search index models to create.
    ///
    /// @return The names of the created indexes.
    ///
    std::vector<std::string> create_many(const client_session& session,
                                         const std::vector<search_index_model>& models);

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Drops a single search index from the collection by the index name.
    ///
    /// @param name
    ///    The name of the search index to drop.
    ///
    void drop_one(bsoncxx::v_noabi::string::view_or_value name);

    ///
    /// Drops a single search index from the collection by the index name.
    ///
    /// @param session
    ///   The mongocxx::v_noabi::client_session with which to perform the operation.
    /// @param name
    ///   The name of the search index to drop.
    ///
    void drop_one(const client_session& session, bsoncxx::v_noabi::string::view_or_value name);

    ///
    /// @}
    ///

    ///
    /// @{
    ///
    /// Updates a single search index from the collection by the search index name.
    ///
    /// @param name
    ///   The name of the search index to update.
    /// @param definition
    ///   The definition to update the search index to.
    ///
    void update_one(bsoncxx::v_noabi::string::view_or_value name,
                    bsoncxx::v_noabi::document::view_or_value definition);

    ///
    /// Updates a single search index from the collection by the search index name.
    ///
    /// @param session
    ///   The mongocxx::v_noabi::client_session with which to perform the operation.
    /// @param name
    ///   The name of the search index to update.
    /// @param definition
    ///   The definition to update the search index to.
    ///
    void update_one(const client_session& session,
                    bsoncxx::v_noabi::string::view_or_value name,
                    bsoncxx::v_noabi::document::view_or_value definition);

    ///
    /// @}
    ///

   private:
    friend ::mongocxx::v_noabi::collection;

    class MONGOCXX_PRIVATE impl;

    MONGOCXX_PRIVATE search_index_view(void* coll, void* client);

    MONGOCXX_PRIVATE std::vector<std::string> _create_many_helper(
        bsoncxx::v_noabi::array::view created_indexes);

    MONGOCXX_PRIVATE const impl& _get_impl() const;

    MONGOCXX_PRIVATE impl& _get_impl();

   private:
    std::unique_ptr<impl> _impl;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
