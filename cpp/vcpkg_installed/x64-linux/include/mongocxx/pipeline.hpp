// Copyright 2014 MongoDB Inc.
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

#include <cstdint>
#include <memory>
#include <string>

#include <bsoncxx/array/view.hpp>
#include <bsoncxx/array/view_or_value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/view_or_value.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
MONGOCXX_INLINE_NAMESPACE_BEGIN

class client;
class collection;
class database;

///
/// Class representing a MongoDB aggregation pipeline.
///
class MONGOCXX_API pipeline {
   public:
    ///
    /// Creates a new aggregation pipeline.
    ///
    /// @see https://docs.mongodb.com/manual/core/aggregation-pipeline/
    ///
    pipeline();

    ///
    /// Move constructs a pipeline.
    ///
    pipeline(pipeline&&) noexcept;

    ///
    /// Move assigns a pipeline.
    ///
    pipeline& operator=(pipeline&&) noexcept;

    ///
    /// Destroys a pipeline.
    ///
    ~pipeline();

    ///
    /// Adds new fields to documents.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/addFields/
    ///
    /// @param fields_to_add
    ///   A document specifying the fields to add.  For each field specified in this parameter, a
    ///   corresponding field will be added to the documents, where the value of the added field is
    ///   the result of evaluating the specified expression.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& add_fields(bsoncxx::document::view_or_value fields_to_add);

    ///
    /// Categorizes documents into groups, called buckets, based on a specified expression and
    /// bucket boundaries.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/bucket/
    ///
    /// @param bucket_args
    ///   The specification for the bucket operation.  The required fields `groupBy` and
    ///   `boundaries` must be included.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& bucket(bsoncxx::document::view_or_value bucket_args);

    ///
    /// Categorizes documents into a specific number of groups, called buckets, based on a
    /// specified expression.  Bucket boundaries are automatically determined in an attempt to
    /// evenly distribute the documents into the specified number of buckets.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/bucketAuto/
    ///
    /// @param bucket_auto_args
    ///   The specification for the bucket_auto operation.  This required fields `groupBy` and
    ///   `buckets` must be included.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& bucket_auto(bsoncxx::document::view_or_value bucket_auto_args);

    ///
    /// Returns statistics regarding a collection or view.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/collStats/
    ///
    /// @param coll_stats_args
    ///   The specification for the coll_stats operation.  See link above for a list of valid
    ///   options.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& coll_stats(
        bsoncxx::document::view_or_value coll_stats_args = bsoncxx::document::view{});

    ///
    /// Returns a document containing a count of the number of documents input to the stage.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/count/
    ///
    /// @param field
    ///   Name of the field for the count to be written to.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& count(std::string field);

    ///
    /// Returns a stream of documents containing information on active and/or dormant
    /// operations as well as inactive sessions that are holding locks as part of a
    /// transaction. The stage returns a document for each operation or session.
    ///
    /// This stage must be used with database aggregate on the 'admin' database.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/currentOp/
    ///
    /// @param current_op_args
    ///   A document containing the arguments for the current_op operation.
    ///
    /// @return
    ///   A reference to the object on which this method is being called.
    ///
    pipeline& current_op(bsoncxx::document::view_or_value current_op_args);

    ///
    /// Processes multiple aggregation pipelines within a single stage on the same set of input
    /// documents.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/facet/
    ///
    /// @param facet_args
    ///   The specification for the facet operation.  Each field in the the provided document should
    ///   specify an aggregation pipeline, as an array.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& facet(bsoncxx::document::view_or_value facet_args);

    ///
    /// Appends a stage to this pipeline object.
    ///
    /// Use this method to run new pipeline stages that are not yet offered as
    /// explicit methods on this class. Pass in a document describing a
    /// pipeline stage:
    ///
    ///    { "$addFields" : { "field" : 0 }}
    ///
    /// @param stage
    ///   A bson document view or value describing a pipeline stage.
    ///
    /// @return
    ///   A reference to this object on which this member function is being called.
    ///
    pipeline& append_stage(bsoncxx::document::view_or_value stage);

    ///
    /// Appends stages to this pipeline object from the given bson array.
    ///
    /// Use this method to run new pipeline stages that are not yet offered as
    /// explicit methods on this class. Pass in an array of documents, where each
    /// document in the array describes a pipeline stage:
    ///
    ///    [ { "$addFields" : { "field" : 0 } }, { "$newStage" : ... }, ... ]
    ///
    /// @param stages
    ///   A bson array containing documents describing pipeline stages.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.
    ///
    pipeline& append_stages(bsoncxx::array::view_or_value stages);

    ///
    /// Outputs documents in order of nearest to farthest from a specified point.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/geoNear/
    ///
    /// @param geo_near_args
    ///   The specification for the geo_near operation.  The required fields `near` and
    ///   `distanceField` must be included.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& geo_near(bsoncxx::document::view_or_value geo_near_args);

    ///
    /// Performs a recursive search on a collection.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/graphLookup/
    ///
    /// @param graph_lookup_args
    ///   The specification for the graph_lookup operation.  The required fields `from`,
    ///   `connectFromField`, `startWith`, `connectToField`, and `as` must be included.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& graph_lookup(bsoncxx::document::view_or_value graph_lookup_args);

    ///
    /// Groups documents by some specified expression and outputs to the next stage a
    /// document for each distinct grouping. The output documents contain an `_id` field
    /// which contains the the distinct key for that group. The output documents can also
    /// contain computed fields that hold the values of some accumulator expression grouped
    /// by the group's `_id` field.
    ///
    /// @note group does not order output documents.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/group/
    ///
    /// @param group_args
    ///   The specification for the group operation.  The required field `_id` must be included.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& group(bsoncxx::document::view_or_value group_args);

    ///
    /// Returns statistics regarding the use of each index for the collection.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/indexStats/
    ///
    pipeline& index_stats();

    ///
    /// Limits the number of documents passed to the next stage in the pipeline.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/limit/
    ///
    /// @param limit
    ///   The number of documents to which output should be limited.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& limit(std::int32_t limit);

    ///
    /// Lists the sessions cached in memory by the mongod or mongos instance.
    ///
    /// This option must be used with database aggregate.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/listLocalSessions/
    ///
    /// @param list_local_sessions_args
    ///   A document containing the arguments for list_local_sessions.
    ///
    /// @return
    ///   A reference to the object on which this method is being called.
    ///
    pipeline& list_local_sessions(bsoncxx::document::view_or_value list_local_sessions_args);

    ///
    /// Lists all sessions stored in the system.sessions collection in the config database.
    /// These sessions are visible to all members of the MongoDB deployment.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/listSessions/
    ///
    /// @param list_sessions_args
    ///   A document containing the arguments for list_sessions.
    ///
    /// @return
    ///   A reference to the object on which this method is being called.
    ///
    pipeline& list_sessions(bsoncxx::document::view_or_value list_sessions_args);

    ///
    /// Performs a left outer join to an unsharded collection in the same database to filter in
    /// documents from the "joined" collection for processing.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/
    ///
    /// @param lookup_args
    ///   The specification for the lookup operation.  The required fields `from`, `localField`,
    ///   `foreignField`, and `as` must be included.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& lookup(bsoncxx::document::view_or_value lookup_args);

    ///
    /// Filters the documents. Only the documents that match the condition(s) specified by the
    /// `filter` will continue to the next pipeline stage.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/match/
    ///
    /// @param filter
    ///   The filter.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& match(bsoncxx::document::view_or_value filter);

    ///
    /// Outputs the aggregation results to a collection.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/merge/
    ///
    /// @param merge_args
    ///   The specification for the merge options. Must include an `into` field that
    ///   is either a collection name or a subdocument of the form:
    ///   { `db` : &lt;db name&gt;, `coll` : &lt;collection name&gt; }.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.
    ///
    pipeline& merge(bsoncxx::document::view_or_value merge_args);

    ///
    /// Takes documents returned by the aggregation pipeline and writes them to a specified
    /// collection. This stage must be the last stage in the pipeline. The out operator lets the
    /// aggregation framework return result sets of any size.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/out/
    ///
    /// @param collection_name
    ///   The name of the collection where the output documents should go.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& out(std::string collection_name);

    ///
    /// Projects a subset of the fields in the documents to the next stage of the pipeline.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/project/
    ///
    /// @param projection
    ///   The projection specification.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& project(bsoncxx::document::view_or_value projection);

    ///
    /// Restricts the contents of the documents based on information stored in the documents
    /// themselves.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/redact/
    ///
    /// @param restrictions
    ///   The document restrictions.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& redact(bsoncxx::document::view_or_value restrictions);

    ///
    /// Promotes a specified document to the top level and replaces all other fields.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/replaceRoot/
    ///
    /// @param replace_root_args
    ///   The specification for the replace_root operation.  The required field `newRoot` must be
    ///   included.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& replace_root(bsoncxx::document::view_or_value replace_root_args);

    ///
    /// Randomly selects the specified number of documents that pass into the stage and passes the
    /// remaining documents to the next stage in the pipeline.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/sample/
    ///
    /// @param size
    ///   The number of input documents to select.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& sample(std::int32_t size);

    ///
    /// Skips over the specified number of documents that pass into the stage and passes the
    /// remaining documents to the next stage in the pipeline.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/skip/
    ///
    /// @param docs_to_skip
    ///   The number of input documents to skip.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& skip(std::int32_t docs_to_skip);

    ///
    /// Sorts all input documents and returns them to the pipeline in sorted order.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/sort/
    ///
    /// @param ordering
    ///   Document specifying the ordering by which the documents are sorted.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    pipeline& sort(bsoncxx::document::view_or_value ordering);

    ///
    /// Groups incoming documents based on the value of a specified expression, then computes the
    /// count of documents in each distinct group.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/
    ///
    /// @param field_expression
    ///   The expression to group by, as an object.  The expression can not evaluate to an object.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @note
    ///   This overload of sort_by_count() is intended to be used when the desired sort is over a
    ///   grouping of the result of a complex expression computed from the input documents.
    ///
    pipeline& sort_by_count(bsoncxx::document::view_or_value field_expression);

    ///
    /// Groups incoming documents based on the value of a specified expression, then computes the
    /// count of documents in each distinct group.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/sortByCount/
    ///
    /// @param field_expression
    ///   The expression to group by, as a string.  To specify a field path, prefix the field path
    ///   with a dollar sign (`$`).
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @note
    ///   This overload of sort_by_count() is intended to be used when the desired sort is over a
    ///   grouping of the value of a particular element in the input documents.
    ///
    pipeline& sort_by_count(std::string field_expression);

    ///
    /// Deconstructs an array field from the input documents to output a document for each element.
    /// Each output document is an input document with the value of its array field replaced by
    /// an element from the unwound array.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/unwind/
    ///
    /// @param unwind_args
    ///   The specification for the unwind operation.  The required field path must be included.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @note
    ///   This overload of unwind() is intended to be used when additional options other than the
    ///   field name need to be specified.
    ///
    pipeline& unwind(bsoncxx::document::view_or_value unwind_args);

    ///
    /// Deconstructs an array field from the input documents to output a document for each element.
    /// Each output document is an input document with the value of its array field replaced by
    /// an element from the unwound array.
    ///
    /// @see https://docs.mongodb.com/manual/reference/operator/aggregation/unwind/
    ///
    /// @param field_name
    ///   The name of the field to unwind.
    ///
    /// @return
    ///   A reference to the object on which this member function is being called.  This facilitates
    ///   method chaining.
    ///
    /// @note
    ///   This overload of unwind() is intended to be used when no options other than the field name
    ///   need to be specified.
    ///
    pipeline& unwind(std::string field_name);

    ///
    /// @return A view of the underlying BSON array this pipeline represents.
    ///
    bsoncxx::array::view view_array() const;

   private:
    friend class client;
    friend class collection;
    friend class database;

    class MONGOCXX_PRIVATE impl;
    std::unique_ptr<impl> _impl;
};

MONGOCXX_INLINE_NAMESPACE_END
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
