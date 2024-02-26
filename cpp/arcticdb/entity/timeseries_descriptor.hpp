/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/field_collection.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>

namespace arcticdb {

struct TimeseriesDescriptor {
  using Proto = arcticdb::proto::descriptors::TimeSeriesDescriptor;

  std::shared_ptr<Proto> proto_ = std::make_shared<Proto>();
  std::shared_ptr<FieldCollection> fields_ = std::make_shared<FieldCollection>();
  TimeseriesDescriptor() = default;

  TimeseriesDescriptor(std::shared_ptr<Proto> proto, std::shared_ptr<FieldCollection> fields) :
    proto_(std::move(proto)),
    fields_(std::move(fields)) {
  }

  [[nodiscard]] std::shared_ptr<FieldCollection> fields_ptr() const  {
      return fields_;
  }

  [[nodiscard]] std::shared_ptr<Proto> proto_ptr() const {
      return proto_;
  }

  [[nodiscard]] bool proto_is_null() const {
      return !proto_;
  }

  void set_stream_descriptor(const StreamDescriptor& desc) {
      fields_ = std::make_shared<FieldCollection>(desc.fields().clone());
      proto_ = std::make_shared<Proto>();
      proto_->mutable_stream_descriptor()->CopyFrom(desc.proto());
  }

  [[nodiscard]] const FieldCollection& fields() const {
      return *fields_;
  }

  [[nodiscard]] FieldCollection& mutable_fields() {
      return *fields_;
  }

  [[nodiscard]] Proto& mutable_proto() {
       return *proto_;
  }

  [[nodiscard]] const Proto& proto() const {
      return *proto_;
  }

  [[nodiscard]] TimeseriesDescriptor clone() const {
      auto proto = std::make_shared<Proto>();
      proto->CopyFrom(*proto_);
      return {std::move(proto), std::make_shared<FieldCollection>(fields_->clone())};
  }

  [[nodiscard]] StreamDescriptor as_stream_descriptor() const {
      auto stream_descriptor = std::make_shared<arcticdb::proto::descriptors::StreamDescriptor>();
      stream_descriptor->CopyFrom(proto_->stream_descriptor());
      return StreamDescriptor(stream_descriptor, fields_);
  }

  void copy_to_self_proto() {
      proto_->mutable_stream_descriptor()->mutable_fields()->Clear();
      for(const auto& field : *fields_) {
          auto new_field = proto_->mutable_stream_descriptor()->mutable_fields()->Add();
          new_field->set_name(std::string(field.name()));
          new_field->mutable_type_desc()->set_dimension(static_cast<uint32_t>(field.type().dimension()));
          set_data_type(field.type().data_type(), *new_field->mutable_type_desc());
      }
  }
};

} //namespace arcticdb

namespace fmt {
    template<>
    struct formatter<arcticdb::TimeseriesDescriptor> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::TimeseriesDescriptor &tsd, FormatContext &ctx) const {
            if(!tsd.fields_ptr())
                return fmt::format_to(ctx.out(), "TimeseriesDescriptor<fields=empty, proto={}>", tsd.proto());

            return fmt::format_to(ctx.out(), "TimeseriesDescriptor<fields={}, proto={}>", tsd.fields(), tsd.proto());
        }
    };

    template<>
    struct formatter<arcticdb::TimeseriesDescriptor::Proto> {
        template<typename ParseContext>
        constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

        template<typename FormatContext>
        auto format(const arcticdb::TimeseriesDescriptor::Proto &tsd, FormatContext &ctx) const {
            return fmt::format_to(ctx.out(), "{}", tsd.ShortDebugString());
        }
    };

} //namespace fmt
