#pragma once

namespace rc {
namespace gen {
namespace boost {

template <typename T>
Gen<::boost::optional<T>> optional(Gen<T> gen) {
  return gen::map<Maybe<T>>(gen::maybe(std::move(gen)),
                            [](Maybe<T> &&m) {
                              return m ? ::boost::optional<T>(std::move(*m))
                                       : ::boost::optional<T>();
                            });
}

} // namespace boost
} // namespace gen

template <typename T>
struct Arbitrary<boost::optional<T>> {
  static Gen<boost::optional<T>> arbitrary() {
    return gen::boost::optional(gen::arbitrary<T>());
  }
};

template <typename T>
void showValue(const boost::optional<T> &x, std::ostream &os) {
  if (x) {
    show(*x, os);
  } else {
    os << "boost::none";
  }
}

} // namespace rc
