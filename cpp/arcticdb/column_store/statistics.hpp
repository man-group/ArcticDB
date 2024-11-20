#include <typeindex>
#include <memory>

#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {
class IntegerStatistics {
    struct Concept {
        virtual ~Concept() = default;
        [[nodiscard]] virtual const void* get_min() const = 0;

        [[nodiscard]] virtual const void* get_max() const = 0;
        virtual void set_min(const void*) = 0;
        virtual void set_max(const void*) = 0;
        [[nodiscard]] virtual std::type_index type() const = 0;
    };

    template<typename T>
    struct Model : Concept {
        T min_;
        T max_;
        [[nodiscard]] const void* get_min() const override { return &min_; }
        [[nodiscard]] const void* get_max() const override { return &max_; }
        void set_min(const void* min) override { min_ = *static_cast<const T*>(min); }
        void set_max(const void* max) override { max_ = *static_cast<const T*>(max); }

        [[nodiscard]] std::type_index type() const override { return typeid(T); }
    };

    std::unique_ptr<Concept> impl_;
    size_t unique_count_ = 0;
    bool sorted_ = true;

public:
    template<typename T>
    void initialize() {
        impl_ = std::make_unique<Model<T>>();
    }

    [[nodiscard]] bool empty() const {
        return !impl_;
    }

    template<typename T>
    const T& get_min() const {
        util::check(impl_->type() != typeid(T), "Bad type case in statistics get_min");
        return *static_cast<const T*>(impl_->get_min());
    }

    template<typename T>
    const T& get_max() const {
        util::check(impl_->type() != typeid(T), "Bad type case in statistics get_max");
        return *static_cast<const T*>(impl_->get_min());
    }

    template<typename T>
    void set_min(const void* t) const {
        util::check(impl_->type() == typeid(T), "Bad type case in statistics set_min {} != {}", impl_->type().name(), typeid(T).name());
        impl_->set_min(t);
    }

    template<typename T>
    void set_max(const void* t) const {
        util::check(impl_->type() == typeid(T), "Bad type case in statistics get_max {} != {}", impl_->type().name(), typeid(T).name());
        impl_->set_max(t);
    }

    void set_unique_count(size_t count) {
        unique_count_ = count;
    }

    [[nodiscard]] size_t get_unique_count() const { return unique_count_; }

    void set_sorted(bool sorted) {
        sorted_ = sorted;
    }

    [[nodiscard]] size_t get_sorted() const { return sorted_; }
};

class FloatingPointStatistics {
    double max_;
    double min_;
    size_t unique_count_;
    bool sorted_;

public:
    [[nodiscard]] double get_max() const { return max_; }
    [[nodiscard]] double get_min() const { return min_; }
    [[nodiscard]] size_t get_unique_count() const { return unique_count_; }

    void set_max(double max) { max_ = max; }
    void set_min(double min) { min_ = min; }
    void set_unique_count(size_t count) { unique_count_ = count; }
    void set_sorted(bool sorted) { sorted_ = sorted; }

    [[nodiscard]] size_t get_sorted() const { return sorted_; }
};

class StringStatistics {
        size_t unique_count_;

public:
    [[nodiscard]] size_t get_unique_count() const { return unique_count_; }

    void set_unique_count(size_t count) { unique_count_ = count; }
};

using Statistics = std::variant<std::monostate, IntegerStatistics, FloatingPointStatistics, StringStatistics>;

}