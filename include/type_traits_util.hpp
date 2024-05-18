#ifndef LANCEDB_INCLUDE_TYPE_TRAITS_UTIL_HPP_
#define LANCEDB_INCLUDE_TYPE_TRAITS_UTIL_HPP_

#include <type_traits>
#include <functional>

#ifndef LANCEDB_LOGD
#define LANCEDB_LOGD(fmt, ...) fprintf(stdout, "lancedb debug: " fmt "\n", ## __VA_ARGS__)
#define LANCEDB_LOGE(fmt, ...) fprintf(stderr, "lancedb error: " fmt "\n", ## __VA_ARGS__)
#endif // LANCEDB_LOGD

namespace lancedb {

template <class T>
struct function_helper;

template <class T, class ...Args>
struct function_helper<T(*)(Args...)> {
  using return_type = T;
  using arguments = std::tuple<Args...>;
};

#if __cplusplus >= 201400
template <class T, class ...Args>
struct function_helper<T(*)(Args...) noexcept> :
    public function_helper<T(*)(Args...)> { };
#endif

template <class T, class ...Args>
struct function_helper<std::function<T(Args...)>> {
  using return_type = T;
  using arguments = std::tuple<Args...>;
};

template <class T, class ...Args>
struct function_helper<T(Args...)> {
  using return_type = T;
  using arguments = std::tuple<Args...>;
};

#if __cplusplus >= 201400
template <class T, class ...Args>
struct function_helper<T(Args...) noexcept> :
    public function_helper<T(Args...)> { };
#endif

template <class T, class C, class ...Args>
struct function_helper<T (C::*)(Args...)> {
  using return_type = T;
  using arguments = std::tuple<Args...>;
  using class_type = C;
  using functional_type = std::function<T (Args...)>;
};

#if __cplusplus >= 201400
template <class T, class C, class ...Args>
struct function_helper<T (C::*)(Args...) noexcept> :
    public function_helper<T (C::*)(Args...)> { };
#endif

template <class Func>
struct function_returns_void {
  constexpr const static bool value =
      std::is_same<typename function_helper<Func>::return_type, void>::value;
};

}

#endif // LANCEDB_INCLUDE_TYPE_TRAITS_UTIL_HPP_