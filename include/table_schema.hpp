#ifndef LANCEDB_INCLUDE_LANCEDB_SCHEMA_HPP_
#define LANCEDB_INCLUDE_LANCEDB_SCHEMA_HPP_

#include <vector>

#include "type_traits_util.hpp"
#include "lancedb.hpp"

#include <typeinfo>
#include <memory>
#include <cstring>

#if LANCEDB_DEBUG_DEMANGLE_TYPE
#include <cxxabi.h>

static std::string DemangleSymbol(const std::type_info& type) {
  int status;
  std::unique_ptr<char, void(*)(void*)> result(
      abi::__cxa_demangle(type.name(), nullptr, 0, &status),
      std::free
  );
  return (status==0) ? result.get() : type.name();
}

#define DEMANGLE_TYPE(type) \
    do {\
      std::string demangle_field_ty = DemangleSymbol(typeid(type));\
      printf("   " #type "   size: %zd    : %s\n", sizeof(type), demangle_field_ty.c_str());\
    } while(0)
#endif // LANCEDB_DEBUG_DEMANGLE_TYPE

namespace lancedb {

namespace internal {

template <class T>
struct NativeFieldTypeHelper {
  using type = T;
  const static bool is_scalar_type = true;
  const static bool is_memory_continuous = true;
};

template <>
struct NativeFieldTypeHelper<std::string> {
  using type = const char*;
  const static bool is_scalar_type = true;
  // Note: string can be treated as a memory_continuous type
  //       since we directly assign std::string, and const char*, etc, with std::string,
  //       are identical to other scalar types.
  const static bool is_memory_continuous = true;
};

template <>
struct NativeFieldTypeHelper<const char*>
    : public NativeFieldTypeHelper<std::string> {};

template <>
struct NativeFieldTypeHelper<char*>
    : public NativeFieldTypeHelper<std::string> {};

template <>
struct NativeFieldTypeHelper<BinaryData> {
  using type = const uint8_t*;
  const static bool is_scalar_type = true;
  const static bool is_memory_continuous = false;
};

// write a NativeFieldTypeHelper for std::vector<T>
template <class T>
struct NativeFieldTypeHelper<std::vector<T>> {
  using type = T;
  const static bool is_scalar_type = false;
  const static bool is_memory_continuous = true;
};

static_assert(NativeFieldTypeHelper<int>::is_scalar_type, "int is a scalar type");
static_assert(NativeFieldTypeHelper<int>::is_memory_continuous, "float is memory continuous");
static_assert(NativeFieldTypeHelper<std::string>::is_scalar_type, "std::string is a scalar type");
static_assert(NativeFieldTypeHelper<std::string>::is_memory_continuous, "std::string is memory continuous");
static_assert(NativeFieldTypeHelper<const char*>::is_memory_continuous, "C-style string is memory continuous");
static_assert(NativeFieldTypeHelper<BinaryData>::is_scalar_type, "BinaryData is a scalar type");
static_assert(!NativeFieldTypeHelper<BinaryData>::is_memory_continuous, "std::string is not memory continuous");
static_assert(!NativeFieldTypeHelper<std::vector<int>>::is_scalar_type, "std::vector<int> is not a scalar type");
static_assert(NativeFieldTypeHelper<std::vector<int>>::is_memory_continuous, "std::vector<int> is memory continuous");

}

template <class AdapterType, template<class> class ContainerTy = std::vector>
class TableSchema {
public:
  explicit TableSchema(LanceDB& lancedb_conn) : lancedb_conn_(&lancedb_conn) {
    static_assert(std::is_class<AdapterType>::value, "AdapterType must be a class type");
  }

  using BeanType = typename AdapterType::bean_type;
  using BeanList = ContainerTy<BeanType>;
  static_assert(std::is_same<BeanType, typename BeanList::value_type>::value,
                "ContainerTy must be a container of BeanType");

  bool IsInited() const {
    return lancedb_conn_ != nullptr && lancedb_conn_->IsInited();
  }

  template <class... Args>
  using Tuple = std::tuple<Args...>;

  LanceDBError Run(const BeanList& beans) {
    if (!IsInited()) {
      return kLanceDBNotConnected;
    }
    return RunInternal(beans, std::make_index_sequence<kNumFields>());
  }

  TableSchema& SetCreateTable(bool create_table = true) {
    create_table_ = create_table;
    return *this;
  }

  TableSchema& SetCreateData(bool create_data = true) {
    create_data_ = create_data;
    return *this;
  }

  template <class FloatType>
  LanceDBError Query(const std::string& field_name,
                     const std::vector<FloatType>& embedding, LanceDB::SearchResults& results) {
    if (!IsInited()) {
      return kLanceDBNotConnected;
    }
    return lancedb_conn_->Query(AdapterType::table_name, field_name, embedding, results);
  }

  template<class FloatType, class BeanSearchResult>
  LanceDBError Query(const std::string& field_name,
                     const std::vector<FloatType>& embedding, BeanSearchResult& result) {
    LanceDB::SearchResults sr;
    auto err = Query("embedding", embedding, sr);
    if (err != kLanceDBSuccess) {
      return err;
    }
    err = QueryInternal(result.results, sr, std::make_index_sequence<AdapterType::N>());
    if (err != kLanceDBSuccess) {
      return err;
    }
    err = FillDistanceField(result.distances, sr);
    return err;
  }

  LanceDB& GetLanceDB() {
    return *lancedb_conn_;
  }

private:
  template<size_t ...I>
  LanceDBError RunInternal(const BeanList& beans, std::index_sequence<I...>) {
    std::vector<const char*> field_names = { AdapterType::template FieldName<I>()... };

    using DataTuple = Tuple<ContainerTy<
        std::decay_t<decltype(std::declval<AdapterType>().template FieldValue<I>(&beans[0]))>> ...>;
    DataTuple data;

    LanceDBError ret = kLanceDBSuccess;
    for (const BeanType& bean: beans) {
      ( std::get<I>(data).emplace_back(AdapterType::template FieldValue<I>(&bean)), ... );
    }

    std::string demangle_tmp;
#if LANCEDB_DEBUG_DEMANGLE_TYPE
    ( printf("field: %s    size: %zd     type: %s\n",
             field_names[I], std::get<I>(data).size(),
             (demangle_tmp = DemangleSymbol(typeid(std::decay_t<decltype(
                 std::declval<AdapterType>().template FieldValue<I>(&beans[0]))>))).c_str()), ... );
#endif
    auto data_tuple = std::make_tuple(LanceDB::FieldData(field_names[I], std::get<I>(data))...);
    auto create_inserter = [&](auto&&... args) {
      return lancedb_conn_->CreateBatchInserter(std::forward<decltype(args)>(args)...);
    };
    auto inserter = std::apply(create_inserter, data_tuple);
    if (create_table_) {
      ret = inserter.CreateTable(AdapterType::table_name);
      if (ret != kLanceDBSuccess) {
        return ret;
      }
    }
    if (create_data_) {
      ret = inserter.Insert(AdapterType::table_name);
    }
    return ret;
  }

  LanceDBError FillDistanceField(std::vector<float>& distance, const LanceDB::SearchResults& results) {
    lancedb_data_t data = results.Get();
    lancedb_field_data_t* data_field = nullptr;
    for (int i = 0; i < data.num_fields; ++i) {
      if (!strcmp(data.fields[i].name, "_distance")) {
        data_field = &data.fields[i];
        break;
      }
    }
    if (data_field == nullptr) {
      return kLanceDBFieldNotFound;
    }
    if (distance.empty()) {
      distance.resize(data_field->data_count);
    }
    float* data_ptr = (float*)data_field->data;
    for (int i=0; i<data_field->data_count; i++) {
      distance[i] = data_ptr[i];
    }
    return kLanceDBSuccess;
  }

  template <size_t I>
  LanceDBError FillBeanField(BeanList& beans, const char* field_name,
                             const LanceDB::SearchResults& results) {
    // LANCEDB_LOGD("Field Name: %s", field_name);
    using FieldType = std::decay_t<decltype(std::declval<AdapterType>().template FieldValue<I>(&beans[0]))>;
    using FieldNativeType = typename internal::NativeFieldTypeHelper<FieldType>::type;

//    DEMANGLE_TYPE(FieldType);
//    DEMANGLE_TYPE(FieldNativeType);

    lancedb_data_t data = results.Get();
    lancedb_field_data_t* data_field = nullptr;
    for (int i = 0; i < data.num_fields; ++i) {
      if (!strcmp(data.fields[i].name, field_name)) {
        data_field = &data.fields[i];
        break;
      }
    }

    if (data_field == nullptr) {
      LANCEDB_LOGD("note: no such field");
      return kLanceDBSuccess;
      // return kLanceDBFieldNotFound;
    }

    if (beans.empty()) {
      beans.resize(data_field->data_count);
      // LANCEDB_LOGD("bean list is empty, resize it to %ld", data_field->data_count);
    }
//    else {
//      LANCEDB_LOGD("bean list is not empty, data size = %ld, bean size = %ld", data_field->data_count, beans.size());
//    }
    FieldNativeType* data_ptr = (FieldNativeType*)data_field->data;
    for (int i=0; i<data_field->data_count; i++) {
      if constexpr (internal::NativeFieldTypeHelper<FieldType>::is_scalar_type) {
        // Is scalar type
        // LANCEDB_LOGD("Is scalar type");
        AdapterType::template FieldValue<I>(&beans[i]) = data_ptr[i];
      } else if constexpr (internal::NativeFieldTypeHelper<FieldType>::is_memory_continuous) {
        // Is vector type, but memory continuous
        // LANCEDB_LOGD("Is vector type, but memory continuous, dimension: %d", data_field->dimension);
        auto& vec = AdapterType::template FieldValue<I>(&beans[i]);
        vec.resize(data_field->dimension);
        void* dst = vec.data();
        void* src = data_ptr + (i * data_field->dimension);
        size_t copy_sz = data_field->dimension * sizeof(FieldNativeType);
        // LANCEDB_LOGD("    Copy size: %zd", copy_sz);
        memcpy(dst, src, copy_sz);
      } else {
        // Is neither scalar type nor memory continuous
        // LANCEDB_LOGD("Is neither scalar type nor memory continuous");
        if constexpr (std::is_same_v<BinaryData, FieldType>) {
          BinaryData& bin = AdapterType::template FieldValue<I>(&beans[i]);
          if (data_field->binary_size) {
            LANCEDB_LOGD("warning: binary_size == nullptr, ignore this field");
            return kLanceDBInvalidData;
          }
          size_t bin_size = data_field->binary_size[i];
          // LANCEDB_LOGD("   binary size: %zd", bin_size);
          bin.data.resize(bin_size);
          void* dst = bin.data.data();
          void* src = data_ptr[i];
          memcpy(dst, src, bin_size);
        }
        else {
          static_assert(std::is_same_v<FieldType, void>, "invalid field type, field type is unsupported");
        }
      }
    }

    return kLanceDBSuccess;
  }

  template<size_t ...I>
  LanceDBError QueryInternal(BeanList& beans, LanceDB::SearchResults& results, std::index_sequence<I...>) {
    std::vector<const char*> field_names = { AdapterType::template FieldName<I>()... };

    (FillBeanField<I>(beans, field_names[I], results), ...);
    return kLanceDBSuccess;
  }

  constexpr static const int kNumFields = AdapterType::N;

  LanceDB* lancedb_conn_;
  bool create_table_ = false;
  bool create_data_ = true;
};

}

#endif // LANCEDB_INCLUDE_LANCEDB_SCHEMA_HPP_
