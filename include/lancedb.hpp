#ifndef LANCEDB_INCLUDE_LANCEDB_HPP_
#define LANCEDB_INCLUDE_LANCEDB_HPP_

#include <string>
#include <vector>
#include <type_traits>
#include <tuple>

#include "lancedb.h"

namespace lancedb {

enum LanceDBError {
  kLanceDBSuccess              = 0,
  kLanceDBNotConnected         = 1,
  kLanceDBInvalidArgument      = 2,
  kLanceDBInvalidOperation     = 3,
  kLanceDBInternalError        = 4,
  kLanceDBUnsupportedDataType  = 5,
  kLanceDBFieldNotFound        = 6,
  kLanceDBInsertFailed         = 7,
  kLanceDBInvalidData          = 8,
};

struct BinaryData {
  std::vector<uint8_t> data;
};

template <typename T>
struct InnerValueType {
  using type = T;
  using real_type = type;
};

template <>
struct InnerValueType<std::string> {
  using type = const char*;
  using real_type = std::string;
};

template <>
struct InnerValueType<BinaryData> {
  using type = const uint8_t*;
  using real_type = BinaryData;
};

template <template<typename...> class Container, typename... Ts>
struct InnerValueType<Container<Ts...>> {
  using type = typename InnerValueType<typename std::tuple_element<0, std::tuple<Ts...>>::type>::type;
  using real_type = type;
};

class LanceDBTypes {
  typedef int8_t      Int8;
  typedef int16_t     Int16;
  typedef int32_t     Int32;
  typedef int64_t     Int64;
  typedef uint8_t     UInt8;
  typedef uint16_t    UInt16;
  typedef uint32_t    UInt32;
  typedef uint64_t    UInt64;
  typedef float       Float32;
  typedef double      Float64;
  typedef std::string String;
  typedef BinaryData  Blob;
  typedef uint64_t    Timestamp; // data type for timestamp type
};

class LanceDB {
public:
  explicit LanceDB(const char* uri) {
    hnd_ = lancedb_init(uri);
    is_inited_ = hnd_ != nullptr;
  }

  bool IsInited() const { return is_inited_; }

  ~LanceDB() {
    if (hnd_ == nullptr) {
      return;
    }
    lancedb_close(hnd_);
    is_inited_ = false;
  }

  typedef lancedb_field_data_type_t DataType;
  typedef lancedb_field_type_t      FieldType;

  struct Field {
    std::string  name;
    DataType     data_type;
    FieldType    field_type    = kLanceDBFieldTypeScalar;
    bool         create_index  = false; // unused
    int          dimension     = 1;
    bool         nullable      = false;
  };

  template <class T> using List = std::vector<T>;
  template <class T> using VectorList = List<List<T>>;

  struct Schema {
    List<Field> fields;
  };

  template<class T>
  struct IsStringType {
    static constexpr bool value = std::is_same_v<T, std::string> ||
                                  std::is_same_v<T, const char*> ||
                                  std::is_same_v<T, char*>;
  };

  template <class T>
  struct IsScalarType {
    static constexpr bool value = std::is_scalar_v<T> || IsStringType<T>::value
        || std::is_same_v<T, BinaryData>;
  };

  template <class T, template <class U> class Container>
  class BaseFieldData {
  public:
    int GetDimension() const { return field_info.dimension; }
    const std::string& GetName() const { return field_info.name; }
    const Container<T>& GetData() const { return data; }
    Container<T>& GetData() { return data; }
    FieldType GetFieldType() const { return field_info.field_type; }
    DataType GetDataType() const { return field_info.data_type; }
    bool IsDataValid() const { return data_valid; }
    const Field& GetFieldInfo() const { return field_info; }
    size_t GetDataCount() const { return data.size(); }

  protected:
    template<class U>
    DataType GetDataTypeByNativeType() const {
      if constexpr (std::is_same_v<U, int8_t> || std::is_same_v<U, char> || std::is_same_v<U, unsigned char>) {
        return kLanceDBFieldTypeInt8;
      } else if constexpr (std::is_same_v<U, int16_t>) {
        return kLanceDBFieldTypeInt16;
      } else if constexpr (std::is_same_v<U, int32_t>) {
        return kLanceDBFieldTypeInt32;
      } else if constexpr (std::is_same_v<U, int64_t>) {
        return kLanceDBFieldTypeInt64;
      } else if constexpr (std::is_same_v<U, uint8_t>) {
        return kLanceDBFieldTypeUInt8;
      } else if constexpr (std::is_same_v<U, uint16_t>) {
        return kLanceDBFieldTypeUInt16;
      } else if constexpr (std::is_same_v<U, uint32_t>) {
        return kLanceDBFieldTypeUInt32;
      } else if constexpr (std::is_same_v<U, uint64_t>) {
        return kLanceDBFieldTypeUInt64;
      } else if constexpr (std::is_same_v<U, float>) {
        return kLanceDBFieldTypeFloat32;
      } else if constexpr (std::is_same_v<U, double>) {
        return kLanceDBFieldTypeFloat64;
      } else if constexpr (std::is_same_v<U, std::string> ||
          std::is_same_v<U, const char*> || std::is_same_v<U, char*>) {
        return kLanceDBFieldTypeString;
      } else if constexpr (std::is_same_v<U, BinaryData>) {
        return kLanceDBFieldTypeBlob;
      }
      else {
        static_assert(std::is_same_v<U, void>, "Unsupported data type");
      }
    }

  protected:
    Container<T> data;
    Field field_info;
    bool data_valid = false;
  };

  template <class T, template<class X> class Container>
  class FieldData : public BaseFieldData<T, Container> {
  public:
    FieldData(const std::string& name, const Container<T>& data, bool nullable = false,
              bool create_index = false) {
      BaseFieldData<T, Container>::field_info.name = name;
      BaseFieldData<T, Container>::data = data;
      BaseFieldData<T, Container>::field_info.nullable = nullable;
      BaseFieldData<T, Container>::field_info.create_index = create_index;
      SetFieldType();
    }

    template <class U = T>
    std::enable_if_t<IsScalarType<typename U::value_type>::value && !std::is_same_v<U, std::string>>
    SetFieldType() {
      BaseFieldData<T, Container>::field_info.field_type = kLanceDBFieldTypeVector;
      BaseFieldData<T, Container>::field_info.data_type =
          BaseFieldData<T, Container>::template GetDataTypeByNativeType<typename U::value_type>();
      // check data valid
      if (BaseFieldData<T, Container>::data.empty()) {
        BaseFieldData<T, Container>::data_valid = false;
        return;
      }
      // ensure all data has same dimension
      for (auto& v: BaseFieldData<T, Container>::data) {
        if (v.size() != BaseFieldData<T, Container>::data[0].size()) {
          BaseFieldData<T, Container>::data_valid = false;
          return;
        }
      }
      BaseFieldData<T, Container>::field_info.dimension = BaseFieldData<T, Container>::data[0].size();
      BaseFieldData<T, Container>::data_valid = true;
      for (auto& v: BaseFieldData<U, Container>::data) {
        flatten_data.insert(flatten_data.end(), v.begin(), v.end());
      }
    }

    template<class U = T>
    std::enable_if_t<!IsScalarType<typename U::value_type>::value>
    SetFieldType() {
      static_assert(std::is_same_v<U, void>, "Unsupported nested type, only support two-level");
    }

    template <class U = T>
    std::enable_if_t<IsScalarType<U>::value>
    SetFieldType() {
      BaseFieldData<T, Container>::field_info.field_type = kLanceDBFieldTypeScalar;
      BaseFieldData<T, Container>::field_info.data_type =
          BaseFieldData<T, Container>::template GetDataTypeByNativeType<T>();
      BaseFieldData<T, Container>::field_info.dimension = 1;
      BaseFieldData<T, Container>::data_valid = BaseFieldData<T, Container>::data.size() > 0;
      SetFlattenData();
    }

    typedef typename InnerValueType<T>::type InnerType;
    static_assert(IsScalarType<InnerType>::value, "must be scalar type");

    typedef typename InnerValueType<T>::real_type RealInnerType;

    template <class U = RealInnerType>
    std::enable_if_t<!std::is_same_v<U, std::string> && !std::is_same_v<U, BinaryData>>
    SetFlattenData() {
      flatten_data = this->data;
    }

    template <class U = RealInnerType>
    std::enable_if_t<std::is_same_v<U, std::string>>
    SetFlattenData() {
      for (std::string& str: BaseFieldData<T, Container>::data) {
        flatten_data.push_back(str.c_str());
      }
    }

    template <class U = RealInnerType>
    std::enable_if_t<std::is_same_v<U, BinaryData>>
    SetFlattenData() {
      for (BinaryData& bin: BaseFieldData<T, Container>::data) {
        flatten_data.push_back(bin.data.data());
        data_size_holder.push_back(bin.data.size());
      }
    }

    const std::vector<InnerType>& GetFlattenData() const {
      return flatten_data;
    }

    const std::vector<size_t>& GetBinaryDataSize() const {
      return data_size_holder;
    }

  private:
    std::vector<InnerType> flatten_data;
    std::vector<size_t> data_size_holder; // only for Binary type for now
  };

  template <class T>
  class FlatFieldData : public BaseFieldData<T, List> {
  public:
    FlatFieldData(const std::string& name, const List<T>& data,
                  FieldType field_type, int dimension = 1, bool nullable = false,
                  bool create_index = false) {
      BaseFieldData<T, List>::field_info.name = name;
      BaseFieldData<T, List>::data = data;
      BaseFieldData<T, List>::field_info.field_type = field_type;
      BaseFieldData<T, List>::field_info.dimension = dimension;
      BaseFieldData<T, List>::field_info.data_type =
          BaseFieldData<T, List>::template GetDataTypeByNativeType<T>();
      BaseFieldData<T, List>::field_info.create_index = create_index;
      BaseFieldData<T, List>::field_info.nullable = nullable;
      BaseFieldData<T, List>::data_valid = CheckDataValid();
    }

    typedef typename InnerValueType<T>::type InnerType;
    static_assert(IsScalarType<InnerType>::value, "must be scalar type");

    typedef typename InnerValueType<T>::real_type RealInnerType;

    template <class U = RealInnerType>
    const std::enable_if_t<!std::is_same_v<U, std::string> && !std::is_same_v<U, BinaryData>, List<T>> &
    GetFlattenData() const {
      return BaseFieldData<T, List>::data;
    }

    template <class U = RealInnerType>
    const std::enable_if_t<std::is_same_v<U, BinaryData>, List<InnerType>> &
    GetFlattenData() const {
      if (!data_holder.empty()) {
        return data_holder;
      }
      for (auto& bin: this->data) {
        const_cast<FlatFieldData<T>*>(this)->data_holder.push_back(bin.data.data());
        const_cast<FlatFieldData<T>*>(this)->data_size_holder.push_back(bin.data.size());
      }
      return data_holder;
    }

    template <class U = RealInnerType>
    const std::enable_if_t<std::is_same_v<U, std::string>, List<InnerType>> &
    GetFlattenData() const {
      if (!data_holder.empty()) {
        return data_holder;
      }
      for (auto& str: this->data) {
        const_cast<FlatFieldData<T>*>(this)->data_holder.push_back(str.c_str());
      }
      return data_holder;
    }

    const std::vector<size_t>& GetBinaryDataSize() const {
      return data_size_holder;
    }

  private:
    bool CheckDataValid() {
      int dim = BaseFieldData<T, List>::field_info.dimension;
      if (dim <= 0) {
        return false;
      }
      if (BaseFieldData<T, List>::field_info.field_type == kLanceDBFieldTypeScalar) {
        // check data valid
        if (BaseFieldData<T, List>::data.empty()) {
          return false;
        }
      } else {
        // check data valid
        if (BaseFieldData<T, List>::data.empty()) {
          return false;
        }
        // ensure all data has same dimension
        size_t sz = BaseFieldData<T, List>::data.size();
        if (sz / dim * dim != sz) {
          return false;
        }
      }
      return true;
    }

    std::vector<InnerType> data_holder;
    std::vector<size_t> data_size_holder; // only for Binary type for now
  };

  typedef lancedb_field_data_t CFieldData;

  template <class FieldDataType>
  static CFieldData GetCFieldData(const FieldDataType& fd) {
    CFieldData cfd;
    const Field& field_info = fd.GetFieldInfo();
    cfd.name = field_info.name.c_str();
    cfd.data_type = field_info.data_type;
    cfd.field_type = field_info.field_type;
    cfd.data_count = fd.GetDataCount();
    cfd.dimension = field_info.dimension;
    if constexpr (std::is_same_v<typename FieldDataType::RealInnerType, BinaryData>) {
      cfd.data = const_cast<void*>(reinterpret_cast<const void*>(fd.GetFlattenData().data()));
      cfd.binary_size = const_cast<size_t*>(const_cast<FieldDataType*>(&fd)->GetBinaryDataSize().data());
    } else {
      cfd.data = const_cast<void*>(reinterpret_cast<const void*>(fd.GetFlattenData().data()));
      cfd.binary_size = nullptr;
    }
    return cfd;
  }

  typedef lancedb_table_field_t CField;
  static CField GetCField(const Field& field) {
    CField cf;
    cf.name = field.name.c_str();
    cf.data_type = field.data_type;
    cf.field_type = field.field_type;
    cf.create_index = field.create_index;
    cf.dimension = field.dimension;
    cf.nullable = field.nullable;
    return cf;
  }

  template <class... FieldDataTypes>
  class BatchInserter {
  private:
    explicit BatchInserter(lancedb_handle_t hnd, FieldDataTypes&&... field_data) {
      fields_ = { GetCField((std::forward<FieldDataTypes>(field_data)).GetFieldInfo()) ... };
      cfd_ = { GetCFieldData(std::forward<FieldDataTypes>(field_data)) ... };
      hnd_ = hnd;
      std::vector<int> valid = { std::forward<FieldDataTypes>(field_data).IsDataValid() ... };
      for (auto v: valid) {
        if (v == 0) {
          is_valid_ = false;
          return;
        }
      }
      is_valid_ = true;
    }
  public:
    LanceDBError CreateTable(const std::string& table_name) {
      if (hnd_ == nullptr) {
        return kLanceDBNotConnected;
      }
      if (!is_valid_) {
        return kLanceDBInvalidData;
      }
      lancedb_schema_t schema;
      schema.fields = fields_.data();
      schema.num_fields = fields_.size();
      bool result = lancedb_create_table_with_schema(hnd_, table_name.c_str(), &schema);
      return result ? kLanceDBSuccess : kLanceDBInternalError;
    }

    LanceDBError Insert(const std::string& table_name) {
      if (hnd_ == nullptr) {
        return kLanceDBNotConnected;
      }
      if (!is_valid_) {
        return kLanceDBInvalidData;
      }
      lancedb_data_t ld;
      ld.fields = cfd_.data();
      ld.num_fields = cfd_.size();
      //LanceDBTool::PrintResult(ld);
      bool result = lancedb_insert(hnd_, table_name.c_str(), &ld);
      return result ? kLanceDBSuccess : kLanceDBInsertFailed;
    }

  private:
    List<CField> fields_;
    List<CFieldData> cfd_;
    lancedb_handle_t hnd_;
    bool is_valid_ = false;

    friend class LanceDB;
  };

  template <class... FieldDataTypes>
  BatchInserter<FieldDataTypes...> CreateBatchInserter(FieldDataTypes&&... field_data) {
    return BatchInserter<FieldDataTypes...>(hnd_, std::forward<FieldDataTypes>(field_data)...);
  }

  struct SearchResults {
  public:
    SearchResults() = default;
    ~SearchResults() {
      if (!is_valid_) {
        return;
      }
      lancedb_free_search_results(&data_);
    }

    const lancedb_data_t& Get() const { return data_; }
    bool IsValid() const { return is_valid_; }
  private:

    lancedb_data_t data_;
    bool is_valid_ = false;

    friend class LanceDB;
  };

  template <class T>
  std::enable_if_t<std::is_same_v<T, float> || std::is_same_v<T, double>, LanceDBError>
  Query(const std::string& table_name, const std::string& column_name, const std::vector<T>& embeddings,
        SearchResults& sr) {
    if (hnd_ == nullptr) {
      return kLanceDBNotConnected;
    }
    if (embeddings.empty()) {
      return kLanceDBInvalidData;
    }
    lancedb_data_t& result_data = sr.data_;
    bool result = lancedb_search(hnd_, table_name.c_str(), column_name.c_str(),
                                 (void*)embeddings.data(), embeddings.size(), &result_data);
    sr.is_valid_ = result;
    return result ? kLanceDBSuccess : kLanceDBInternalError;
  }
private:
  bool is_inited_ = false;
  lancedb_handle_t hnd_;
};

} // namespace lancedb

#endif // LANCEDB_INCLUDE_LANCEDB_HPP_