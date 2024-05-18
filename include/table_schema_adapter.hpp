#ifndef LANCEDB_INCLUDE_TABLE_SCHEMA_ADAPTER_HPP_
#define LANCEDB_INCLUDE_TABLE_SCHEMA_ADAPTER_HPP_

#include <typeinfo>
#include <tuple>
#include <vector>
#include <sstream>
#include <cstring>

#include "table_schema.hpp"

namespace lancedb {

#define LANCE_DB_INTERNAL_DEFINE_TABLE_NAME(tbl_name)  \
          static constexpr const char * table_name = #tbl_name ;
#define LANCE_DB_INTERNAL_FOR_BEAM_TYPE(beam_ty) \
          using bean_type = beam_ty;
#define LANCE_DB_INTERNAL_DEFINE_MEMBER_COUNT(count)  \
          const static int N = count;
#define LANCE_DB_INTERNAL_DEFINE_ID() \
          LANCE_DB_INTERNAL_DEFINE_TABLE_FIELD(0, id)
#define LANCE_DB_INTERNAL_DEFINE_TABLE_FIELD(index, name) \
    template <int I>                   \
    static auto& FieldValue(typename std::enable_if<I == (index), bean_type*>::type bean) {\
        return bean->name;\
    }                                                     \
    template <int I>                   \
    static const auto& FieldValue(typename std::enable_if<I == (index), const bean_type*>::type bean) {\
        return bean->name;\
    }                                                             \
    template <int I>                   \
    static typename std::enable_if<I == (index), const char*>::type FieldName() {           \
        return #name ;                 \
    }                                  \
    static_assert(index < N);

#define BEGIN_DEFINE_LANCEDB_SCHEMA_ADAPTER(bean_name, member_count) \
struct bean_name##Adapter {                             \
    LANCE_DB_INTERNAL_DEFINE_MEMBER_COUNT(member_count)        \
    LANCE_DB_INTERNAL_DEFINE_TABLE_NAME(bean_name)             \
    LANCE_DB_INTERNAL_FOR_BEAM_TYPE(bean_name)

#define END_DEFINE_LANCEDB_SCHEMA_ADAPTER(bean_name) \
    static_assert(std::is_same_v<bean_type, bean_name>); \
    static_assert(N > 0);                            \
private:                                             \
    bean_name##Adapter() = default;                  \
};                                                   \
struct bean_name##Result {                          \
  std::vector<bean_name> results;                    \
  std::vector<float> distances;                      \
};\
typedef ::lancedb::TableSchema<bean_name##Adapter> bean_name##Schema;

} // namespace lancedb

#endif // LANCEDB_INCLUDE_TABLE_SCHEMA_ADAPTER_HPP_