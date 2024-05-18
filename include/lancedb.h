#ifndef LANCEDB_INCLUDE_LANCEDB_H_
#define LANCEDB_INCLUDE_LANCEDB_H_

#include <stdbool.h>
#include <stddef.h>

typedef enum {
  kLanceDBFieldTypeInt8,
  kLanceDBFieldTypeInt16,
  kLanceDBFieldTypeInt32,
  kLanceDBFieldTypeInt64,
  kLanceDBFieldTypeUInt8,
  kLanceDBFieldTypeUInt16,
  kLanceDBFieldTypeUInt32,
  kLanceDBFieldTypeUInt64,
  kLanceDBFieldTypeFloat16,
  kLanceDBFieldTypeFloat32,
  kLanceDBFieldTypeFloat64,
  kLanceDBFieldTypeString,
  kLanceDBFieldTypeBlob,
  kLanceDBFieldTypeTimestamp,
} lancedb_field_data_type_t;

typedef enum {
  kLanceDBFieldTypeScalar,
  kLanceDBFieldTypeVector,
} lancedb_field_type_t;

typedef struct lancedb_table_field_t {
  const char* name;
  lancedb_field_data_type_t data_type;
  lancedb_field_type_t field_type;
  int create_index;
  int dimension;
  int nullable;
} lancedb_table_field_t;

typedef struct lancedb_schema_t {
  lancedb_table_field_t* fields;
  size_t num_fields;
} lancedb_schema_t;

typedef struct lancedb_field_data_t {
  const char* name; // can be nullptr when insert data
  lancedb_field_data_type_t data_type;
  lancedb_field_type_t field_type;
  size_t data_count;
  size_t dimension; // only for vector
  void* data;       // for vector, it is a pointer to a 2D array; for scalar, it is a pointer to a 1D array
  size_t* binary_size; // only for blob type
} lancedb_field_data_t;

typedef struct lancedb_data_t {
  lancedb_field_data_t* fields;
  size_t num_fields;
} lancedb_data_t;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

typedef void* lancedb_handle_t;

lancedb_handle_t lancedb_init(const char* uri);

bool lancedb_close(lancedb_handle_t handle);

bool lancedb_create_table(lancedb_handle_t handle, const char* table_name,
                          float* data, int dimension, int count);

bool lancedb_create_table_with_schema(lancedb_handle_t handle, const char* table_name,
                                      lancedb_schema_t* schema);

bool lancedb_insert(lancedb_handle_t handle, const char* table_name,
                    lancedb_data_t* field_data);

bool lancedb_search(lancedb_handle_t handle, const char* table_name, const char* column_name,
                    void* data, int dimension, lancedb_data_t* search_results);

bool lancedb_free_search_results(lancedb_data_t* search_results);

#ifdef __cplusplus
}
#endif // __cplusplus

#endif // LANCEDB_INCLUDE_LANCEDB_H_