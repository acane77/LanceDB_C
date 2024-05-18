#include "lancedb.h"

#include <cstdlib>

extern "C"
bool lancedb_free_search_results(lancedb_data_t* search_results) {
  if (search_results == nullptr) {
    return false;
  }

  for (size_t i = 0; i < search_results->num_fields; i++) {
    lancedb_field_data_t* field_data = &search_results->fields[i];

    if (field_data->data_type == kLanceDBFieldTypeString ||
        field_data->data_type == kLanceDBFieldTypeBlob) {
      char** data = (char**)field_data->data;
      for (int j = 0; j < field_data->data_count; j++) {
        free(data[j]);
      }
    }

    if (field_data->data != nullptr) {
      free(field_data->data);
      field_data->data = nullptr;
    }
    if (field_data->binary_size != nullptr) {
      free(field_data->binary_size);
      field_data->binary_size = nullptr;
    }
    if (field_data->name != nullptr) {
      free((char*)field_data->name);
    }
  }

  if (search_results->fields != nullptr) {
    free(search_results->fields);
    search_results->fields = nullptr;
  }
  return true;
}
