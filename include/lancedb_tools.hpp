#ifndef LANCEDB_INCLUDE_LANCEDB_TOOLS_HPP_
#define LANCEDB_INCLUDE_LANCEDB_TOOLS_HPP_

#include <cstdio>
#include <cstdint>

#include "lancedb.h"

#ifndef LANCEDB_TOOL_LOGD
#define LANCEDB_TOOL_LOGD(fmt, ...) printf(fmt "\n", ##__VA_ARGS__)
#endif // LANCEDB_TOOL_LOGD

struct LanceDBTool {
  static void PrintFieldData(lancedb_field_data_t &field, int i, int j) {
    if (field.data_type == kLanceDBFieldTypeInt8) {
      int8_t* data = (int8_t*)field.data;
      printf("%d\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeInt16) {
      int16_t* data = (int16_t*)field.data;
      printf("%d\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeInt32) {
      int32_t* data = (int32_t*)field.data;
      printf("%d\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeInt64) {
      int64_t* data = (int64_t*)field.data;
      printf("%ld\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeUInt8) {
      uint8_t* data = (uint8_t*)field.data;
      printf("%u\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeUInt16) {
      uint16_t* data = (uint16_t*)field.data;
      printf("%u\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeUInt32) {
      uint32_t* data = (uint32_t*)field.data;
      printf("%u\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeUInt64) {
      uint64_t* data = (uint64_t*)field.data;
      printf("%lu\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeFloat16) {
      float* data = (float*)field.data;
      printf("%f\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeFloat32) {
      float* data = (float*)field.data;
      printf("%f\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeFloat64) {
      double* data = (double*)field.data;
      printf("%f\t", data[i * field.dimension + j]);
    }
    else if (field.data_type == kLanceDBFieldTypeString) {
      const char** data = (const char**)field.data;
      printf("%s\t", data[i * field.dimension]);
    }
    else if (field.data_type == kLanceDBFieldTypeBlob) {
      const char** data = (const char**)field.data;
      printf("%s\t", data[i * field.dimension]);
    }
    else if (field.data_type == kLanceDBFieldTypeTimestamp) {
      int64_t* data = (int64_t*)field.data;
      printf("%ld\t", data[i * field.dimension + j]);
    }
  }
  static void PrintResult(const lancedb_data_t &result_data) {
    LANCEDB_TOOL_LOGD("Results:\n");
    LANCEDB_TOOL_LOGD("   num_fields: %zd", result_data.num_fields);
    LANCEDB_TOOL_LOGD("   field_info:  %p", result_data.fields);
    for (int i=0; i<result_data.num_fields; i++) {
      LANCEDB_TOOL_LOGD("Field: %s    Type: %d", result_data.fields[i].name, result_data.fields[i].data_type);
      auto& field = result_data.fields[i];
      LANCEDB_TOOL_LOGD("   data_count: %zd", result_data.fields[i].data_count);
      LANCEDB_TOOL_LOGD("   dimension:  %zd", result_data.fields[i].dimension);
      LANCEDB_TOOL_LOGD("   data:       %p", result_data.fields[i].data);
      LANCEDB_TOOL_LOGD("   binary_size: %p", result_data.fields[i].binary_size);
      LANCEDB_TOOL_LOGD("   field_type: %d", result_data.fields[i].field_type);

      if (field.data == nullptr) {
        LANCEDB_TOOL_LOGD(" No data present");
        continue;
      }
      // continue;

      if (field.field_type == kLanceDBFieldTypeScalar) {
        for (int j=0; j<field.data_count; j++) {
          printf("[%3d] ", j);
          if (field.data_type == kLanceDBFieldTypeString) {
            const char* data = ((const char**)field.data)[j];
            size_t datasize = field.binary_size[j];
            printf("(length: %5zd) ", datasize);
            for (int k=0; k<datasize; k++) {
              printf("%c", data[k]);
              if (k > 100) {
                printf("...");
                break;
              }
            }
          }
          else if (field.data_type == kLanceDBFieldTypeBlob) {
            const char* data = ((const char**)field.data)[j];
            size_t datasize = field.binary_size[j];
            printf("(length: %5zd) ", datasize);
            for (int k=0; k<datasize; k++) {
              uint8_t d = data[k] & 0xff;
              printf("%s%x ", d < 0x10 ? "0":"",  d);
              if (k > 100) {
                printf("...");
                break;
              }
            }
          }
          else {
            PrintFieldData(field, j, 0);
          }
          printf("\n");
        }
      }
      else {
        for (int j=0; j<field.data_count; j++) {
          printf("[%-3d] ", j);
          for (int k=0; k<field.dimension; k++) {
            PrintFieldData(field, j, k);
            if (k > 100) {
              printf("...");
              break;
            }
          }
          printf("\n");
        }
      }
    }
  }
};

#undef LANCEDB_TOOL_LOGD

#endif