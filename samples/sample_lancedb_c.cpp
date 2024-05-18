#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <vector>
#include <sys/time.h>
#include <cmath>
#include <cstdint>
#include <cstring>

#include "lancedb.h"

typedef enum LogLevel {
  kLogDebug = 0,
  kLogInfo = 1,
  kLogError = 2,
} LogLevel;

LogLevel log_level = kLogInfo;

extern "C" __attribute__((visibility("default"))) LogLevel GetLogLevel() { return log_level; }

void SetLogLevel(LogLevel level) { log_level = level; }

#define LANCEDB_TAG "lancedb"
#define LANCEDB_TAG_LOGE "lancedb error"
#define LANCEDB_TAG_LOGI "lancedb info "
#define LANCEDB_TAG_LOGD "lancedb debug"

#define LANCEDB_LOGE(fmt, ...)                                      \
  if (GetLogLevel() <= kLogError) {                               \
    fprintf(stderr, LANCEDB_TAG_LOGE ": " fmt "\n", ##__VA_ARGS__); \
  }
#define LANCEDB_LOGI(fmt, ...)                                      \
  if (GetLogLevel() <= kLogInfo) {                                \
    fprintf(stdout, LANCEDB_TAG_LOGI ": " fmt "\n", ##__VA_ARGS__); \
  }
#define LANCEDB_LOGD(fmt, ...)                                      \
  if (GetLogLevel() <= kLogDebug) {                               \
    fprintf(stdout, LANCEDB_TAG_LOGD ": " fmt "\n", ##__VA_ARGS__); \
  }

#define LANCEDB_TIME_S(tag) double time_##tag##_start = TimeMS();
#define LANCEDB_TIME_E(tag)             \
  double time_##tag##_end = TimeMS(); \
  LANCEDB_LOGD(#tag " time: %f ms", time_##tag##_end - time_##tag##_start);
#define LANCEDB_TIME_ES(tag, str)       \
  double time_##tag##_end = TimeMS(); \
  LANCEDB_LOGD(#tag "_%s time: %f ms", str, time_##tag##_end - time_##tag##_start);

static double TimeMS() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

namespace {
void Normalize(float *vec, size_t n) {
  float norm = 0.0;
  float sum = 0;
  for (int i = 0; i < n; i++) {
    norm += vec[i] * vec[i];
    sum += std::abs(vec[i]);
  }
  norm = std::sqrt(norm);
  // printf("  norm: %f   sum: %f   dim=%d\n", norm, sum, n);
  if (norm != 0.0) {
    for (int i = 0; i < n; i++) {
      vec[i] /= norm;
    }
  }
}

float Sum(float *vec, size_t n) {
  float sum = 0;
  for (int i = 0; i < n; i++) {
    sum += std::abs(vec[i]);
  }
  return sum;
}
}

int test_create_table_with_schema() {
  lancedb_handle_t handle = lancedb_init("test_schema.db");

  // Define the fields
  lancedb_table_field_t id_field = {
      "id",
      kLanceDBFieldTypeInt32,
      kLanceDBFieldTypeScalar,
      0,
      0,
      0
  };

  lancedb_table_field_t name_field = {
      "name",
      kLanceDBFieldTypeInt32,
      kLanceDBFieldTypeScalar,
      0,
      0,
      0
  };

  lancedb_table_field_t vector_field = {
      "vector",
      kLanceDBFieldTypeFloat32,
      kLanceDBFieldTypeVector,
      0,
      512,
      0
  };

  lancedb_table_field_t time_field = {
      "time",
      kLanceDBFieldTypeTimestamp,
      kLanceDBFieldTypeScalar,
      0,
      0,
      0
  };

  lancedb_table_field_t comment_field = {
      "comment",
      kLanceDBFieldTypeString,
      kLanceDBFieldTypeScalar,
      0,
      0,
      0
  };

  lancedb_table_field_t binary_field = {
      "binary",
      kLanceDBFieldTypeBlob,
      kLanceDBFieldTypeScalar,
      0,
      0,
      0
  };

  // Create an array of fields
  lancedb_table_field_t fields[] = {id_field, name_field, vector_field,
                                    time_field, comment_field, binary_field};

  // Define the schema
  lancedb_schema_t schema = {
      fields,
      sizeof(fields) / sizeof(lancedb_table_field_t)
  };

  // Create the table
  bool result = lancedb_create_table_with_schema(handle, "test_table", &schema);

  // Check the result
  if (result) {
    printf("Table created successfully\n");
  } else {
    printf("Failed to create table\n");
  }

  lancedb_close(handle);

  return 0;
}

int test_insert_data() {
  lancedb_handle_t handle = lancedb_init("test_schema.db");

  int seq_data[30];
  int64_t tm_data[30];
  int64_t tm_curr = time(nullptr) * 1000;
  for (int i = 0; i < 30; i++) {
    seq_data[i] = i;
    tm_data[i] = tm_curr + i * 1000 * 1000;
  }

  // Define the fields
  lancedb_field_data_t id_field = {
      nullptr,
      kLanceDBFieldTypeInt32,
      kLanceDBFieldTypeScalar,
      30,
      1,
      seq_data
  };

  lancedb_field_data_t name_field = {
      nullptr,
      kLanceDBFieldTypeInt32,
      kLanceDBFieldTypeScalar,
      30,
      1,
      seq_data
  };


  // Generate random 512-dim vectors
  srand(static_cast<unsigned>(time(0)));
  float *vectors = new float[30 * 512];
  for (int i = 0; i < 30 * 512; ++i) {
    vectors[i] = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
  }

  // fill the 15th row to all 1
  for (int i = 0; i < 512; i++) {
    vectors[17 * 512 + i] = 1.0;
  }

  // normalize for each row
  for (int i = 0; i < 30; i++) {
    Normalize(vectors + i * 512, 512);
  }

  lancedb_field_data_t vector_field = {
      nullptr,
      kLanceDBFieldTypeFloat32,
      kLanceDBFieldTypeVector,
      30,
      512,
      vectors
  };

  lancedb_field_data_t time_field = {
      nullptr,
      kLanceDBFieldTypeTimestamp,
      kLanceDBFieldTypeScalar,
      30,
      1,
      tm_data
  };

  const char *comment_data[30];
  for (int i = 0; i < 30; i++) {
    comment_data[i] = "Hello world";
  }
  lancedb_field_data_t comment_field = {
      nullptr,
      kLanceDBFieldTypeString,
      kLanceDBFieldTypeScalar,
      30,
      1,
      comment_data
  };

  const char *blob_test = "\012\0Hello\0World\0H1234";
  const char *blob_data[30];
  size_t blob_data_sz[30];
  for (int i = 0; i < 30; i++) {
    blob_data[i] = blob_test;
    blob_data_sz[i] = 12;
  }

  lancedb_field_data_t binary_field = {
      nullptr,
      kLanceDBFieldTypeBlob,
      kLanceDBFieldTypeScalar,
      30,
      1,
      blob_data,
      blob_data_sz
  };

  // Create an array of fields
  lancedb_field_data_t fields[] = {id_field, name_field, vector_field,
                                   time_field, comment_field, binary_field};

  // Define the data
  lancedb_data_t data = {
      fields,
      sizeof(fields) / sizeof(lancedb_field_data_t)
  };

  LANCEDB_TIME_S(insert)
  // Insert the data
  bool result = lancedb_insert(handle, "test_table", &data);
  LANCEDB_TIME_E(insert)

  // Check the result
  if (result) {
    printf("Data inserted successfully\n");
  } else {
    printf("Failed to insert data\n");
  }

  lancedb_close(handle);

  delete[] vectors;

  return 0;
}

void PrintFieldData(lancedb_field_data_t &field, int i, int j) {
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

void test_search_from_created() {
  // Initialize the database
  lancedb_handle_t handle = lancedb_init("test_schema.db");

  // Define the data to search for
  int dimension = 512; // replace with your actual dimension
  float *data = new float[dimension];
  // fill data with the values you want to search for
  for (int i = 0; i < dimension; i++) {
    data[i] = 1.0;
  }
  Normalize(data, dimension);

  // Perform the search
  LANCEDB_TIME_S(query);
  lancedb_data_t result_data;
  bool result = lancedb_search(handle, "test_table", "vector", data, dimension, &result_data);
  LANCEDB_TIME_E(query);

  // Check the result
  if (result) {
    LANCEDB_LOGD("Search completed successfully");

    LANCEDB_LOGD("Results:\n");
    LANCEDB_LOGD("   num_fields: %zd", result_data.num_fields);
    LANCEDB_LOGD("   field_info:  %p", result_data.fields);
    for (int i=0; i<result_data.num_fields; i++) {
      LANCEDB_LOGD("Field: %s    Type: %d", result_data.fields[i].name, result_data.fields[i].data_type);
      auto& field = result_data.fields[i];
      LANCEDB_LOGD("   data_count: %zd", result_data.fields[i].data_count);
      LANCEDB_LOGD("   dimension:  %zd", result_data.fields[i].dimension);
      LANCEDB_LOGD("   data:       %p", result_data.fields[i].data);
      LANCEDB_LOGD("   binary_size: %p", result_data.fields[i].binary_size);
      LANCEDB_LOGD("   field_type: %d", result_data.fields[i].field_type);

      if (field.data == nullptr) {
        LANCEDB_LOGD(" No data present");
        continue;
      }
      //continue;

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
          }
          printf("\n");
        }
      }
    }

    lancedb_free_search_results(&result_data);

  } else {
    printf("Failed to perform search\n");
  }

  // Clean up
  delete[] data;
  lancedb_close(handle);
}

int test_search_vector() {
  //srand(time(NULL));
  srand(12345);
  void *ptr = lancedb_init("test.db");
  int dim = 768;
  int nz = 100000;
  std::vector<float> data(dim * nz);

  FILE *fp = fopen("test_data.bin", "rb");
  if (fp) {
    LANCEDB_LOGD("Reading data from test_data.bin");
    fread(data.data(), sizeof(float), dim * nz, fp);
    fclose(fp);
  } else {
    // fill with random data
    for (int i = 0; i < dim * nz; i++) {
      data[i] = (float) rand() / RAND_MAX;
    }
    // normalize for each vector
    for (int i = 0; i < nz; i++) {
      Normalize(data.data() + dim * i, dim);
    }
  }

  LANCEDB_LOGD("dim: %d, nz: %d", dim, nz);

  LANCEDB_TIME_S(create_table);
  lancedb_data_t result_data;
  lancedb_create_table(ptr, "test_table", data.data(), dim, nz);
  LANCEDB_TIME_E(create_table);

  LANCEDB_TIME_S(query);
  bool result = lancedb_search(ptr, "test_table", "vector", data.data() + dim * 1033, dim, &result_data);
  LANCEDB_TIME_E(query);

  lancedb_close(ptr);
  if (result) {
    lancedb_field_data_t *id_field, *distance_field;
    for (int i=0; i<result_data.num_fields; i++) {
      if (strcmp(result_data.fields[i].name, "id") == 0) {
        id_field = &result_data.fields[i];
      } else if (strcmp(result_data.fields[i].name, "_distance") == 0) {
        distance_field = &result_data.fields[i];
      }
    }
    size_t data_count = id_field->data_count;
    for (int i=0; i<data_count; i++) {
      printf("[%d] index=%d, simi=%f\n",
             i, ((int32_t*)id_field->data)[i],
             1 - ((float*)distance_field->data)[i]);
    }

    lancedb_free_search_results(&result_data);
  }
  return 0;
}

int main() {
  SetLogLevel(kLogDebug);
  system("rm -rf test*.db");

  test_create_table_with_schema();
  test_insert_data();
  test_search_from_created();
  // test_search_vector();
}