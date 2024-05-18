#include <cstdlib>
#include <vector>

#include "gtest/gtest.h"
#include "lancedb.h"

TEST(LanceDB, CAPI) {
  system("rm -rf test.db");
  lancedb_handle_t handle = lancedb_init("test.db");
  int32_t dim = 768;
  int32_t nz = 100;
  int32_t k = 10;
  std::vector<float> data(dim*nz);

  FILE* fp = fopen("test/data/test_data.bin", "rb");
  ASSERT_NE(fp, nullptr);
  printf("Reading data from test_data.bin\n");

  fread(&dim, sizeof(int32_t), 1, fp);
  ASSERT_GT(dim, 0);
  fread(&nz, sizeof(int32_t), 1, fp);
  ASSERT_GT(nz, 0);
  fread(&k, sizeof(int32_t), 1, fp);
  ASSERT_GT(k, 0);
  std::vector<int> target_indexes(k, 0);
  fread(target_indexes.data(), sizeof(int32_t), target_indexes.size(), fp);
  fread(data.data(), sizeof(float), dim*nz, fp);
  fclose(fp);

  printf("dim=%d, nz=%d, k=%d\n", dim, nz, k);

  printf("Create table\n");
  bool res = lancedb_create_table(handle, "test_table", data.data(), dim, nz);
  ASSERT_TRUE(res);

  printf("Search\n");
  lancedb_data_t result_data;
  res = lancedb_search(handle, "test_table", "vector", data.data() + dim * 33, dim, &result_data);
  ASSERT_TRUE(res);

  lancedb_close(handle);

  lancedb_field_data_t *id_field, *distance_field;
  for (int i=0; i<result_data.num_fields; i++) {
    if (strcmp(result_data.fields[i].name, "id") == 0) {
      id_field = &result_data.fields[i];
    } else if (strcmp(result_data.fields[i].name, "_distance") == 0) {
      distance_field = &result_data.fields[i];
    }
  }
  size_t data_count = id_field->data_count;
  ASSERT_EQ(data_count, k);

  for (int i=0; i<data_count; i++) {
    printf("[%d] index=%d, simi=%f\n",
           i, ((int32_t*)id_field->data)[i],
           1 - ((float*)distance_field->data)[i]);
    ASSERT_EQ(((int32_t*)id_field->data)[i], target_indexes[i]);
  }

  lancedb_free_search_results(&result_data);
}