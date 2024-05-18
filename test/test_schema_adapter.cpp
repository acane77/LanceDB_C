#include "lancedb.hpp"
#include "table_schema_adapter.hpp"
#include "gtest/gtest.h"
#include "table_schema.hpp"
#include "lancedb_tools.hpp"

#include <cstdlib>
#include <cmath>

using namespace lancedb;

TEST(LanceDB, FieldData) {
  std::vector<std::vector<float>> float_data = {{1.0f, 2.0f, 3.0f}, {4.0f, 5.0f, 6.0f}};
  LanceDB::FieldData field_data("test", float_data);

  ASSERT_EQ(field_data.GetFieldType(), kLanceDBFieldTypeVector);
  ASSERT_EQ(field_data.GetDataType(), kLanceDBFieldTypeFloat32);
  ASSERT_EQ(field_data.GetDimension(), float_data[0].size());
  ASSERT_TRUE(field_data.IsDataValid());
  ASSERT_EQ(field_data.GetFlattenData().size(), 6);
  auto& flatdata1 = field_data.GetFlattenData();
  printf("flat data: ");
  for (float i : flatdata1) {
    printf("%f   ", i);
  }
  printf("\n");

  std::vector<int32_t> int_data = {1, 2, 3};
  LanceDB::FieldData field_data2("test2", int_data);
  ASSERT_EQ(field_data2.GetFieldType(), kLanceDBFieldTypeScalar);
  ASSERT_EQ(field_data2.GetDataType(), kLanceDBFieldTypeInt32);
  ASSERT_EQ(field_data2.GetDimension(), 1);
  ASSERT_TRUE(field_data2.IsDataValid());

  float_data.push_back({ 3.0 });
  LanceDB::FieldData field_data3("test3", float_data);
  ASSERT_TRUE(!field_data3.IsDataValid());


  LanceDB::FlatFieldData field_data4("test4", std::vector<float>(128, 0.f),
                                            kLanceDBFieldTypeScalar);
  ASSERT_EQ(field_data4.GetFieldType(), kLanceDBFieldTypeScalar);
  ASSERT_EQ(field_data4.GetDataType(), kLanceDBFieldTypeFloat32);
  ASSERT_EQ(field_data4.GetDimension(), 1);
  ASSERT_TRUE(field_data4.IsDataValid());

  LanceDB::FlatFieldData field_data5("test5", std::vector<float>(128, 0.f),
                                            kLanceDBFieldTypeVector, 16);
  ASSERT_EQ(field_data5.GetFieldType(), kLanceDBFieldTypeVector);
  ASSERT_EQ(field_data5.GetDataType(), kLanceDBFieldTypeFloat32);
  ASSERT_EQ(field_data5.GetDimension(), 16);
  ASSERT_TRUE(field_data5.IsDataValid());

  LanceDB::FlatFieldData field_data6("test5", std::vector<int16_t>(128, 0),
                                            kLanceDBFieldTypeVector, 19);
  ASSERT_EQ(field_data6.GetFieldType(), kLanceDBFieldTypeVector);
  ASSERT_EQ(field_data6.GetDataType(), kLanceDBFieldTypeInt16);
  ASSERT_EQ(field_data6.GetDimension(), 19);
  ASSERT_TRUE(!field_data6.IsDataValid());

  LanceDB::FieldData field_data7("test7",
                                        std::vector<std::string>{ "hello", "kitty", "!" });
  ASSERT_TRUE(field_data7.IsDataValid());
  ASSERT_EQ(field_data7.GetFieldType(), kLanceDBFieldTypeScalar);
  ASSERT_EQ(field_data7.GetDataType(), kLanceDBFieldTypeString);
  ASSERT_EQ(field_data7.GetDimension(), 1);
  ASSERT_TRUE(field_data7.IsDataValid());
  auto& flatdata3 = field_data7.GetFlattenData();
  printf("flat data: ");
  for (auto i : flatdata3) {
    printf("%s   ", i);
  }
  printf("\n");

  LanceDB::FieldData field_data8("test7",
                                        std::vector<const char*>{ "hello", "world" });
  ASSERT_TRUE(field_data8.IsDataValid());
  ASSERT_EQ(field_data8.GetFieldType(), kLanceDBFieldTypeScalar);
  ASSERT_EQ(field_data8.GetDataType(), kLanceDBFieldTypeString);
  ASSERT_EQ(field_data8.GetDimension(), 1);
  ASSERT_TRUE(field_data8.IsDataValid());
  auto& flatdata2 = field_data8.GetFlattenData();
  printf("flat data: ");
  for (auto i : flatdata2) {
    printf("%s   ", i);
  }
  printf("\n");

  LanceDB::FlatFieldData field_data9("test7",
                                        std::vector<const char*>{ "hello", "world" }, kLanceDBFieldTypeScalar);
  ASSERT_TRUE(field_data9.IsDataValid());
  ASSERT_EQ(field_data9.GetFieldType(), kLanceDBFieldTypeScalar);
  ASSERT_EQ(field_data9.GetDataType(), kLanceDBFieldTypeString);
  ASSERT_EQ(field_data9.GetDimension(), 1);
  ASSERT_TRUE(field_data9.IsDataValid());
  auto& flatdata9 = field_data9.GetFlattenData();
  printf("flat data: ");
  for (auto i : flatdata9) {
    printf("%s   ", i);
  }
  printf("\n");

  {
    LanceDB::FlatFieldData field_data9("test7",
                                              std::vector<std::string>{ "hello", "world" }, kLanceDBFieldTypeScalar);
    ASSERT_TRUE(field_data9.IsDataValid());
    ASSERT_EQ(field_data9.GetFieldType(), kLanceDBFieldTypeScalar);
    ASSERT_EQ(field_data9.GetDataType(), kLanceDBFieldTypeString);
    ASSERT_EQ(field_data9.GetDimension(), 1);
    ASSERT_TRUE(field_data9.IsDataValid());
    auto& flatdata9 = field_data9.GetFlattenData();
    printf("flat data: ");
    for (auto& i : flatdata9) {
      printf("%s   ", i);
    }
    printf("\n");
  }


  {
    LanceDB::FieldData test_data("test7",
                                        std::vector<BinaryData>{
                                            { { 1, 2, 3, 4, 5, } },
                                            { { 2, 4, 5, 6, 7, 8, 9, 0 } }
                                        }, kLanceDBFieldTypeScalar);
    ASSERT_TRUE(test_data.IsDataValid());
    ASSERT_EQ(test_data.GetFieldType(), kLanceDBFieldTypeScalar);
    ASSERT_EQ(test_data.GetDataType(), kLanceDBFieldTypeBlob);
    ASSERT_EQ(test_data.GetDimension(), 1);
    ASSERT_TRUE(test_data.IsDataValid());
    auto& flatdata = test_data.GetFlattenData();
    auto& ds = test_data.GetBinaryDataSize();
    ASSERT_EQ(flatdata.size(), ds.size());
    printf("flat data: ");
    for (int i=0; i<flatdata.size(); i++) {
      for (auto j=0; j<ds[i]; j++) {
        printf("%d  ", flatdata[i][j]);
      }
      printf("\n");
    }
    printf("\n");
  }

  {
    LanceDB::FlatFieldData test_data("test7",
                                        std::vector<BinaryData>{
                                            { { 1, 2, 3, 4, 5, } },
                                            { { 2, 4, 5, 6, 7, 8, 9, 0 } }
                                        }, kLanceDBFieldTypeScalar);
    ASSERT_TRUE(test_data.IsDataValid());
    ASSERT_EQ(test_data.GetFieldType(), kLanceDBFieldTypeScalar);
    ASSERT_EQ(test_data.GetDataType(), kLanceDBFieldTypeBlob);
    ASSERT_EQ(test_data.GetDimension(), 1);
    ASSERT_TRUE(test_data.IsDataValid());
    auto& flatdata = test_data.GetFlattenData();
    auto& ds = test_data.GetBinaryDataSize();
    ASSERT_EQ(flatdata.size(), ds.size());
    printf("flat data: ");
    for (int i=0; i<flatdata.size(); i++) {
      for (auto j=0; j<ds[i]; j++) {
        printf("%d  ", flatdata[i][j]);
      }
      printf("\n");
    }
    printf("\n");

    auto cfd = LanceDB::GetCFieldData(test_data);
    ASSERT_EQ(cfd.data_type, kLanceDBFieldTypeBlob);
    ASSERT_EQ(cfd.field_type, kLanceDBFieldTypeScalar);
    ASSERT_EQ(cfd.data_count, 2);
    ASSERT_EQ(cfd.dimension, 1);
    ASSERT_EQ(cfd.binary_size[0], 5);
    ASSERT_EQ(cfd.binary_size[1], 8);
    ASSERT_EQ(((uint8_t**)cfd.data)[0][0], 1);
    ASSERT_EQ(((uint8_t**)cfd.data)[0][1], 2);
  }
}

TEST(LanceDB, BatchInserter) {
  using namespace lancedb;
  system("rm -rf test_inserter.db");
  LanceDB db("test_inserter.db");
  int data_count = 100;
  std::vector<int> idx;
  std::vector<std::vector<float>> embeddings;
  for (int i=0; i<data_count; i++) {
    idx.push_back(i);

    // generate random embedding and normalize
    std::vector<float> embedding(768);
    for (int j=0; j<768; j++) {
      embedding[j] = (rand() % 1000) / 1000.0f;
    }
    // set the 45th embedding to all-1.0
    if (i == 44) {
      for (int j=0; j<768; j++) {
        embedding[j] = 1.f;
      }
    }
    // normalize
    float norm = 0;
    for (int j=0; j<768; j++) {
      norm += embedding[j] * embedding[j];
    }
    norm = std::sqrt(norm);
    for (int j=0; j<768; j++) {
      embedding[j] /= norm;
    }
    embeddings.push_back(std::move(embedding));
  }
  std::vector<std::string> comments;
  for (int i=0; i<data_count; i++) {
    comments.push_back(std::string("Today you are so beautiful! I repeat for ")
          + std::to_string(i) + " times!");
  }

  LanceDB::FieldData idx_data("idx", idx);
  LanceDB::FieldData embedding_data("embedding", embeddings);
  LanceDB::FieldData comment_data("comment", comments);

  auto inserter = db.CreateBatchInserter(idx_data, embedding_data, comment_data);
  auto err = inserter.CreateTable("test_table");
  ASSERT_EQ(err, kLanceDBSuccess);
  err = inserter.Insert("test_table");
  ASSERT_EQ(err, kLanceDBSuccess);

  // get the 45th embedding
  auto embedding = embeddings[44];
  LanceDB::SearchResults sr;
  err = db.Query("test_table", "embedding", embedding, sr);
  ASSERT_EQ(err, kLanceDBSuccess);
  ASSERT_EQ(sr.IsValid(), true);
  LanceDBTool::PrintResult(sr.Get());
}

struct TestTable {
  int id;
  std::vector<float> embedding;
  std::string content;
  int page;
  int chapter;
  std::string chapter_title;
};

BEGIN_DEFINE_LANCEDB_SCHEMA_ADAPTER(TestTable, 6)
  LANCE_DB_INTERNAL_DEFINE_TABLE_FIELD(0, id)
  LANCE_DB_INTERNAL_DEFINE_TABLE_FIELD(1, embedding)
  LANCE_DB_INTERNAL_DEFINE_TABLE_FIELD(2, content)
  LANCE_DB_INTERNAL_DEFINE_TABLE_FIELD(3, page)
  LANCE_DB_INTERNAL_DEFINE_TABLE_FIELD(4, chapter)
  LANCE_DB_INTERNAL_DEFINE_TABLE_FIELD(5, chapter_title)
END_DEFINE_LANCEDB_SCHEMA_ADAPTER(TestTable)

static std::vector<TestTable> LoadTestData() {
  std::vector<TestTable> data;
  int num_data = 100;
  for (int i=0; i<num_data; i++) {
    TestTable tt;
    tt.id = i;
    tt.page = i % 10;
    tt.chapter = i % 5;
    tt.chapter_title = "Chapter " + std::to_string(tt.chapter);
    tt.content = "This is the content of page " + std::to_string(tt.page);
    tt.embedding.resize(768);
    for (int j=0; j<768; j++) {
      tt.embedding[j] = (rand() % 1000) / 1000.0f;
    }
    // set 55th embedding to all-1.0
    if (i == 55) {
      for (int j=0; j<100; j++) {
        tt.embedding[j] = 1.f;
      }
    }
    // normalize
    float norm = 0;
    for (int j=0; j<768; j++) {
      norm += tt.embedding[j] * tt.embedding[j];
    }
    norm = std::sqrt(norm);
    for (int j=0; j<768; j++) {
      tt.embedding[j] /= norm;
    }
    data.push_back(std::move(tt));
  }
  return data;
}

TEST(LanceDB, SchameAdapter) {
  system("rm -rf test_schema_adapter.db");

  std::vector<TestTable> data = LoadTestData();
  LanceDB db("test_schema_adapter.db");
  TestTableSchema schema = TestTableSchema(db)
    .SetCreateTable(true)
    .SetCreateData(true);
  auto err = schema.Run(data);
  ASSERT_EQ(err, kLanceDBSuccess);

  const auto& embedding = data[55].embedding;
  LanceDB::SearchResults sr;
  err = schema.Query("embedding", embedding, sr);
  ASSERT_EQ(err, kLanceDBSuccess);
  ASSERT_EQ(sr.IsValid(), true);
  //LanceDBTool::PrintResult(sr.Get());

  TestTableResult res;
  err = schema.Query("embedding", embedding, res);
  ASSERT_EQ(err, kLanceDBSuccess);
  ASSERT_FALSE(res.distances.empty());
  ASSERT_FALSE(res.results.empty());

  printf("ID:       ");
  for (auto& tbl: res.results) {
    printf("%d  ", tbl.id);
  }
  printf("\n");

  printf("Distance: ");
  for (auto& d: res.distances) {
    printf("%f  ", d);
  }
  printf("\n");

  ASSERT_EQ(res.results[0].id, 55);
  printf("Embedding[0]: ");
  for (int i=0; i<10; i++) {
    printf("%f  ", res.results[0].embedding[i]);
    ASSERT_FLOAT_EQ(res.results[0].embedding[i], embedding[i]);
  }
  printf("...\n");
}
