#include <cstdio>
#include <cstdlib>
#include <cmath>

#include "lancedb.hpp"
#include "lancedb_tools.hpp"
#include "table_schema.hpp"
#include "table_schema_adapter.hpp"

#define ASSERT_EQ(a, b) if ((a) != (b)) { fprintf(stderr, "assert failed: %s != %s\n", #a, #b); exit(1); }
#define ASSERT_FALSE(x) ASSERT_EQ(x, false)
#define ASSERT_FLOAT_EQ(a, b) if (std::abs((a) - (b)) > 1e-6) { fprintf(stderr, "assert failed: %f != %f\n", (a), (b)); exit(1); }

using namespace lancedb;

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

int main() {
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