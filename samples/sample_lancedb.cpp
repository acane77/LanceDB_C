#include <cstdio>
#include <cstdlib>
#include <cmath>

#include "lancedb.hpp"
#include "lancedb_tools.hpp"

#define ASSERT_EQ(a, b) if ((a) != (b)) { fprintf(stderr, "assert failed: %s != %s\n", #a, #b); exit(1); }

int main() {
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
