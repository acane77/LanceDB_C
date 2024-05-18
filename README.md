# LanceDB C API

This project is a C/C++ API for [LanceDB](https://github.com/lancedb/lancedb).

## Build RUST dependencies

**Linux (x86_64)**
```shell
./build_c_lib.sh
```

**Android**

Specify Android NDK path in `build_c_lib.sh` and run the following commands:

```shell
rustup target add aarch64-linux-android
./build_c_lib.sh android
```

## Header files

* [lancedb.h](include/lancedb.h) - C API header file
* [lancedb.hpp](include/lancedb.hpp) - C++ API header file
* [table_schema.hpp](include/table_schema.hpp) - Table schema helper class
* [table_schema_adapter.hpp](include/table_schema_adapter.hpp) - Table schema adapter, to define an adapter between C++ types and LanceDB data fields.

## Sample Usage

* Sample for C API usage: [samples/sample_lancedb_c.cpp](samples/sample_lancedb_c.cpp)
* Sample for C++ API usage: [samples/sample_lancedb.cpp](samples/sample_lancedb.cpp)
* Sample for Table Schema API usage: [samples/sample_lancedb_schema.cpp](samples/sample_lancedb_schema.cpp)