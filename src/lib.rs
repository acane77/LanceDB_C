#[macro_use]
extern crate lazy_static;

pub use lancedb;
use lancedb::{Connection};
use tokio::runtime::Runtime;
use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, BinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, RecordBatchIterator, StringArray, TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use std::collections::HashMap;
use std::sync::Mutex;
use std::os::raw::c_void;
use std::os::raw::c_char;
use std::marker::PhantomData;
use std::hash::{Hash, Hasher};

use std::ffi::{CStr, CString};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMillisecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow_schema::DataType::FixedSizeList;
use lancedb::query::ExecutableQuery;
use std::mem;
use std::ptr::{null_mut};

struct SendPtr(*mut c_void, PhantomData<Vec<u8>>);

impl PartialEq for SendPtr {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SendPtr {}

impl Hash for SendPtr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

unsafe impl Send for SendPtr {}

lazy_static! {
    static ref CONNECTIONS: Mutex<HashMap<SendPtr, SendPtr>> = Mutex::new(HashMap::new());
}

pub async fn lancedb_init_async(uri: &str) -> Connection {
    let db = lancedb::connect(uri).execute().await.unwrap();
    // !("db connected");
    return db;
}

////////////// C TYPES //////////////

#[repr(C)]
#[derive(Debug)]
pub enum lancedb_field_data_type_t {
    LanceDBFieldTypeInt8,
    LanceDBFieldTypeInt16,
    LanceDBFieldTypeInt32,
    LanceDBFieldTypeInt64,
    LanceDBFieldTypeUInt8,
    LanceDBFieldTypeUInt16,
    LanceDBFieldTypeUInt32,
    LanceDBFieldTypeUInt64,
    LanceDBFieldTypeFloat16,
    LanceDBFieldTypeFloat32,
    LanceDBFieldTypeFloat64,
    LanceDBFieldTypeString,
    LanceDBFieldTypeBlob,
    LanceDBFieldTypeTimestamp,
}

#[repr(C)]
#[derive(Debug)]
pub enum lancedb_field_type_t {
    LanceDBFieldTypeScalar,
    LanceDBFieldTypeVector,
}
impl PartialEq for lancedb_field_type_t {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (lancedb_field_type_t::LanceDBFieldTypeScalar, lancedb_field_type_t::LanceDBFieldTypeScalar) => true,
            (lancedb_field_type_t::LanceDBFieldTypeVector, lancedb_field_type_t::LanceDBFieldTypeVector) => true,
            _ => false,
        }
    }
}

#[repr(C)]
pub struct lancedb_table_field_t {
    name: *const c_char,
    data_type: lancedb_field_data_type_t,
    field_type: lancedb_field_type_t,
    create_index: i32,
    dimension: i32,
    nullable: i32,
}

#[repr(C)]
pub struct lancedb_schema_t {
    fields: *mut lancedb_table_field_t,
    num_fields: usize,
}


#[repr(C)]
pub struct lancedb_field_data_t {
    name: *const c_char,
    data_type: lancedb_field_data_type_t,
    field_type: lancedb_field_type_t,
    data_count: usize,
    dimension: usize, // only for vector
    data: *mut c_void, // for vector, it is a pointer to a 2D array; for scalar, it is a pointer to a 1D array
    binary_size: * mut usize, // only for blob types
}

#[repr(C)]
pub struct lancedb_data_t {
    fields: *mut lancedb_field_data_t,
    num_fields: usize,
}

////////////// END OF C TYPES //////////////

#[no_mangle]
pub extern "C" fn lancedb_init(uri: *const c_char) -> *mut c_void {
    let uri = unsafe {
        assert!(!uri.is_null());
        CStr::from_ptr(uri).to_str().unwrap()
    };

    let rt = Runtime::new().unwrap();
    let connection = rt.block_on(lancedb_init_async(uri));

    let connection_box = Box::new(connection);
    let connection_ptr = Box::into_raw(connection_box) as *mut c_void;

    CONNECTIONS.lock().unwrap().insert(SendPtr(connection_ptr, PhantomData),
                                       SendPtr(connection_ptr, PhantomData));

    connection_ptr
}

#[no_mangle]
pub extern "C" fn lancedb_close(connection_ptr: *mut c_void) -> bool {
    let mut connections = CONNECTIONS.lock().unwrap();
    let send_ptr = SendPtr(connection_ptr, PhantomData);
    if let Some(_connection) = connections.remove(&send_ptr) {
        // Deallocate the memory for the Connection instance
        unsafe {
            let _ = Box::from_raw(connection_ptr as *mut Connection);
        }
        // println!("connection closed");
        true
    } else {
        // println!("connection close failed");
        false
    }
}

#[no_mangle]
pub extern "C" fn lancedb_create_table(
    connection_ptr: *mut c_void,
    table_name: *const c_char,
    data: *mut f32,
    dimension: i32,
    count: i32,
) -> bool {

    use std::sync::Arc;
    use std::slice;

    // Convert C types to Rust types
    let table_name = unsafe {
        assert!(!table_name.is_null());
        CStr::from_ptr(table_name).to_str().unwrap()
    };

    let data = unsafe {
        assert!(!data.is_null());
        slice::from_raw_parts(data, (count * dimension) as usize)
    };

    // println!("data: dimension: {}, count: {}", dimension, count);

    // Create the table schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)),
                                    dimension as i32),
            true,
        ),
    ]));

    // Create a RecordBatch stream
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..count)),
                    Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            data.chunks(dimension as usize).map(
                                |chunk| Some(chunk.iter().map(|&v| Some(v)).collect::<Vec<_>>())),
                            dimension as i32,
                        ),
                    ),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
        schema.clone(),
    );

    // Get the connection from the HashMap
    let connections = CONNECTIONS.lock().unwrap();
    let send_ptr = SendPtr(connection_ptr, PhantomData);
    let send_ptr = connections.get(&send_ptr).unwrap();
    let connection = unsafe { &mut *(send_ptr.0 as *mut Connection) };

    // Create the table
    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        connection
            .create_table(table_name, Box::new(batches))
            .execute()
            .await
    });

    let _ = match result {
        Ok(table) => table,
        Err(e) => {
            eprintln!("Failed to create table: {}", e);
            return false;
        }
    };

    // // Index with kmeans,
    // // will report error: "KMeans: can not train 256 centroids with 128 vectors, choose a smaller K (< 128) instead
    // // if cout >= 128
    // use lancedb::index::Index;
    // rt.block_on(async {
    //     table.create_index(&["vector"], Index::Auto)
    //         .execute()
    //         .await
    //         .unwrap();
    // });

    return true;
}

#[no_mangle]
pub extern "C" fn lancedb_create_table_with_schema(
    connection_ptr: *mut c_void,
    table_name: *const c_char,
    schema: *mut lancedb_schema_t,
) -> bool {
    use std::sync::Arc;
    use std::slice;

    // Convert C types to Rust types
    let table_name = unsafe {
        assert!(!table_name.is_null());
        CStr::from_ptr(table_name).to_str().unwrap()
    };

    let schema = unsafe {
        assert!(!schema.is_null());
        &*schema
    };

    // Convert the schema to a Rust vector of Fields
    let fields = unsafe {
        slice::from_raw_parts(schema.fields, schema.num_fields)
    };

    let mut rust_fields = Vec::new();
    for field in fields {
        let name = unsafe {
            assert!(!field.name.is_null());
            CStr::from_ptr(field.name).to_str().unwrap()
        };

        let data_type = match &field.data_type {
            lancedb_field_data_type_t::LanceDBFieldTypeInt8 => DataType::Int8,
            lancedb_field_data_type_t::LanceDBFieldTypeInt16 => DataType::Int16,
            lancedb_field_data_type_t::LanceDBFieldTypeInt32 => DataType::Int32,
            lancedb_field_data_type_t::LanceDBFieldTypeInt64 => DataType::Int64,
            lancedb_field_data_type_t::LanceDBFieldTypeUInt8 => DataType::UInt8,
            lancedb_field_data_type_t::LanceDBFieldTypeUInt16 => DataType::UInt16,
            lancedb_field_data_type_t::LanceDBFieldTypeUInt32 => DataType::UInt32,
            lancedb_field_data_type_t::LanceDBFieldTypeUInt64 => DataType::UInt64,
            lancedb_field_data_type_t::LanceDBFieldTypeFloat16 => DataType::Float16,
            lancedb_field_data_type_t::LanceDBFieldTypeFloat32 => DataType::Float32,
            lancedb_field_data_type_t::LanceDBFieldTypeFloat64 => DataType::Float64,
            lancedb_field_data_type_t::LanceDBFieldTypeString => DataType::Utf8,
            lancedb_field_data_type_t::LanceDBFieldTypeBlob => DataType::Binary,
            &lancedb_field_data_type_t::LanceDBFieldTypeTimestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
        };

        match field.field_type {
            lancedb_field_type_t::LanceDBFieldTypeScalar => {
                // println!("field: {}, data_type: {:?}, nullable: {}",
                //          name, data_type, field.nullable != 0);
                rust_fields.push(Field::new(name, data_type, field.nullable != 0));
            }
            lancedb_field_type_t::LanceDBFieldTypeVector => {
                // println!("field: {}, data_type: {:?}[], dimension: {}, nullable: {}",
                //          name, data_type, field.dimension, field.nullable != 0);
                rust_fields.push(Field::new(name, DataType::FixedSizeList(Arc::new(Field::new("item", data_type, true)),
                                        field.dimension as i32), field.nullable != 0));
            }
        }
    }

    // Create the table schema
    let schema = Arc::new(Schema::new(rust_fields));
    // println!("schema: {:?}", schema);

    // Get the connection from the HashMap
    let connections = CONNECTIONS.lock().unwrap();
    let send_ptr = SendPtr(connection_ptr, PhantomData);
    let send_ptr = connections.get(&send_ptr).unwrap();
    let connection = unsafe { &mut *(send_ptr.0 as *mut Connection) };

    // Create the table
    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        connection
            .create_table(table_name, Box::new(RecordBatchIterator::new(vec![].into_iter().map(Ok), schema.clone())))
            .execute()
            .await
    });

    match result {
        Ok(_) => true,
        Err(e) => {
            eprintln!("Failed to create table: {}", e);
            false
        }
    }
}

#[no_mangle]
pub extern "C" fn lancedb_insert(
    connection_ptr: *mut c_void,
    table_name: *const c_char,
    field_data: *mut lancedb_data_t,
) -> bool {
    use std::sync::Arc;
    use std::slice;

    // Convert C types to Rust types
    let table_name = unsafe {
        assert!(!table_name.is_null());
        CStr::from_ptr(table_name).to_str().unwrap()
    };

    let field_data = unsafe {
        assert!(!field_data.is_null());
        &*field_data
    };

    // Convert the field_data to a Rust vector of Fields
    let data = unsafe {
        slice::from_raw_parts(field_data.fields as *const lancedb_field_data_t, field_data.num_fields)
    };

    // Get the connection from the HashMap
    let connections = CONNECTIONS.lock().unwrap();
    let send_ptr = SendPtr(connection_ptr, PhantomData);
    let send_ptr = connections.get(&send_ptr).unwrap();
    let connection = unsafe { &mut *(send_ptr.0 as *mut Connection) };

    // Insert the data into the table
    let rt = Runtime::new().unwrap();
    let table = rt.block_on(async {
        let table = connection.open_table(table_name).execute().await;
        return table;
    });

    let table = match table {
        Ok(table) => table,
        Err(e) => {
            eprintln!("Failed to open table: {}", e);
            return false;
        }
    };

    // println!("field_data.num_fields: {:?}", field_data.num_fields);

    let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

    for field_data in data {
        // println!("field_data.data_count: {:?}", field_data.data_count);
        let dimension = &field_data.dimension;
        if field_data.field_type == lancedb_field_type_t::LanceDBFieldTypeScalar {
            macro_rules! create_array_for_scalar {
                ($array_type:ty, $primitive_type:ty) => {
                    let raw_data = field_data.data as *mut $primitive_type;
                    let rust_data = unsafe {
                        assert!(!raw_data.is_null());
                        slice::from_raw_parts(raw_data, field_data.data_count as usize)
                    };
                    let array = Arc::new(<$array_type>::from_iter_values(rust_data.iter().map(|&v| (v))));
                    arrays.push(array);
                };
            }
            match field_data.data_type {
                lancedb_field_data_type_t::LanceDBFieldTypeInt8 => {
                    create_array_for_scalar!(Int8Array, i8);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeInt16 => {
                    create_array_for_scalar!(Int16Array, i16);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeInt32 => {
                    create_array_for_scalar!(Int32Array, i32);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeInt64 => {
                    create_array_for_scalar!(Int64Array, i64);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeUInt8 => {
                    create_array_for_scalar!(UInt8Array, u8);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeUInt16 => {
                    create_array_for_scalar!(UInt16Array, u16);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeUInt32 => {
                    create_array_for_scalar!(UInt32Array, u32);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeUInt64 => {
                    create_array_for_scalar!(UInt64Array, u64);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeFloat16 => {
                    create_array_for_scalar!(Float16Array, <Float16Type as ArrowPrimitiveType>::Native);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeFloat32 => {
                    create_array_for_scalar!(Float32Array, f32);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeFloat64 => {
                    create_array_for_scalar!(Float64Array, f64);
                }
                lancedb_field_data_type_t::LanceDBFieldTypeString => {
                    // Create arrow_array to represent a string
                    let raw_data = field_data.data as *mut *const c_char;
                    let rust_data = unsafe {
                        assert!(!raw_data.is_null());
                        slice::from_raw_parts(raw_data, field_data.data_count as usize)
                    };
                    let mut string_data = Vec::new();
                    for i in 0..field_data.data_count {
                        let c_str = unsafe {
                            assert!(!rust_data[i].is_null());
                            CStr::from_ptr(rust_data[i])
                        };
                        let string = c_str.to_str().unwrap();
                        string_data.push(string);
                    }

                    arrays.push(Arc::new(StringArray::from_iter_values(string_data)));
                }
                lancedb_field_data_type_t::LanceDBFieldTypeBlob => {
                    let binary_size = field_data.binary_size;
                    // Create arrow_array to represent a blob
                    let raw_data = field_data.data as *mut *const u8;
                    let rust_data = unsafe {
                        assert!(!raw_data.is_null());
                        slice::from_raw_parts(raw_data, field_data.data_count as usize)
                    };
                    let mut blob_data = Vec::new();
                    for i in 0..field_data.data_count {
                        let size = unsafe {
                            assert!(!binary_size.is_null());
                            *binary_size.wrapping_add(i)
                        };
                        let data = unsafe {
                            assert!(!rust_data[i].is_null());
                            slice::from_raw_parts(rust_data[i], size)
                        };
                        blob_data.push(data);
                    }
                    arrays.push(Arc::new(BinaryArray::from_iter_values(blob_data)));
                }
                lancedb_field_data_type_t::LanceDBFieldTypeTimestamp => {
                    create_array_for_scalar!(TimestampMillisecondArray, i64);
                }
            };
        }
        else {
            // println!("field_data.dimension: {:?}", field_data.dimension);
            macro_rules! create_array_data {
                ($data_type:ty, $rust_type:ty) => {
                    // print!("field_data.data_count: {:?}", field_data.data_count);
                    let raw_data = field_data.data as *mut $rust_type;
                    // print!("raw_data: {:?}", raw_data);
                    let rust_data = unsafe {
                        assert!(!raw_data.is_null());
                        slice::from_raw_parts(raw_data, (field_data.data_count * dimension) as usize)
                    };
                    let dimensionv: usize = *dimension;
                    let arr = Arc::new(FixedSizeListArray::from_iter_primitive::<$data_type, _, _>(
                            rust_data.chunks(dimensionv).map(
                                |chunk| Some(chunk.iter().map(|&v| Some(v)).collect::<Vec<_>>())),
                            field_data.dimension as i32,
                        ));
                    // print!("insert array: {:?}", arr);
                    arrays.push(arr)
                };
            }
            let _ = match field_data.data_type {
                lancedb_field_data_type_t::LanceDBFieldTypeInt8 => {
                    create_array_data!(Int8Type, i8);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeInt16 => {
                    create_array_data!(Int16Type, i16);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeInt32 => {
                    create_array_data!(Int32Type, i32);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeInt64 => {
                    create_array_data!(Int64Type, i64);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeUInt8 => {
                    create_array_data!(UInt8Type, u8);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeUInt16 => {
                    create_array_data!(UInt16Type, u16);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeUInt32 => {
                    create_array_data!(UInt32Type, u32);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeUInt64 => {
                    create_array_data!(UInt64Type, u64);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeFloat16 => {
                    create_array_data!(Float16Type, <Float16Type as ArrowPrimitiveType>::Native);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeFloat32 => {
                    create_array_data!(Float32Type, f32);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeFloat64 => {
                    create_array_data!(Float64Type, f64);
                },
                lancedb_field_data_type_t::LanceDBFieldTypeString => {
                    panic!("Unsupported data type: String");
                },
                lancedb_field_data_type_t::LanceDBFieldTypeTimestamp => {
                    create_array_data!(TimestampMillisecondType, i64);
                },
                _ => {
                    panic!("Unsupported data type");
                }
            };
        }

        // println!("insert array: {:?}", array);
        // arrays.push(array.unwrap());
    }

    // Create a RecordBatch stream
    let schema = rt.block_on(async {
        return table.schema().await.unwrap();
    });

    let batches =
        RecordBatchIterator::new(
            vec![Ok(RecordBatch::try_new( schema.clone(), arrays).unwrap())].into_iter(),
            schema.clone());

    let result = rt.block_on(async {
        return table.add(Box::new(batches)).execute().await;
    });
    match result {
        Ok(_) => true,
        Err(e) => {
            eprintln!("Failed to insert data: {}", e);
            false
        }
    }
}


fn convert_to_i32_vec(array: &ArrayRef) -> Result<Vec<i32>, std::io::Error> {
    let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
    let mut vec = Vec::new();
    for i in 0..int32_array.len() {
        let value = int32_array.value(i);
        vec.push(value);
    }

    Ok(vec)
}

fn string_to_c_char_ptr(s: String) -> *mut c_char {
    let c_string = CString::new(s).unwrap();
    c_string.into_raw()
}

#[no_mangle]
pub extern "C" fn lancedb_search(
    connection_ptr: *mut c_void,
    table_name: *const c_char,
    column_name: *const c_char,
    data: *const c_void,
    dimension: i32,
    search_results: *mut lancedb_data_t,
) -> bool {
    use std::slice;

    // Convert C types to Rust types
    let table_name = unsafe {
        assert!(!table_name.is_null());
        CStr::from_ptr(table_name).to_str().unwrap()
    };
    let column_name = unsafe {
        assert!(!column_name.is_null());
        CStr::from_ptr(column_name).to_str().unwrap()
    };

    // Get the connection from the HashMap
    let connections = CONNECTIONS.lock().unwrap();
    let send_ptr = SendPtr(connection_ptr, PhantomData);
    let send_ptr = connections.get(&send_ptr).unwrap();
    let connection = unsafe { &mut *(send_ptr.0 as *mut Connection) };

    use futures_util::TryStreamExt;
    // Perform the query
    let rt = Runtime::new().unwrap();
    let result = rt.block_on(async {
        let table = connection.open_table(table_name).execute().await;

        let table = match table {
            Ok(table) => table,
            Err(e) => {
                eprintln!("Failed to open table: {}", e);
                return false;
            }
        };

        let schema = table.schema().await.unwrap();
        // find the column matches column name
        let mut column_index = 0xffffffff;
        for (index, field) in schema.fields().iter().enumerate() {
            if field.name() == column_name {
                column_index = index;
                break;
            }
        }
        if column_index == 0xffffffff {
            eprintln!("Failed to find column: {}", column_name);
            return false;
        }

        let fields = schema.fields();
        let field = fields.get(column_index).unwrap();
        let field_data_type = field.data_type();
        let mut inner_type= field_data_type;
        let field_type = match field_data_type {
            FixedSizeList(inner_ty, dim) => {
                inner_type = inner_ty.data_type();
                lancedb_field_type_t::LanceDBFieldTypeVector
            },
            _ => {
                lancedb_field_type_t::LanceDBFieldTypeScalar
            }
        };
        if field_type == lancedb_field_type_t::LanceDBFieldTypeScalar {
            eprintln!("Not a vector field: {}", column_name);
            return false;
        }

        let query = table
            .query();

        let results = match inner_type {
            DataType::Float32 => {
                let data = unsafe {
                    assert!(!data.is_null());
                    slice::from_raw_parts(data as *const f32, dimension as usize)
                };
                // println!("f32 data: {:?}", data);
                query.nearest_to(data)
            },
            DataType::Float64 => {
                let data = unsafe {
                    assert!(!data.is_null());
                    slice::from_raw_parts(data as *const f64, dimension as usize)
                };
                // println!("f64 data: {:?}", data);
                query.nearest_to(data)
            },
            _ => {
                panic!("Unsupported data type");
            }
        };

        // let results = query
        //     .nearest_to(data);

        let results = results
            .unwrap()
            .distance_type(lancedb::DistanceType::Cosine)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        for result in &results {
            // println!("result: {:?}", result);

            let indexes_array = result.column(0);
            let indexes_vec = convert_to_i32_vec(indexes_array).unwrap();
            // println!("indexes: {:?}", indexes);
            let schema = result.schema();
            /*
            schema: Schema {
                fields: [
                    Field { name: "id", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                    Field { name: "name", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                    Field { name: "vector", data_type: FixedSizeList(Field { name: "item", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, 512), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                    Field { name: "time", data_type: Timestamp(Millisecond, None), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                    Field { name: "comment", data_type: Binary, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                    Field { name: "binary", data_type: Binary, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} },
                    Field { name: "_distance", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }
                ]
             }
            */
            let mut field_data_vec: Vec<lancedb_field_data_t> = Vec::new();
            let fiels_data_count = schema.fields().len();

            for (index, field) in schema.fields().iter().enumerate() {
                // println!("field: {:?}", field);

                let mut field_data_type = field.data_type();
                let mut field_type = lancedb_field_type_t::LanceDBFieldTypeScalar;
                let mut dimension :usize = 1;
                let field_name = field.name();
                // println!(">> name: {:?}", field_name);
                // println!("    data_type: {:?}", field_data_type);
                // let mut inner_field: &FieldRef = field;

                match field.data_type() {
                    FixedSizeList(inner_type, dim) => {
                        field_type = lancedb_field_type_t::LanceDBFieldTypeVector;
                        field_data_type = inner_type.data_type();
                        // println!("    inner field_data_type: {:?}", field_data_type);
                        dimension = *dim as usize;
                        // inner_field = inner_type;
                    },
                    _ => { /* do nothing */ }
                };

                let column_data = result.column(index);
                let data_count = column_data.len();

                // println!("    field_type: {:?}", field_type);
                // println!("    dimension: {:?}", dimension);
                // println!("    data_count: {:?}", data_count);
                // println!("    column_data: {:?}", column_data);
                // println!("    inner_field: {:?}", inner_field);

                let data_ptr : *mut c_void;
                let mut binary_size_ptr : *mut usize = null_mut();

                // macro_rules! heap_malloc {
                //     ($alloc_sz:expr) => {
                //         Box::into_raw(vec![0 as i8; $alloc_sz].into_boxed_slice()) as *mut c_void
                //     };
                // }

                macro_rules! create_array_data_scalar {
                    ($array_type:ty, $rust_type:ty) => {
                        let alloc_sz = data_count * dimension * mem::size_of::<$rust_type>();
                        let data = Box::into_raw(vec![0 as i8; alloc_sz].into_boxed_slice()) as *mut c_void;
                        data_ptr = data;
                        // println!("allocated memory {} bytes", alloc_sz);
                        assert_eq!(dimension, 1);
                        let array = column_data.as_any().downcast_ref::<$array_type>().unwrap();
                        let mut vec = Vec::new();
                        for i in 0..array.len() {
                            let value = array.value(i);
                            vec.push(value);
                        }
                        let raw_data = data as *mut $rust_type;
                        let rust_data = unsafe {
                            assert!(!raw_data.is_null());
                            slice::from_raw_parts_mut(raw_data, data_count as usize)
                        };
                        rust_data.copy_from_slice(&vec);
                    };
                }

                macro_rules! create_array_data_vector {
                    ($array_type:ty, $rust_type:ty) => {
                        // alloc memory in heap with size of data_count * dimension * size_of(i8)
                        let alloc_sz = data_count * dimension * mem::size_of::<$rust_type>();
                        let data = Box::into_raw(vec![0 as i8; alloc_sz].into_boxed_slice()) as *mut c_void;
                        // println!("allocated memory {} bytes", alloc_sz);
                        data_ptr = data;
                        // copy data to data with dimension * data_count
                        let fs_lst = column_data.as_fixed_size_list();
                        // println!("fs_lst: {:?}", fs_lst);
                        let mut vec :Vec<$rust_type> = Vec::new();
                        for i in 0..fs_lst.len() {
                            let value_array = fs_lst.value(i);
                            if let Some(st_array) = value_array.as_any().downcast_ref::<$array_type>() {
                                for _j in 0..dimension {
                                    vec.push(st_array.value(_j));
                                }
                            } else {
                                panic!("Not a/an {} array", stringify!($array_type));
                            }

                        }
                        let raw_data = data as *mut $rust_type;
                        let rust_data = unsafe {
                            assert!(!raw_data.is_null());
                            std::slice::from_raw_parts_mut(raw_data, data_count as usize * dimension)
                        };
                        rust_data.copy_from_slice(&vec);
                    };
                }

                macro_rules! create_array_data {
                    ($array_type:ty, $rust_type:ty) => {
                        match field_type {
                            lancedb_field_type_t::LanceDBFieldTypeScalar => {
                                create_array_data_scalar!($array_type, $rust_type);
                            },
                            lancedb_field_type_t::LanceDBFieldTypeVector => {
                                create_array_data_vector!($array_type, $rust_type);
                            }
                        }
                    };
                }

                let data_type = match field_data_type {
                    DataType::Int8 => {
                        create_array_data!(Int8Array, i8);
                        lancedb_field_data_type_t::LanceDBFieldTypeInt8
                    },
                    DataType::Int16 => {
                        create_array_data!(Int16Array, i16);
                        lancedb_field_data_type_t::LanceDBFieldTypeInt16
                    },
                    DataType::Int32 => {
                        create_array_data!(Int32Array, i32);
                        lancedb_field_data_type_t::LanceDBFieldTypeInt32
                    },
                    DataType::Int64 => {
                        create_array_data!(Int64Array, i64);
                        lancedb_field_data_type_t::LanceDBFieldTypeInt64
                    },
                    DataType::UInt8 => {
                        create_array_data!(UInt8Array, u8);
                        lancedb_field_data_type_t::LanceDBFieldTypeUInt8
                    },
                    DataType::UInt16 => {
                        create_array_data!(UInt16Array, u16);
                        lancedb_field_data_type_t::LanceDBFieldTypeUInt16
                    },
                    DataType::UInt32 => {
                        create_array_data!(UInt32Array, u32);
                        lancedb_field_data_type_t::LanceDBFieldTypeUInt32
                    },
                    DataType::UInt64 => {
                        create_array_data!(UInt64Array, u64);
                        lancedb_field_data_type_t::LanceDBFieldTypeUInt64
                    },
                    DataType::Float16 => {
                        create_array_data!(Float16Array, <Float16Type as ArrowPrimitiveType>::Native);
                        lancedb_field_data_type_t::LanceDBFieldTypeFloat16
                    },
                    DataType::Float32 => {
                        create_array_data!(Float32Array, f32);
                        lancedb_field_data_type_t::LanceDBFieldTypeFloat32
                    },
                    DataType::Float64 => {
                        create_array_data!(Float64Array, f64);
                        lancedb_field_data_type_t::LanceDBFieldTypeFloat64
                    },
                    DataType::Binary => {
                        match field_type {
                            lancedb_field_type_t::LanceDBFieldTypeScalar => {
                                let fs_lst = column_data.as_binary::<i32>();
                                let binary_len : Vec<usize> = fs_lst.iter().map(|x| x.unwrap().len()).collect();
                                // println!("binary_len: {:?}", binary_len);
                                binary_size_ptr = Box::into_raw(binary_len.into_boxed_slice()) as *mut usize;
                                // println!("fs_lst: {:?}", fs_lst);
                                let mut binary_vec :Vec<*const c_char> = Vec::new();
                                for i in 0..fs_lst.len() {
                                    let mut single_data: Vec<u8> = Vec::new();
                                    let value_array = fs_lst.value(i);
                                    for _j in 0..value_array.len() {
                                        single_data.push(value_array[_j]);
                                    }
                                    // println!("single_data: {:?}", single_data);
                                    let raw_data = Box::into_raw(single_data.into_boxed_slice()) as *const u8;
                                    binary_vec.push(raw_data as *const c_char);
                                }
                                let binary_vector_c = Box::into_raw(binary_vec.into_boxed_slice()) as *mut *const c_char;
                                data_ptr = binary_vector_c as *mut c_void;
                            },
                            lancedb_field_type_t::LanceDBFieldTypeVector => {
                                panic!("Unsupported data type: Vec<Binary>");
                            }
                        }
                        lancedb_field_data_type_t::LanceDBFieldTypeBlob
                    },
                    DataType::Timestamp(_, _) => {
                        create_array_data!(TimestampMillisecondArray, i64);
                        lancedb_field_data_type_t::LanceDBFieldTypeTimestamp
                    },
                    DataType::Utf8 => {
                        match field_type {
                            lancedb_field_type_t::LanceDBFieldTypeScalar => {
                                let fs_lst = column_data.as_string::<i32>();
                                let binary_len : Vec<usize> = fs_lst.iter().map(|x| x.unwrap().len()).collect();
                                // println!("binary_len: {:?}", binary_len);
                                binary_size_ptr = Box::into_raw(binary_len.into_boxed_slice()) as *mut usize;
                                // println!("fs_lst: {:?}", fs_lst);
                                let mut binary_vec :Vec<*const c_char> = Vec::new();
                                for i in 0..fs_lst.len() {
                                    let single_data = fs_lst.value(i).to_string();
                                    // println!("single_data: {:?}", single_data);
                                    let raw_data = string_to_c_char_ptr(single_data);
                                    binary_vec.push(raw_data as *const c_char);
                                }
                                let binary_vector_c = Box::into_raw(binary_vec.into_boxed_slice()) as *mut *const c_char;
                                data_ptr = binary_vector_c as *mut c_void;
                            },
                            lancedb_field_type_t::LanceDBFieldTypeVector => {
                                panic!("Unsupported data type: Vec<Binary>");
                            }
                        }
                        lancedb_field_data_type_t::LanceDBFieldTypeString
                    },
                    _ => { panic!("Unsupported data type"); }
                };

                // Create field data info
                let field_data : lancedb_field_data_t = lancedb_field_data_t {
                    name: string_to_c_char_ptr(field_name.to_string()),
                    data_type: data_type,
                    field_type: field_type,
                    data_count: data_count,
                    dimension: dimension,
                    data: data_ptr,
                    binary_size: binary_size_ptr, // TODO: assign binary size
                };

                // println!("name: {:?}, data_type: {:?}, field_type: {:?}, data_count: {:?}, dimension: {:?}, data: {:?}, binary_size: {:?}",
                //          field_data.name, field_data.data_type, field_data.field_type, field_data.data_count, field_data.dimension, field_data.data, field_data.binary_size);
                field_data_vec.push(field_data);
            }


            let field_data_heap =
                Box::into_raw(field_data_vec.into_boxed_slice()) as *mut lancedb_field_data_t;
            // println!("field_data_heap: {:?}", field_data_heap);

            let rust_lance_data: lancedb_data_t = lancedb_data_t {
                fields: field_data_heap,
                num_fields: fiels_data_count,
            };

            // Assign to search_results as output
            unsafe {
                assert!(!search_results.is_null());
                *search_results = rust_lance_data;
            }

            break; // consider multiple vector search
        }

        return true;
    });

    return result;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {

    }
}
