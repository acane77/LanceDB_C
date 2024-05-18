#!/bin/bash

function execute() {
  echo $*
  eval $*
  return $?
}

if [ "$1" == "android" ]; then
  CARGO_TARGET="--target aarch64-linux-android"
  NDK_PATH="/mnt/nfs/__android-ndk-r23b/clang/prebuilt/linux-x86_64"
  NDK_PREFIX="${NDK_PATH}/bin/aarch64-linux-android21-"
  export CC="${NDK_PREFIX}clang"
  export CXX="${NDK_PREFIX}clang++"
  export LD="${NDK_PATH}/bin/ld"
  export SYSROOT="$NDK_PATH/sysroot"
  export RUSTFLAGS="-C linker=$LD -C link-arg=-L$SYSROOT/usr/lib/aarch64-linux-android/23 -C link-arg=-L$NDK_PATH/lib64/clang/12.0.8/lib/linux/aarch64"
  PLATFORM_STR=aarch64-linux-android
  PLATFORM="android-aarch64"
else
  export CC="gcc"
  export CXX="g++"
  PLATFORM_STR=.
  PLATFORM="linux-x86_64"
fi

echo "-- Compile the RUST library"
execute cargo build --release $CARGO_TARGET
if [ $? -ne 0 ]; then
  exit $?
fi

rm -rf build/$PLATFORM
mkdir -p build/$PLATFORM
cp target/$PLATFORM_STR/release/liblancedb_c.a build/$PLATFORM/liblancedb_c.a

CXX_SRC="samples/sample_lancedb.cpp"
echo "-- Compile the sample"
if [ "$1" == "android" ]; then
  execute $CXX -c src/impl.cpp -o lancedb_c_impl.o -Iinclude
  execute $CXX $CXX_SRC lancedb_c_impl.o -Iinclude -Ltarget/$PLATFORM_STR/release -llancedb_c -o c_sample -ldl
else
  execute $CXX -c src/impl.cpp -o lancedb_c_impl.o -Iinclude
  execute $CXX $CXX_SRC lancedb_c_impl.o -Iinclude -Ltarget/$PLATFORM_STR/release -llancedb_c -o c_sample -lpthread -ldl
fi

if [ $? -ne 0 ]; then
  exit $?
fi

if [ "$1" == "android" ]; then
  echo "-- Run the sample on Android"
  adb shell mkdir -p /data/local/tmp/test_lancedb
  adb push c_sample /data/local/tmp/test_lancedb/
  adb shell "chmod 777 /data/local/tmp/test_lancedb/c_sample"
  adb shell <<EOF
    cd /data/local/tmp/test_lancedb
    rm -rf test.db
    export LD_LIBRARY_PATH=.
    ./c_sample
EOF
else
  echo "-- Run the sample"
  rm -rf test*.db
  export LD_LIBRARY_PATH=target/release
  execute ./c_sample
fi