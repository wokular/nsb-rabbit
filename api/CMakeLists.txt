cmake_minimum_required(VERSION 3.10)
project(SimClientProject)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define paths to Boost, RabbitMQ, and Protobuf
set(BOOST_INCLUDE_DIR /opt/homebrew/Cellar/boost/1.87.0/include)
set(BOOST_LIB_DIR /opt/homebrew/lib)
set(PROTOBUF_INCLUDE_DIR /opt/homebrew/include)
set(PROTOBUF_LIB_DIR /opt/homebrew/lib)

# Define paths to your manually built SimpleAmqpClient
set(SIMPLEAMQPCLIENT_INCLUDE_DIR /usr/local/include)
set(SIMPLEAMQPCLIENT_LIB_DIR /usr/local/lib)

# Find Protobuf package
find_package(Protobuf REQUIRED)
find_package(absl REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})
link_directories(${PROTOBUF_LIB_DIR})

# Include directories
include_directories(${BOOST_INCLUDE_DIR} ${SIMPLEAMQPCLIENT_INCLUDE_DIR})

# Linker search paths
link_directories(${BOOST_LIB_DIR} ${SIMPLEAMQPCLIENT_LIB_DIR})

# Generate Protobuf files
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS nsb_payload.proto)

# Add the static library target
add_library(SimClient STATIC SimClient.cpp ${PROTO_SRCS})

# Specify library output location
set_target_properties(SimClient PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Link required libraries for SimClient
target_link_libraries(SimClient PRIVATE 
    boost_system 
    boost_filesystem 
    rabbitmq 
    protobuf
    absl::strings
    absl::base
    absl::status
    absl::synchronization
    absl::log_initialize   # Explicitly include Abseil logging components
    absl::log_internal_check_op
    absl::log_internal_conditions
    absl::log_internal_fnmatch
    absl::log_internal_format
    absl::log_internal_globals
    absl::log_internal_log_sink_set
    absl::log_internal_message
    absl::log_internal_nullguard
    absl::log_internal_proto
)


# Add the executable target
add_executable(RunSimClient RunSimClient.cpp)

target_link_libraries(RunSimClient PRIVATE
    SimClient
    ${SIMPLEAMQPCLIENT_LIB_DIR}/libSimpleAmqpClient.dylib
    boost_system
    boost_filesystem
    rabbitmq
    protobuf
    absl::strings
    absl::base
    absl::status
    absl::synchronization
    absl::log_initialize
    absl::log_internal_check_op
    absl::log_internal_conditions
    absl::log_internal_fnmatch
    absl::log_internal_format
    absl::log_internal_globals
    absl::log_internal_log_sink_set
    absl::log_internal_message
    absl::log_internal_nullguard
    absl::log_internal_proto
)


# Set rpath for runtime linking
set(CMAKE_INSTALL_RPATH ${SIMPLEAMQPCLIENT_LIB_DIR})
