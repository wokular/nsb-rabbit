Notes for installing rabbitmq-c:

- cd into api if not already 
- prereq of cmake installed
git clone git@github.com:alanxz/rabbitmq-c.git rabbitmq-c/

- build it
cd rabbitmq-c && mkdir build && cd build
cmake .. -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DENABLE_SSL_SUPPORT=OFF
cmake --build .

- install it
make
sudo make install

- test it works
- start rabbitmq docker service, ensuring port 5672
- compile a listener
cd ../examples/
gcc -o amqp_listen amqp_listen.c utils.c -I../librabbitmq -L../build/librabbitmq -lrabbitmq -arch arm64
./amqp_listen localhost 5672 amq.direct test
- open a new terminal window in examples/
gcc -o amqp_sendstring amqp_sendstring.c utils.c -I../librabbitmq -L../build/librabbitmq -lrabbitmq -arch arm64
./amqp_sendstring localhost 5672 amq.direct test "hello world"
- other terminal window should print hello world


Notes for installing SimpleAmqpClient:

- cd into api if not alreaedy
- clone the repo
git clone git@github.com:nsb-ucsc/SimpleAmqpClient-Updated.git amqpclient/

- install prereqs (primarily boost)
macos via homebrew: brew install boost
windows via vcpkg: vcpkg install boost
ubuntu/debian via apt: sudo apt update && sudo apt install libboost-all-dev

- start build process
cd amqpclient
mkdir build && cd build
cmake .. 
make
sudo make install

- compile the test file (make sure your boost include path is correct, as well as your lib path. this works on macos)
g++ -std=c++17 -o simple_amqp_test simpleamqpclient_test.cpp \
    -I/opt/homebrew/Cellar/boost/1.87.0/include \
    -L/opt/homebrew/lib \
    -lSimpleAmqpClient -lboost_system -lboost_filesystem -lrabbitmq

./simple_amqp_test

- If the above does not work, debug it as necessary, but it worked fully up to this point for me



- compiling the simclientlib.cpp file for testing

- compile SimClient.cpp as static library
g++ -std=c++17 -c SimClient.cpp \
    -I/opt/homebrew/Cellar/boost/1.87.0/include
ar rcs libSimClient.a SimClient.o

- compile a test file, using necessary linkers
g++ -std=c++17 -o test_simclient test_simclient.cpp \
    -L. -lSimClient \
    -I/opt/homebrew/Cellar/boost/1.87.0/include \
    -L/opt/homebrew/lib \
    -lSimpleAmqpClient -lboost_system -lboost_filesystem -lrabbitmq


(Same as above but protobuf version)
- compile the protobuf headers: protoc --cpp_out=. nsb_payload.proto 

g++ -std=c++17 -c SimClient.cpp api/nsb_payload.pb.cc \
    -Iapi/ -I/opt/homebrew/include \
    -L/opt/homebrew/lib \
    -lprotobuf
ar rcs libSimClient.a SimClient.o nsb_payload.pb.o

g++ -std=c++17 -o RunSimClient RunSimClient.cpp \
    -L. -lSimClient \
    -Iapi/ -I/opt/homebrew/include \
    -L/opt/homebrew/lib \
    -lSimpleAmqpClient -lboost_system -lboost_filesystem -lrabbitmq -lprotobuf




OR use the cmake (Recommended)
cd api
protoc --cpp_out=. nsb_payload.proto 
mkdir -p build && cd build
cmake ..
make
./RunSimClient  # Run the compiled test executable

Note: I had issues with protobuf + abseil, so I had to rebuild protobuf from source:
brew uninstall --ignore-dependencies protobuf
brew install --build-from-source protobuf
- I also updated the CMake to include absl:: namespace, which is alr in the existing Cmake