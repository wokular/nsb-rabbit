Notes for installing AMQP-CPP:

- cd into api if not already 
- prereq of cmake installed

- git clone repo: (ssh) git clone git@github.com:CopernicaMarketingSoftware/AMQP-CPP.git amqpcpp/
-- clone it into the amqpcpp dir

- build it
-- cd amqpcpp
-- mkdir build && cd build
-- cmake .. -DAMQP-CPP_BUILD_SHARED=OFF -DAMQP-CPP_LINUX_TCP=OFF -DCMAKE_OSX_ARCHITECTURES=arm64
-- sudo cmake --build . --target install
--- might encounter a permissions error with installing the libamqpcpp.a file into /usr/local/lib, try sudo

Notes for installing dependencies for cpp simclient version:
- Use homebrew if macos/linux, or only apt for linux:
-- brew install boost openssl
or 
-- sudo apt install libboost-dev libssl-dev



- Need to use these flags -lboost_system -lssl -lcrypto -lpthread

How to compile cpp simclient

- cd to simcpp
- mkdir build && cd build
- if using macos with m1/m2:
-- cmake .. -DCMAKE_OSX_ARCHITECTURES=arm64 && make -j$(sysctl -n hw.ncpu)
- else (macos/linux):
-- cmake .. && make -j$(nproc)


- Compile program, making sure to include -lamqpcpp flag (clang/gcc)
- Need to use these flags -lboost_system -lssl -lcrypto -lpthread
-- example: g++ -g -Wall -lamqcpp my-amqp-cpp.c -o my-amqp-cpp

