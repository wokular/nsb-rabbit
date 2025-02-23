
##  Notes For Building/Using C++ RabbitMQ NSB Simulator Client

### Setting up rabbitmq-c
- cd into api
- prereq of cmake installed

Install the community RabbitMQ C-edition into your api/ directory, we will need it for the C++ wrapper later on.

`git clone git@github.com:alanxz/rabbitmq-c.git rabbitmq-c/`

Build it:
`cd rabbitmq-c && mkdir build && cd build`

`cmake .. -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DENABLE_SSL_SUPPORT=OFF`

`cmake --build .` 

Install it:
`make`

`sudo make install`

  

Test it works:
- Start RabbitMQ daemon, ensuring on port 5672 (default)
- Compile a listener test from provided library:

	(in rabbitmq-c dir)
	`cd ../examples/`

	`gcc -o amqp_listen amqp_listen.c utils.c -I../librabbitmq -L../build/librabbitmq -lrabbitmq -arch arm64`
	where -arch (arm64) is your architecture.
	
	I'm on M2 Mac, so `arm64` is mine. 
	TODO: test on other archs

- Let's run the listener test on port 5672 (RabbitMQ port)
	`./amqp_listen localhost 5672 amq.direct test`

- Compile and run the sender test:
	- Open a new terminal window in `examples/` dir

	`gcc -o amqp_sendstring amqp_sendstring.c utils.c -I../librabbitmq -L../build/librabbitmq -lrabbitmq -arch arm64`

	`./amqp_sendstring localhost 5672 amq.direct test "hello world"`

	The first terminal window (listener test) should print hello world

  
  

## Installing SimpleAmqpClient (C++ wrapper):

  

- cd into api if not already
- clone the repo
`git clone git@github.com:nsb-ucsc/SimpleAmqpClient-Updated.git amqpclient/`
- install prereqs (primarily boost):
	-	macos via homebrew: `brew install boost`
	-	windows via vcpkg: `vcpkg install boost`
	-  ubuntu/debian via apt: `sudo apt install libboost-all-dev`

#### Start build process
`cd amqpclient`

`mkdir build && cd build`

`cmake ..`

`make`

`sudo make install`

  

Compile the test file found in the `api/` directory (make sure your boost include path is correct, as well as your lib path. this works on macos):

```
g++ -std=c++17 -o simple_amqp_test simpleamqpclient_test.cpp \

-I/opt/homebrew/Cellar/boost/1.87.0/include \

-L/opt/homebrew/lib \

-lSimpleAmqpClient -lboost_system -lboost_filesystem -lrabbitmq
```
  Run the test:
`./simple_amqp_test`

  

- If the above does not work, debug it as necessary, but it worked fully up to this point for me
-
## Building/using the RunSimClient file (main test usage for SimClient.cpp)

You can:
- A) Use CMake to build the makefile, then run the makefile to build (Recommended)
- Or B) Manually build the static SimClient library, linking all necessary libraries yourself

#### Using CMake (Recommended)
I wrote out the CMakeLists.txt file so compiling and running everything will be as smooth as possible. This assumes CMake, protobuf, abseil (see below), and the above libraries have been installed correctly and work with all the tests up to this point.

Install abseil (necessary for protobuf C++):
macOS via Homebrew: `brew install abseil
`

Then:

	cd api

	protoc --cpp_out=. nsb_payload.proto

	mkdir -p build && cd build

	cmake ..

	make

`./RunSimClient`  will run the compiled test executable, giving you a working SimClient C++ equivalent. Then you can run the NodeClient python file as usual to test.

Note: I had issues with protobuf + abseil, so I had to rebuild protobuf from source which was a pain but necessary on my system. It might not be necessary for your system, so unless you're getting errors related to absl/abseil, I don't recommend going about this way of installing protobuf + abseil.

An error might look like 
```
absl::lts_20240722::log_internal::LogMessage::CopyToEncodedBuffer<(absl::lts_20240722::log_internal::LogMessage::StringType)0>(std::__1::basic_string_view<char, std::__1::char_traits<char>>)", referenced from: absl::lts_20240722::log_internal::LogMessage& absl::lts_20240722::log_internal::LogMessage::operator<<<19>(char const (&) [19]) in libSimClient.a(SimClient.cpp.o) absl::lts_20240722::log_internal::LogMessage& absl::lts_20240722::log_internal::LogMessage::operator<<<19>(char const (&) [19]) in libSimClient.a(nsb_payload.pb.cc.o) "absl::lts_20240722::log_internal::LogMessage& absl::lts_20240722::log_internal::LogMessage::operator<<<unsigned long, 0>(unsigned long const&)", referenced from:
```
so if you're getting an error with the absl:: namespace and you've chagpt'd to debug it (make sure abseil is installed, and that protobuf was built with it), try 

`brew uninstall --ignore-dependencies protobuf`

`brew install --build-from-source protobuf`

- A simple way to test if protobuf is installed correctly with abseil linked then is 
	- 	`otool -L /opt/homebrew/lib/libprotobuf.dylib | grep absl
` to make sure that abseil is installed with protobuf and linked together. If the output is empty, something's broken.


#### Compile SimClient.cpp as a static library method

Compile the .a library file of SimClient:

- Compile the protobuf headers: `protoc --cpp_out=. nsb_payload.proto`
- Compile the .a static library file
```
g++ -std=c++17 -c SimClient.cpp api/nsb_payload.pb.cc \

-Iapi/ -I/opt/homebrew/include \

-L/opt/homebrew/lib \

-lprotobuf

ar rcs libSimClient.a SimClient.o nsb_payload.pb.o
```
 Now compile the RunSimClient test
 ```
g++ -std=c++17 -o RunSimClient RunSimClient.cpp \

-L. -lSimClient \

-Iapi/ -I/opt/homebrew/include \

-L/opt/homebrew/lib \

-lSimpleAmqpClient -lboost_system -lboost_filesystem -lrabbitmq -lprotobuf
```
  

## Footnotes
The installation process was quite difficult, but the main problems I had were with inproper boost/protobuf/abseil installation, as well as finding the appropriate directories where the compiled libraries (rabbitmq-c, libsimpleamqpclient, libsimclient) were being kept. I've provided the CMake for compiling RunSimClient easily, and this instruction guide to help streamline the process as much as possible. The installation steps above were all performed on an M2 Macbook on Ventura (13.5.1), so Windows + Linux steps for installation will differ. In the future, I'm planning to write installation steps for each platform.

\- Alex Woelkers