Prerequisites for compiling 'librdkafka' on Windows
---------------------------------------------------

* 'Visual Studio Community 2013', download from:
  https://www.visualstudio.com/en-us/news/releasenotes/vs2013-community-vs

* 'nuget.exe', download from:
  https://dist.nuget.org/index.html

* 'Win32 OpenSSL v1.0.2k' and 'Win64 OpenSSL v1.0.2k', download from:
  https://slproweb.com/products/Win32OpenSSL.html
  NOTE: Do not change default installation directories since librdkafka's build
        script relies on them ('C:\OpenSSL-Win32' and 'C:\OpenSSL-Win64')

* 'librdkafka' source code. Currently, it is added as submodule of this
  repository at version 0.9.4.



Patching 'librdkafka'
---------------------

Functions 'rd_kafka_conf_properties_show' and 'rd_kafka_dump' use a C-style FILE
pointer as parameter in order to dump some information to filesystem. This is
problematic to use from Delphi, since a FILE pointer is obtained from Visual C
runtime library through function 'fopen'.

To workaround this issue, functions 'rd_kafka_conf_properties_show2' and
'rd_kafka_dump2' have been added. These functions expect 'fopen' parameters,
ie. 'filename' and 'mode'.

In order to make these functions available in the Delphi wrapper, unit
'rdkafka.pas' should be compiled by defining macro
'KAFKA_DELPHI_USE_PATCHED_LIBRDKAFKA'.

Run in a bash console script '%KafkaDelphi_repo%\patches\patch_librdkafka.sh'
for patching 'librdkafka'.



Compiling 'librdkafka' on Windows
---------------------------------

Open 'Developer Command Prompt for VS2013' console and type following commands:
* cd %KafkaDelphi_repo%\externals\librdkafka
* nuget.exe restore win32\librdkafka.sln
* cd win32
* build.bat

Once compilation has finished, the DLL are available at
'%KafkaDelphi_repo%\librdkafka\win32\outdir\v120\[Win32|x64]\[Debug\Release]'.



Compiling 'librdkafka' on Debian/Ubuntu
---------------------------------------

Currently, Debian/Ubuntu repositories contain librdkafka 0.9.3. So, we can get
source code from github.
$ git clone https://github.com/edenhill/librdkafka.git
$ cd librdkafka
$ git checkout v0.9.4
$ cd ..

Next, we can ask APT for its build dependencies:
$ sudo apt-get build-dep librdkafka1

CMake is required in order to build (well, you can use alchemist configure
sciprt, but...):
$ sudo apt-get install cmake

It is not required, but highly recommended to create a 'build' directory, which
will contain make scripts and generated artifacts:
$ mkdir librdkafka_build
$ cd librdkafka_build
$ cmake -DCMAKE_BUILD_TYPE=Release ../librdkafka
$ make
