cmake_minimum_required(VERSION 3.17)
project(hiredis-vip C)

set(CMAKE_C_STANDARD 11)

set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS} -O2")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS} -O0 -Wall -g -ggdb")

IF (WIN32)
    MESSAGE(STATUS "Now is windows")
ELSEIF (APPLE)
    MESSAGE(STATUS "Now is Apple systens.")
ELSEIF (UNIX)
    ADD_DEFINITIONS(-D_XOPEN_SOURCE=500)
ENDIF ()

include_directories("/usr/local/include")
link_directories("/usr/local/lib")
link_directories("/usr/lib64")
link_directories("/usr/lib")
include_directories(.)

IF (0)
    add_executable(${PROJECT_NAME}
            testCluster.c
            adapters/ae.h
            adapters/glib.h
            adapters/libev.h
            adapters/libevent.h
            adapters/libuv.h
            adlist.c
            adlist.h
            async.c
            async.h
            command.c
            command.h
            crc16.c
            dict.c
            dict.h
            fmacros.h
            hiarray.c
            hiarray.h
            hircluster.c
            hircluster.h
            hiredis.c
            hiredis.h
            hiutil.c
            hiutil.h
            net.c
            net.h
            read.c
            read.h
            sds.c
            sds.h
            sdsalloc.h
            sockcompat.c
            sockcompat.h
            alloc.c
            alloc.h
            win32.h)
ELSE ()
    add_library(${PROJECT_NAME} STATIC
            testCluster.c
            adapters/ae.h
            adapters/glib.h
            adapters/libev.h
            adapters/libevent.h
            adapters/libuv.h
            adlist.c
            adlist.h
            async.c
            async.h
            command.c
            command.h
            crc16.c
            dict.c
            dict.h
            fmacros.h
            hiarray.c
            hiarray.h
            hircluster.c
            hircluster.h
            hiredis.c
            hiredis.h
            hiutil.c
            hiutil.h
            net.c
            net.h
            read.c
            read.h
            sds.c
            sds.h
            sdsalloc.h
            sockcompat.c
            sockcompat.h
            alloc.c
            alloc.h
            win32.h)
ENDIF ()

IF (APPLE)
    include_directories("/usr/local/include/mysql")
    include_directories("/usr/local/Cellar/curl/7.66.0/lib")
    target_link_libraries(${PROJECT_NAME} curl iconv event z)
ELSEIF (UNIX)
    target_link_libraries(${PROJECT_NAME} curl uuid event z)
ENDIF ()