#############
# FUNCTIONS #
#############

# Returns all header files in the current directory, recursing into sub-directories.
function (get_headers DIRECTORY RESULT)
  file (GLOB_RECURSE HEADER_FILES_TMP "${DIRECTORY}/*.h" "${DIRECTORY}/*.hpp" "${DIRECTORY}/*.inl" "${DIRECTORY}/*.pch" "${DIRECTORY}/*.tsd" "${DIRECTORY}/*.tsp")
  set (${RESULT} ${HEADER_FILES_TMP} PARENT_SCOPE)
endfunction ()

# Returns all source files in the current directory, recursing into sub-directories
function (get_sources DIRECTORY RESULT)
  file (GLOB_RECURSE SOURCE_FILES_TMP "${DIRECTORY}/*.c" "${DIRECTORY}/*.cpp")
  set (${RESULT} ${SOURCE_FILES_TMP} PARENT_SCOPE)
endfunction ()

################
# Build script #
################

cmake_minimum_required (VERSION 2.8)
project (jace)

set (jace_VERSION_MAJOR 1)
set (jace_VERSION_MINOR 2)
add_definitions (-DJACE_EXPORTS -DJACE_PROXY_EXPORTS)

set (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MP")
IF(CMAKE_COMPILER_IS_GNUCXX)
	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
ENDIF(CMAKE_COMPILER_IS_GNUCXX)

if (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  include_directories (include $ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/win32 $ENV{BOOST_HOME})
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  include_directories (include $ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/linux $ENV{BOOST_HOME})
endif ()

file (TO_CMAKE_PATH "$ENV{BOOST_HOME}/lib" BOOST_LIBRARY)
link_directories (${BOOST_LIBRARY})

get_headers ("${CMAKE_CURRENT_SOURCE_DIR}/include" HEADER_FILES)
get_sources ("${CMAKE_CURRENT_SOURCE_DIR}/source" SOURCE_FILES)

add_library (jace SHARED ${SOURCE_FILES} ${HEADER_FILES})
