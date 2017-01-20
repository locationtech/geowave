#############
# FUNCTIONS #
#############

# Returns all header files in the current directory, recursing into sub-directories.
FUNCTION (get_headers DIRECTORY RESULT)
	FILE (GLOB_RECURSE HEADER_FILES_TMP "${DIRECTORY}/*.h" "${DIRECTORY}/*.hpp" "${DIRECTORY}/*.inl" "${DIRECTORY}/*.pch" "${DIRECTORY}/*.tsd" "${DIRECTORY}/*.tsp")
	SET (${RESULT} ${HEADER_FILES_TMP} PARENT_SCOPE)
ENDFUNCTION ()

# Returns all source files in the current directory, recursing into sub-directories
FUNCTION (get_sources DIRECTORY RESULT)
	FILE (GLOB_RECURSE SOURCE_FILES_TMP "${DIRECTORY}/*.c" "${DIRECTORY}/*.cpp")
	SET (${RESULT} ${SOURCE_FILES_TMP} PARENT_SCOPE)
ENDFUNCTION ()

################
# Build script #
################

CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT (geowave)

SET (geowave_VERSION_MAJOR 0)
SET (geowave_VERSION_MINOR 9)
SET (geowave_VERSION_PATCH 1)
ADD_DEFINITIONS (-DJACE_EXPORTS -DBOOST_ALL_NO_LIB)

IF (CMAKE_COMPILER_IS_GNUCXX)
	SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
ENDIF (CMAKE_COMPILER_IS_GNUCXX)

find_package(JNI)
find_package(Boost REQUIRED COMPONENTS system thread)

INCLUDE_DIRECTORIES (include ${JNI_INCLUDE_DIRS} ${Boost_INCLUDE_DIRS} "${PROJECT_BINARY_DIR}/../proxies/include")

get_headers ("${CMAKE_CURRENT_SOURCE_DIR}/include" HEADER_FILES)
get_sources ("${CMAKE_CURRENT_SOURCE_DIR}/source" SOURCE_FILES)

ADD_LIBRARY (geowave ${SOURCE_FILES} ${HEADER_FILES})

#-----------------------------------------------------------------------------
#
# To fix compilation problem: relocation R_X86_64_32 against `a local symbol' can not be
# used when making a shared object; recompile with -fPIC
# See http://www.cmake.org/pipermail/cmake/2007-May/014350.html
#
IF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )
  SET_TARGET_PROPERTIES(${PROJECT_NAME} PROPERTIES COMPILE_FLAGS "-fPIC")
ENDIF( CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" )

TARGET_LINK_LIBRARIES (geowave ${JNI_LIBRARIES} ${Boost_LIBRARIES})

install(TARGETS geowave DESTINATION lib)
install(DIRECTORY include/jace DESTINATION include)
