add_subdirectory(storage)

add_library(babydb STATIC ${ALL_OBJECT_FILES})

set(BABYDB_LIBS
        babydb_storage
        )

target_link_libraries(
        babydb
        ${BABYDB_LIBS})

target_include_directories(
        babydb PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
