add_library(
    babydb_execution
    OBJECT
    expression/filter.cpp
    hash_join_operator.cpp
    seq_scan_operator.cpp
    insert_operator.cpp
    value_operator.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:babydb_execution>
    PARENT_SCOPE)
