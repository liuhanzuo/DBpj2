add_library(
    babydb_execution
    OBJECT
    hash_join_operator.cpp
    value_operator.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:babydb_execution>
    PARENT_SCOPE)
