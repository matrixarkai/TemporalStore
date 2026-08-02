set(SINGLE_FILE_UTS
  je_allocator_test
  log_allocator_test
  pool_allocator_test
  simple_allocator_test
  pmem_allocator_test
  )

foreach(SINGLE_FILE_UT ${SINGLE_FILE_UTS})
  mtcache_add_test(
    NAME ${SINGLE_FILE_UT}
    SOURCES ${SINGLE_FILE_UT}.cpp
  )
endforeach(SINGLE_FILE_UT)
