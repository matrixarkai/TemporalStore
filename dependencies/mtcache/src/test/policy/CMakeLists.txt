set(SINGLE_FILE_UTS
  fifo_test
  slru_test
  arc_test
  )

foreach(SINGLE_FILE_UT ${SINGLE_FILE_UTS})
  mtcache_add_test(
    NAME ${SINGLE_FILE_UT}
    SOURCES ${SINGLE_FILE_UT}.cpp
  )
endforeach(SINGLE_FILE_UT)
