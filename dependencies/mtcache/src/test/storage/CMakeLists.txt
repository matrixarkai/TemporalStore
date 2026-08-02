set(SINGLE_FILE_UTS
  simple_storage_test
  dram_storage_test
  pmem_storage_test
  gc_threadpool_test
  pmem_dispatcher_test
  pmem_recover_test
  )

if (BUILD_SSD_CACHE)
  list(APPEND SINGLE_FILE_UTS
    ssd_test
    multi_ssd_test
    terarkdb_test
    codec_test
    buffer_manager_test
    zoned_store_index_test
    zoned_store_metrics_test
    zoned_store_recovery_test
    zoned_store_test
    zone_manager_large_mode_test
    zone_store_gc_test
    )
endif()

foreach(SINGLE_FILE_UT ${SINGLE_FILE_UTS})
  mtcache_add_test(
    NAME ${SINGLE_FILE_UT}
    SOURCES ${SINGLE_FILE_UT}.cpp
  )
endforeach(SINGLE_FILE_UT)

if (BUILD_SSD_CACHE)
  target_include_directories(ssd_test PRIVATE ${TERARKDB_INCLUDE_DIR})
  target_include_directories(multi_ssd_test PRIVATE ${TERARKDB_INCLUDE_DIR})
  target_include_directories(terarkdb_test PRIVATE ${TERARKDB_INCLUDE_DIR})
endif()
