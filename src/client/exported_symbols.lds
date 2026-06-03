{
  global:
    temporalstore_*;
    extern "C++" {
        bcache2::swig::*;
        bcache2_*;
    };
  local: *;
};
