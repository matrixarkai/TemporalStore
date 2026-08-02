#!/usr/bin/env bash
set -euo pipefail

TS_ROOT="/opt/github-services/TemporalStore"
TARGET="${1:-cpp}"

if [ "$TARGET" = "cpp" ] || [ "$TARGET" = "c++" ] || [ "$TARGET" = "cppsrc" ]; then
  WORKDIR="$TS_ROOT/src"
  MAIN_FILE="$WORKDIR/main.cc"
elif [ "$TARGET" = "rust" ] || [ "$TARGET" = "rs" ] || [ "$TARGET" = "rustsrc" ]; then
  WORKDIR="$TS_ROOT/crates/temporalstore-rust"
  MAIN_FILE="$WORKDIR/src/lib.rs"
else
  echo "usage: $0 [cpp|rust]" >&2
  exit 1
fi

cd "$TS_ROOT"

if command -v ctags >/dev/null 2>&1 && command -v cscope >/dev/null 2>&1; then
  if [ "$TARGET" = "rust" ] || [ "$TARGET" = "rs" ] || [ "$TARGET" = "rustsrc" ]; then
    find crates/temporalstore-rust crates/temporalstore-snapshot \
      -type f \( -name '*.rs' -o -name '*.toml' \) | sort > cscope.files
    ctags -R --languages=Rust --exclude=target --exclude=.git \
      -f tags crates/temporalstore-rust crates/temporalstore-snapshot
  else
    find src include tools \
      -type f \( -name '*.cc' -o -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) 2>/dev/null | sort > cscope.files
    ctags -R --languages=C++ --exclude=build-ubuntu22 --exclude=output-ubuntu22 --exclude=.git \
      -f tags src include tools 2>/dev/null || true
  fi
  cscope -b -q -k -i cscope.files
fi

cd "$WORKDIR"

VIM_SETUP="$(mktemp /tmp/temporalstore-vim-setup.XXXXXX.vim)"
trap 'rm -f "$VIM_SETUP"' EXIT

cat > "$VIM_SETUP" <<VIMRC
set tags=$TS_ROOT/tags,tags
set path=.,$TS_ROOT/crates/temporalstore-rust/src/**,$TS_ROOT/crates/temporalstore-snapshot/src/**,$TS_ROOT/src/**
set suffixesadd=.rs,/mod.rs,.cc,.h
set cscopequickfix=s-,c-,d-,i-,t-,e-
silent! cscope kill -1
silent! cscope add $TS_ROOT/cscope.out $TS_ROOT
runtime! plugin/taglist.vim
let Tlist_Ctags_Cmd='/usr/bin/ctags'
let Tlist_Show_One_File=1
let Tlist_Exit_OnlyWindow=1
silent! TlistOpen
wincmd p
VIMRC

vim -Nu NONE -U NONE -N -n -S "$VIM_SETUP" "$MAIN_FILE"
