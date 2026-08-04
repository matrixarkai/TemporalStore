#!/usr/bin/env bash
set -euo pipefail

TS_ROOT="/opt/github-services/TemporalStore"
WORKDIR="$TS_ROOT/crates/temporalstore-rust"
MAIN_FILE="$WORKDIR/src/lib.rs"

cd "$TS_ROOT"

if command -v ctags >/dev/null 2>&1 && command -v cscope >/dev/null 2>&1; then
  find crates/temporalstore-rust crates/temporalstore-snapshot \
    -type f \( -name '*.rs' -o -name '*.toml' \) | sort > cscope.files
  ctags -R --languages=Rust --exclude=target --exclude=.git \
    -f tags crates/temporalstore-rust crates/temporalstore-snapshot
  cscope -b -q -k -i cscope.files
fi

cd "$WORKDIR"

VIM_SETUP="$(mktemp /tmp/temporalstore-vim-setup.XXXXXX.vim)"
trap 'rm -f "$VIM_SETUP"' EXIT

cat > "$VIM_SETUP" <<VIMRC
set tags=$TS_ROOT/tags,tags
set path=.,$TS_ROOT/crates/temporalstore-rust/src/**,$TS_ROOT/crates/temporalstore-snapshot/src/**
set suffixesadd=.rs,/mod.rs
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
