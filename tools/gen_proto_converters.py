#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Generate protobuf messages and Rust converters for serde types.

A durable file whose content is a serde type should be a protobuf message, not JSON and not
MessagePack. Hand-mapping hundreds of fields across a dozen nested structs invites a dropped or
misplaced field, so this reads the Rust definitions and emits both halves:

  * `.proto` messages, one per struct / enum, between markers in the proto file;
  * `<type>_to_proto` / `<type>_from_proto` functions, where every struct is DESTRUCTURED WITHOUT
    `..`, so a field added to a Rust struct fails to compile until the converter is regenerated.

It refuses shapes it cannot map exactly (flatten, untagged, serde_json::Value, arrays, generics,
foreign types) and says which, rather than approximating.

usage: gen_proto_converters.py --repo DIR --prefix Mf --roots A,B,C --proto-marker NAME \
           --out-rust crates/temporalstore-rust/src/x_proto.rs [--report]
"""
import argparse
import io
import os
import re
import sys

SRC = "crates/temporalstore-rust/src"
PROTO = "proto/temporalstore/v1/temporalstore.proto"

PRIMS = {"u8", "u16", "u32", "u64", "usize", "i8", "i16", "i32", "i64", "isize", "f32", "f64", "bool"}
PROTO_SCALAR = {
    "u8": "uint32", "u16": "uint32", "u32": "uint32", "u64": "uint64", "usize": "uint64",
    "i8": "int32", "i16": "int32", "i32": "int32", "i64": "int64", "isize": "int64",
    "f32": "float", "f64": "double", "bool": "bool", "string": "string", "bytes": "bytes",
}
MAP_KEYS = {"u8", "u16", "u32", "u64", "usize", "i8", "i16", "i32", "i64", "isize", "bool", "string"}
KEYWORDS = {"type", "ref", "match", "mod", "move", "box", "loop", "use", "fn", "impl", "in", "as", "where", "async", "await"}


class Unsupported(Exception):
    pass


# --------------------------------------------------------------------------- parsing

def strip_comments(text):
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            out.append(text[i:j + 1]); i = j + 1
        elif text.startswith("//", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            j = text.find("*/", i)
            i = n if j < 0 else j + 2
        else:
            out.append(c); i += 1
    return "".join(out)


def match_close(text, i, open_c, close_c):
    depth = 0
    for j in range(i, len(text)):
        if text[j] == open_c:
            depth += 1
        elif text[j] == close_c:
            depth -= 1
            if depth == 0:
                return j
    raise Unsupported("unbalanced " + open_c)


def split_top(s, sep=","):
    parts, depth, cur = [], 0, []
    for c in s:
        if c in "<([{":
            depth += 1
        elif c in ">)]}":
            depth -= 1
        if c == sep and depth == 0:
            parts.append("".join(cur)); cur = []
        else:
            cur.append(c)
    last = "".join(cur)
    if last.strip():
        parts.append(last)
    return [p for p in parts if p.strip()]


ATTR = re.compile(r"#\[(?:[^\[\]]|\[[^\]]*\])*\]\s*")


def take_attrs(s):
    attrs = []
    s = s.lstrip()
    while s.startswith("#["):
        m = ATTR.match(s)
        if not m:
            break
        attrs.append(m.group(0).strip())
        s = s[m.end():].lstrip()
    return attrs, s


class Def:
    def __init__(self, name, kind, path, attrs):
        self.name, self.kind, self.path, self.attrs = name, kind, path, attrs
        self.fields = []     # struct: [(name, type_str, attrs)] ; tuple struct: [(None, type_str, attrs)]
        self.variants = []   # enum: [(name, 'unit'|'tuple'|'struct', [(fname, type_str, attrs)])]
        self.generics = False


ITEM = re.compile(
    r"(?P<attrs>(?:[ \t]*#\[(?:[^\[\]]|\[[^\]]*\])*\][ \t]*\n)*)[ \t]*"
    r"(?:pub(?:\([^)]*\))?[ \t]+)?(?P<kind>struct|enum)[ \t]+(?P<name>[A-Z]\w*)"
)
ALIAS = re.compile(r"(?:pub(?:\([^)]*\))?\s+)?type\s+(\w+)\s*=\s*([^;]+);")


def module_path(rel):
    rel = rel[:-3] if rel.endswith(".rs") else rel
    parts = rel.split(os.sep)
    if parts[-1] == "mod":
        parts = parts[:-1]
    return "crate::" + "::".join(parts)


def parse_item(text, m, d, defs):
    """Fill `d` from the item that starts at match `m`; raise Unsupported when it cannot be read."""
    i = m.end()
    rest = text[i:]
    stripped = rest.lstrip()
    j = i + (len(rest) - len(stripped))
    if stripped.startswith("<") or text.startswith("where", j):
        d.generics = True
        return True
    if j >= len(text):
        return False
    if text[j] == "{":
        k = match_close(text, j, "{", "}")
        body = text[j + 1:k]
        if d.kind == "struct":
            for piece in split_top(body):
                fattrs, p = take_attrs(piece)
                p = re.sub(r"^pub(?:\([^)]*\))?\s+", "", p)
                mm = re.match(r"^(\w+)\s*:\s*(.+)$", p, re.S)
                if not mm:
                    raise Unsupported("%s: cannot parse field %r" % (d.name, piece[:60]))
                d.fields.append((mm.group(1), mm.group(2).strip(), fattrs))
        else:
            for piece in split_top(body):
                vattrs, p = take_attrs(piece)
                p = p.strip()
                mm = re.match(r"^(\w+)\s*(.*)$", p, re.S)
                if not mm:
                    raise Unsupported("%s: cannot parse variant %r" % (d.name, piece[:60]))
                name, tail = mm.group(1), mm.group(2).strip()
                if tail.startswith("("):
                    inner = tail[1:match_close(tail, 0, "(", ")")]
                    fields = [(None, t.strip(), []) for t in split_top(inner)]
                    d.variants.append((name, "tuple", fields, vattrs))
                elif tail.startswith("{"):
                    inner = tail[1:match_close(tail, 0, "{", "}")]
                    fields = []
                    for fp in split_top(inner):
                        fa, fp2 = take_attrs(fp)
                        fp2 = re.sub(r"^pub(?:\([^)]*\))?\s+", "", fp2)
                        m3 = re.match(r"^(\w+)\s*:\s*(.+)$", fp2, re.S)
                        if not m3:
                            raise Unsupported("%s::%s: cannot parse field" % (d.name, name))
                        fields.append((m3.group(1), m3.group(2).strip(), fa))
                    d.variants.append((name, "struct", fields, vattrs))
                else:
                    d.variants.append((name, "unit", [], vattrs))
        return True
    if text[j] == "(" and d.kind == "struct":
        k = match_close(text, j, "(", ")")
        for t in split_top(text[j + 1:k]):
            fattrs, p = take_attrs(t)
            p = re.sub(r"^pub(?:\([^)]*\))?\s+", "", p)
            d.fields.append((None, p.strip(), fattrs))
        return True
    return False


def index_crate(repo):
    defs, aliases = {}, {}
    root = os.path.join(repo, SRC)
    for dp, dn, fn in os.walk(root):
        dn[:] = [d for d in dn if d not in ("tests", "target")]
        for f in fn:
            if not f.endswith(".rs") or f.endswith("_tests.rs") or f.endswith("_test.rs"):
                continue
            full = os.path.join(dp, f)
            rel = os.path.relpath(full, root)
            text = strip_comments(io.open(full, encoding="utf-8", errors="replace").read())
            for m in ALIAS.finditer(text):
                aliases.setdefault(m.group(1), m.group(2).strip())
            for m in ITEM.finditer(text):
                attrs = [a.strip() for a in re.findall(r"#\[(?:[^\[\]]|\[[^\]]*\])*\]", m.group("attrs"))]
                if any("cfg(test)" in a for a in attrs):
                    continue
                d = Def(m.group("name"), m.group("kind"), module_path(rel), attrs)
                d.broken = None
                try:
                    if not parse_item(text, m, d, defs):
                        continue
                except Unsupported as ex:
                    d.broken = str(ex)
                defs.setdefault(d.name, []).append(d)
    return defs, aliases


# --------------------------------------------------------------------------- types

def parse_type(s):
    s = s.strip()
    if s.startswith("("):
        inner = s[1:match_close(s, 0, "(", ")")]
        parts = split_top(inner)
        if not parts:
            raise Unsupported("unit type")
        return ("tuple", [parse_type(p) for p in parts])
    if s.startswith("["):
        raise Unsupported("array type " + s)
    if s.startswith("&"):
        raise Unsupported("reference type " + s)
    m = re.match(r"^([\w:]+)\s*(?:<(.*)>)?$", s, re.S)
    if not m:
        raise Unsupported("cannot parse type " + s)
    name = m.group(1).split("::")[-1]
    args = [parse_type(a) for a in split_top(m.group(2))] if m.group(2) else []
    return ("path", name, args, m.group(1))


class Env:
    def __init__(self, defs, aliases, prefix):
        self.defs, self.aliases, self.prefix = defs, aliases, prefix

    def norm(self, t, depth=0):
        if depth > 20:
            raise Unsupported("alias cycle")
        if t[0] == "tuple":
            return ("tuple", [self.norm(x, depth + 1) for x in t[1]])
        _, name, args, full = t
        if name in PRIMS and not args:
            return ("prim", name)
        if name == "String" and not args:
            return ("string",)
        if name in ("Option",) and len(args) == 1:
            return ("opt", self.norm(args[0], depth + 1))
        if name in ("Vec", "VecDeque") and len(args) == 1:
            inner = self.norm(args[0], depth + 1)
            if inner == ("prim", "u8"):
                return ("bytes", name)
            return ("vec", inner, name)
        if name in ("BTreeSet", "HashSet") and len(args) >= 1:
            return ("set", self.norm(args[0], depth + 1), name)
        if name in ("BTreeMap", "HashMap") and len(args) >= 2:
            hasher = args[2] if len(args) > 2 else None
            return ("map", self.norm(args[0], depth + 1), self.norm(args[1], depth + 1), name,
                    None if hasher is None else hasher[3] if hasher[0] == "path" else None)
        if name in ("Box", "Arc", "Rc") and len(args) == 1:
            return ("box", self.norm(args[0], depth + 1), name)
        if name == "Value" and "serde_json" in full:
            raise Unsupported("serde_json::Value")
        if name in self.aliases and name not in self.defs:
            return self.norm(parse_type(self.aliases[name]), depth + 1)
        if name in self.defs:
            return ("named", name)
        raise Unsupported("unknown type " + full)

    def d(self, name):
        cands = self.defs[name]
        if len(cands) > 1:
            paths = sorted(set(c.path for c in cands))
            # a repeated definition in the same module (a cfg'd twin) is one definition
            if len(paths) > 1:
                raise Unsupported("type name %s is defined in several modules: %s" % (name, ", ".join(paths)))
        return cands[0]

    def is_unit_enum(self, t):
        if t[0] != "named":
            return False
        d = self.d(t[1])
        return d.kind == "enum" and all(v[1] == "unit" for v in d.variants)

    def is_msg(self, t):
        if t[0] == "tuple":
            return True
        if t[0] == "named":
            return not self.is_unit_enum(t)
        return False

    def is_scalar(self, t):
        return t[0] in ("prim", "string", "bytes")

    def is_simple(self, t):
        return self.is_scalar(t) or t[0] in ("named", "tuple")


def snake(name):
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def rfield(name):
    return "r#" + name if name in KEYWORDS else name


def serde_flags(attrs):
    text = " ".join(attrs)
    if "flatten" in text:
        raise Unsupported("serde(flatten)")
    if "untagged" in text or re.search(r"\btag\s*=", text):
        raise Unsupported("serde(untagged/tag)")
    if re.search(r"\bwith\s*=|serialize_with|deserialize_with", text):
        # custom per-field encoding: only the byte-list helper is representable
        if "bytes_serde" not in text:
            raise Unsupported("serde(with = ...) " + text[:80])
    return "skip" in text and re.search(r"\bskip\b|skip_serializing\b(?!_if)", text) is not None


# --------------------------------------------------------------------------- generation

class Gen:
    def __init__(self, env, roots):
        self.env, self.roots = env, roots
        self.p = env.prefix
        self.todo, self.done = list(roots), set()
        self.proto, self.rust = [], []
        self.n = 0
        self.wrappers = {}
        self.unsupported = []
        self.samples = []
        self.checks = []

    def pname(self, name):
        return self.p + name

    def mod(self, name):
        return snake(self.pname(name))

    def rpath(self, name):
        d = self.env.d(name)
        return getattr(self, "path_map", {}).get(d.path, d.path) + "::" + name

    # ----- proto field declarations
    def elem(self, t):
        if t[0] in ("prim", "string", "bytes"):
            return PROTO_SCALAR[t[1] if t[0] == "prim" else t[0]]
        if t[0] == "named":
            self.need(t[1]); return self.pname(t[1])
        if t[0] == "tuple":
            return self.tuple_msg(t)
        if t[0] == "box":
            return self.elem(t[1])
        raise Unsupported("no element type for %r" % (t,))

    def key_ok(self, k):
        return (k[0] == "prim" and k[1] in MAP_KEYS) or k[0] == "string"

    def field(self, t, name, num):
        e = self.env
        if t[0] == "box":
            return self.field(t[1], name, num)
        if t[0] == "opt":
            inner = t[1]
            while inner[0] == "box":
                inner = inner[1]
            if e.is_scalar(inner) or e.is_unit_enum(inner):
                return "optional %s %s = %d;" % (self.elem(inner), name, num)
            if e.is_msg(inner):
                return "%s %s = %d;" % (self.elem(inner), name, num)
            return "%s %s = %d;" % (self.wrap(inner), name, num)
        if t[0] in ("vec", "set"):
            inner = t[1]
            if e.is_simple(inner) or inner[0] == "box":
                return "repeated %s %s = %d;" % (self.elem(inner), name, num)
            return "repeated %s %s = %d;" % (self.wrap(inner), name, num)
        if t[0] == "map":
            k, v = t[1], t[2]
            if self.key_ok(k):
                vt = self.elem(v) if (e.is_simple(v) or v[0] == "box") else self.wrap(v)
                return "map<%s, %s> %s = %d;" % (self.elem(k), vt, name, num)
            return "repeated %s %s = %d;" % (self.entry(k, v), name, num)
        return "%s %s = %d;" % (self.elem(t), name, num)

    def wrap(self, inner):
        key = repr(inner)
        if key in self.wrappers:
            return self.wrappers[key][0]
        self.n += 1
        name = "%sBox%d" % (self.p, self.n)
        self.wrappers[key] = (name, inner)
        self.proto.append("message %s {\n  %s\n}\n" % (name, self.field(inner, "value", 1)))
        return name

    def entry(self, k, v):
        key = "entry" + repr((k, v))
        if key in self.wrappers:
            return self.wrappers[key][0]
        self.n += 1
        name = "%sEntry%d" % (self.p, self.n)
        self.wrappers[key] = (name, (k, v))
        self.proto.append("message %s {\n  %s\n  %s\n}\n" % (name, self.field(k, "key", 1), self.field(v, "value", 2)))
        return name

    def tuple_msg(self, t):
        key = repr(t)
        if key in self.wrappers:
            return self.wrappers[key][0]
        self.n += 1
        name = "%sTup%d" % (self.p, self.n)
        self.wrappers[key] = (name, t)
        lines = [self.field(x, "f%d" % i, i + 1) for i, x in enumerate(t[1])]
        self.proto.append("message %s {\n%s\n}\n" % (name, "\n".join("  " + l for l in lines)))
        return name

    def need(self, name):
        if name not in self.done and name not in self.todo:
            self.todo.append(name)

    # ----- rust: to_proto
    def to_value(self, t, v):
        e = self.env
        k = t[0]
        if k == "prim":
            n = t[1]
            if n in ("u8", "u16"):
                return "u32::from(*%s)" % v
            if n in ("i8", "i16"):
                return "i32::from(*%s)" % v
            if n == "usize":
                return "(*%s) as u64" % v
            if n == "isize":
                return "(*%s) as i64" % v
            return "*%s" % v
        if k in ("string", "bytes"):
            return "%s.clone()" % v
        if k == "box":
            return self.to_value(t[1], "(&**%s)" % v)
        if k == "named":
            return "%s_to_proto(%s)" % (snake(t[1]), v)
        if k == "tuple":
            name = self.tuple_msg(t)
            fields = ", ".join("f%d: %s" % (i, self.to_field(x, "(&%s.%d)" % (v, i))) for i, x in enumerate(t[1]))
            return "v1::%s { %s }" % (name, fields)
        raise Unsupported("to_value of %r" % (t,))

    def to_field(self, t, v):
        e = self.env
        k = t[0]
        if k == "box":
            return self.to_field(t[1], "(&**%s)" % v)
        if k == "opt":
            inner = t[1]
            while inner[0] == "box":
                inner = ("box-skip",) if False else inner[1]
            if e.is_scalar(inner) or e.is_unit_enum(inner) or e.is_msg(inner):
                return "%s.as_ref().map(|x| %s)" % (v, self.to_value(t[1], "x"))
            w = self.wrap(inner)
            return "%s.as_ref().map(|x| v1::%s { value: %s })" % (v, w, self.to_field(t[1], "x"))
        if k in ("vec", "set"):
            inner = t[1]
            if e.is_simple(inner) or inner[0] == "box":
                return "%s.iter().map(|x| %s).collect()" % (v, self.to_value(inner, "x"))
            w = self.wrap(inner)
            return "%s.iter().map(|x| v1::%s { value: %s }).collect()" % (v, w, self.to_field(inner, "x"))
        if k == "bytes":
            return "%s.iter().copied().collect()" % v if t[1] == "VecDeque" else "%s.clone()" % v
        if k == "map":
            kk, vv = t[1], t[2]
            if self.key_ok(kk):
                if e.is_simple(vv) or vv[0] == "box":
                    val = self.to_value(vv, "x")
                else:
                    val = "v1::%s { value: %s }" % (self.wrap(vv), self.to_field(vv, "x"))
                return "%s.iter().map(|(k, x)| (%s, %s)).collect()" % (v, self.to_value(kk, "k"), val)
            en = self.entry(kk, vv)
            return "%s.iter().map(|(k, x)| v1::%s { key: %s, value: %s }).collect()" % (
                v, en, self.to_field(kk, "k"), self.to_field(vv, "x"))
        if e.is_msg(t):
            return "Some(%s)" % self.to_value(t, v)
        return self.to_value(t, v)

    # ----- rust: from_proto (every expression is a Result<_, String>)
    def from_value(self, t, p, ctx):
        k = t[0]
        if k == "prim":
            n = t[1]
            wide = {"u32", "u64", "i32", "i64", "f32", "f64", "bool"}
            if n in wide:
                return "Ok::<_, String>(%s)" % p
            return "%s::try_from(%s).map_err(|_| \"%s is out of range for %s\".to_string())" % (n, p, ctx, n)
        if k in ("string", "bytes"):
            return "Ok::<_, String>(%s)" % p
        if k == "box":
            return "%s.map(%s::new)" % (self.from_value(t[1], p, ctx), t[2])
        if k == "named":
            return "%s_from_proto(%s)" % (snake(t[1]), p)
        if k == "tuple":
            parts = ", ".join("%s?" % self.from_field(x, "m.f%d" % i, ctx) for i, x in enumerate(t[1]))
            return "(|| -> Result<_, String> { let m = %s; Ok((%s,)) })()" % (p, parts)
        raise Unsupported("from_value of %r" % (t,))

    def coll(self, t):
        if t[0] == "vec":
            return "Result<%s<_>, String>" % t[2]
        if t[0] == "set":
            return "Result<%s<_>, String>" % t[2]
        raise Unsupported("coll")

    def from_field(self, t, p, ctx):
        e = self.env
        k = t[0]
        if k == "box":
            return "%s.map(%s::new)" % (self.from_field(t[1], p, ctx), t[2])
        if k == "opt":
            inner = t[1]
            raw = inner
            while raw[0] == "box":
                raw = raw[1]
            if e.is_scalar(raw) or e.is_unit_enum(raw) or e.is_msg(raw):
                return "%s.map(|x| %s).transpose()" % (p, self.from_value(inner, "x", ctx))
            return "%s.map(|w| %s).transpose()" % (p, self.from_field(inner, "w.value", ctx))
        if k in ("vec", "set"):
            inner = t[1]
            if e.is_simple(inner) or inner[0] == "box":
                f = "|x| %s" % self.from_value(inner, "x", ctx)
            else:
                f = "|w| %s" % self.from_field(inner, "w.value", ctx)
            return "%s.into_iter().map(%s).collect::<%s>()" % (p, f, self.coll(t))
        if k == "bytes":
            return "Ok::<_, String>(%s.into_iter().collect())" % p if t[1] == "VecDeque" else "Ok::<_, String>(%s)" % p
        if k == "map":
            kk, vv, name, hasher = t[1], t[2], t[3], t[4]
            target = "%s<_, _>" % name if hasher is None else "%s<_, _, %s>" % (name, hasher)
            if self.key_ok(kk):
                if e.is_simple(vv) or vv[0] == "box":
                    val = "%s?" % self.from_value(vv, "x", ctx)
                else:
                    val = "%s?" % self.from_field(vv, "x.value", ctx)
                return ("%s.into_iter().map(|(k, x)| -> Result<_, String> { Ok((%s?, %s)) })"
                        ".collect::<Result<%s, String>>()") % (p, self.from_value(kk, "k", ctx), val, target)
            return ("%s.into_iter().map(|en| -> Result<_, String> { Ok((%s?, %s?)) })"
                    ".collect::<Result<%s, String>>()") % (
                p, self.from_field(kk, "en.key", ctx), self.from_field(vv, "en.value", ctx), target)
        if e.is_msg(t):
            return "%s.ok_or_else(|| \"%s is missing\".to_string()).and_then(|x| %s)" % (p, ctx, self.from_value(t, "x", ctx))
        return self.from_value(t, p, ctx)

    # ----- sample values: every leaf non-default and distinct, for the round-trip test
    def sample(self, t):
        k = t[0]
        if k == "prim":
            n = t[1]
            if n == "bool":
                return "(next(n) % 2 == 0)"
            if n in ("f32", "f64"):
                return "(next(n) as %s + 0.5)" % n
            mod = {"u8": 100, "i8": 100, "u16": 10000, "i16": 10000}.get(n)
            if mod:
                return "((next(n) %% %d + 1) as %s)" % (mod, n)
            return "((next(n) + 1) as %s)" % n
        if k == "string":
            return 'format!("s{}", next(n))'
        if k == "bytes":
            return "vec![next(n) as u8, 7, 9].into_iter().collect()"
        if k == "box":
            return "%s::new(%s)" % (t[2], self.sample(t[1]))
        if k == "named":
            return "sample_%s(n, depth + 1)" % snake(t[1])
        if k == "tuple":
            return "(%s,)" % ", ".join(self.sample(x) for x in t[1])
        if k == "opt":
            return "if depth > 3 { None } else { Some(%s) }" % self.sample(t[1])
        if k in ("vec", "set"):
            two = "[%s, %s]" % (self.sample(t[1]), self.sample(t[1]))
            return "if depth > 3 { Default::default() } else { %s.into_iter().collect() }" % two
        if k == "map":
            two = "[(%s, %s), (%s, %s)]" % (self.sample(t[1]), self.sample(t[2]), self.sample(t[1]), self.sample(t[2]))
            return "if depth > 3 { Default::default() } else { %s.into_iter().collect() }" % two
        raise Unsupported("sample of %r" % (t,))

    # ----- one definition
    def emit(self, name):
        e = self.env
        d = e.d(name)
        if getattr(d, "broken", None):
            raise Unsupported("%s: %s" % (name, d.broken))
        if d.generics:
            raise Unsupported("%s has generics" % name)
        pn = self.pname(name)
        rp = self.rpath(name)
        fn = snake(name)
        if d.kind == "struct":
            skip_fields = []
            fields = []
            for (fname, ts, attrs) in d.fields:
                if serde_flags(attrs):
                    skip_fields.append(fname)
                    continue
                fields.append((fname, e.norm(parse_type(ts)), ts))
            tuple_struct = bool(d.fields) and d.fields[0][0] is None
            lines, to_f, from_f, binds = [], [], [], []
            for i, (fname, t, ts) in enumerate(fields):
                pf = "f%d" % i if tuple_struct else fname
                lines.append("  " + self.field(t, pf, i + 1))
            self.proto.append("message %s {\n%s\n}\n" % (pn, "\n".join(lines)))
            if tuple_struct:
                if skip_fields:
                    raise Unsupported(name + ": skipped tuple field")
                binds = ["f%d" % i for i in range(len(fields))]
                pat = "let %s(%s) = v;" % (rp, ", ".join(binds))
                to = ", ".join("f%d: %s" % (i, self.to_field(t, "f%d" % i)) for i, (_, t, _) in enumerate(fields))
                fr = ", ".join("%s?" % self.from_field(t, "m.f%d" % i, "%s.f%d" % (name, i)) for i, (_, t, _) in enumerate(fields))
                self.rust.append(
                    "pub fn %s_to_proto(v: &%s) -> v1::%s {\n    %s\n    v1::%s { %s }\n}\n\n"
                    "pub fn %s_from_proto(m: v1::%s) -> Result<%s, String> {\n    Ok(%s(%s))\n}\n" % (
                        fn, rp, pn, pat, pn, to, fn, pn, rp, rp, fr))
            else:
                pat_fields = []
                for (fname, ts, attrs) in d.fields:
                    pat_fields.append("%s: _" % fname if fname in skip_fields else fname)
                pat = "let %s { %s } = v;" % (rp, ", ".join(pat_fields))
                to = ", ".join("%s: %s" % (rfield(fname), self.to_field(t, fname)) for (fname, t, _) in fields)
                fr = [ "%s: %s?" % (fname, self.from_field(t, "m.%s" % rfield(fname), "%s.%s" % (name, fname))) for (fname, t, _) in fields]
                for s in skip_fields:
                    fr.append("%s: Default::default()" % s)
                self.rust.append(
                    "pub fn %s_to_proto(v: &%s) -> v1::%s {\n    %s\n    v1::%s { %s }\n}\n\n"
                    "pub fn %s_from_proto(m: v1::%s) -> Result<%s, String> {\n    Ok(%s { %s })\n}\n" % (
                        fn, rp, pn, pat, pn, to, fn, pn, rp, rp, ", ".join(fr)))
            if tuple_struct:
                body = "%s(%s)" % (rp, ", ".join(self.sample(t) for (_, t, _) in fields))
            else:
                parts = ["%s: %s" % (fname, self.sample(t)) for (fname, t, _) in fields]
                parts += ["%s: Default::default()" % s_ for s_ in skip_fields]
                body = "%s { %s }" % (rp, ", ".join(parts))
            self.samples.append("pub fn sample_%s(n: &mut u64, depth: u8) -> %s {\n    %s\n}\n" % (fn, rp, body))
            return
        # enum
        for v in d.variants:
            serde_flags(v[3])
        if all(v[1] == "unit" for v in d.variants):
            up = snake(pn).upper()
            vals = ["  %s_ZERO_UNUSED = 0;" % up]
            to_arms, from_arms = [], []
            for i, v in enumerate(d.variants, 1):
                vals.append("  %s_%s = %d;" % (up, snake(v[0]).upper(), i))
                to_arms.append("%s::%s => %d" % (rp, v[0], i))
                from_arms.append("%d => Ok(%s::%s)" % (i, rp, v[0]))
            self.proto.append("enum %s {\n%s\n}\n" % (pn, "\n".join(vals)))
            arms = ["%d => %s::%s" % (i, rp, v[0]) for i, v in enumerate(d.variants[:-1])]
            arms.append("_ => %s::%s" % (rp, d.variants[-1][0]))
            self.samples.append("pub fn sample_%s(n: &mut u64, depth: u8) -> %s {\n    match next(n) %% %d {\n        %s,\n    }\n}\n" % (
                fn, rp, len(d.variants), ",\n        ".join(arms)))
            self.rust.append(
                "pub fn %s_to_proto(v: &%s) -> i32 {\n    match v {\n        %s,\n    }\n}\n\n"
                "pub fn %s_from_proto(m: i32) -> Result<%s, String> {\n    match m {\n        %s,\n"
                "        other => Err(format!(\"%s has no variant {other}\")),\n    }\n}\n" % (
                    fn, rp, ",\n        ".join(to_arms), fn, rp, ",\n        ".join(from_arms), name))
            return
        # data enum: one message per variant, a oneof in the enum's own message
        oneof, to_arms, from_arms = [], [], []
        sample_arms = []
        modname = self.mod(name)
        for i, (vname, vkind, vfields, vattrs) in enumerate(d.variants, 1):
            vm = pn + vname
            vs = snake(vname)
            lines = []
            ftypes = []
            for j, (fname, ts, fattrs) in enumerate(vfields):
                if serde_flags(fattrs):
                    raise Unsupported("%s::%s: skipped variant field" % (name, vname))
                t = e.norm(parse_type(ts))
                ftypes.append((fname if fname else "f%d" % j, t))
                lines.append("  " + self.field(t, fname if fname else "f%d" % j, j + 1))
            self.proto.append("message %s {\n%s\n}\n" % (vm, "\n".join(lines)))
            oneof.append("    %s %s = %d;" % (vm, vs, i))
            kind_path = "v1::%s::Kind::%s" % (modname, vname)
            if vkind == "unit":
                made = "%s::%s" % (rp, vname)
            elif vkind == "tuple":
                made = "%s::%s(%s)" % (rp, vname, ", ".join(self.sample(t) for (_, t) in ftypes))
            else:
                made = "%s::%s { %s }" % (rp, vname, ", ".join("%s: %s" % (fn_, self.sample(t)) for fn_, t in ftypes))
            sample_arms.append(made)
            if vkind == "unit":
                to_arms.append("%s::%s => %s(v1::%s {})" % (rp, vname, kind_path, vm))
                from_arms.append("%s(_) => Ok(%s::%s)" % (kind_path, rp, vname))
            elif vkind == "tuple":
                binds = ", ".join("f%d" % j for j in range(len(ftypes)))
                to = ", ".join("%s: %s" % ("f%d" % j, self.to_field(t, "f%d" % j)) for j, (_, t) in enumerate(ftypes))
                fr = ", ".join("%s?" % self.from_field(t, "x.f%d" % j, "%s::%s.f%d" % (name, vname, j)) for j, (_, t) in enumerate(ftypes))
                to_arms.append("%s::%s(%s) => %s(v1::%s { %s })" % (rp, vname, binds, kind_path, vm, to))
                from_arms.append("%s(x) => Ok(%s::%s(%s))" % (kind_path, rp, vname, fr))
            else:
                binds = ", ".join(rfield(fn_) if fn_ else "" for fn_, _ in ftypes)
                to = ", ".join("%s: %s" % (rfield(fn_), self.to_field(t, fn_)) for fn_, t in ftypes)
                fr = ", ".join("%s: %s?" % (fn_, self.from_field(t, "x.%s" % rfield(fn_), "%s::%s.%s" % (name, vname, fn_))) for fn_, t in ftypes)
                to_arms.append("%s::%s { %s } => %s(v1::%s { %s })" % (rp, vname, binds, kind_path, vm, to))
                from_arms.append("%s(x) => Ok(%s::%s { %s })" % (kind_path, rp, vname, fr))
        self.proto.append("message %s {\n  oneof kind {\n%s\n  }\n}\n" % (pn, "\n".join(oneof)))
        arms = ["%d => %s" % (i, a) for i, a in enumerate(sample_arms[:-1])]
        arms.append("_ => %s" % sample_arms[-1])
        self.samples.append("pub fn sample_%s(n: &mut u64, depth: u8) -> %s {\n    match next(n) %% %d {\n        %s,\n    }\n}\n" % (
            fn, rp, len(sample_arms), ",\n        ".join(arms)))
        self.rust.append(
            "pub fn %s_to_proto(v: &%s) -> v1::%s {\n    let kind = match v {\n        %s,\n    };\n    v1::%s { kind: Some(kind) }\n}\n\n"
            "pub fn %s_from_proto(m: v1::%s) -> Result<%s, String> {\n    match m.kind.ok_or_else(|| \"%s has no variant set\".to_string())? {\n        %s,\n    }\n}\n" % (
                fn, rp, pn, ",\n        ".join(to_arms), pn, fn, pn, rp, name, ",\n        ".join(from_arms)))

    def run(self):
        while self.todo:
            n = self.todo.pop(0)
            if n in self.done:
                continue
            self.done.add(n)
            try:
                self.emit(n)
            except Unsupported as ex:
                self.unsupported.append((n, str(ex)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--roots", required=True)
    ap.add_argument("--proto-marker", required=True)
    ap.add_argument("--out-rust", required=True)
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--path-map", default="", help="private=public,... module path rewrites")
    a = ap.parse_args()

    defs, aliases = index_crate(a.repo)
    env = Env(defs, aliases, a.prefix)
    roots = [r for r in a.roots.split(",") if r]
    for r in roots:
        if r not in defs:
            print("ROOT NOT FOUND:", r); sys.exit(2)
    g = Gen(env, roots)
    g.path_map = dict(p.split("=", 1) for p in a.path_map.split(",") if p)
    g.run()
    print("types:", len(g.done), "messages/enums:", len(g.proto), "converters:", len(g.rust))
    if g.unsupported:
        print("UNSUPPORTED:")
        for n, why in g.unsupported:
            print("  %-34s %s" % (n, why))
    if a.report or g.unsupported:
        for n in sorted(g.done):
            d = defs[n][0]
            print("  %-34s %-6s %s" % (n, d.kind, d.path))
        if g.unsupported:
            sys.exit(1)
        return

    marker = a.proto_marker
    begin = "// BEGIN GENERATED %s (tools/gen_proto_converters.py: do not edit)" % marker
    end = "// END GENERATED %s" % marker
    block = begin + "\n\n" + "\n".join(g.proto) + "\n" + end + "\n"
    pp = os.path.join(a.repo, PROTO)
    text = io.open(pp, encoding="utf-8", newline="").read()
    if begin in text:
        s0 = text.index(begin)
        s1 = text.index(end, s0) + len(end)
        s1 = s1 + 1 if text[s1:s1 + 1] == "\n" else s1
        text = text[:s0] + block + text[s1:]
    else:
        text = text.rstrip("\n") + "\n\n" + block
    io.open(pp, "w", encoding="utf-8", newline="").write(text)

    header = (
        "// SPDX-License-Identifier: Apache-2.0\n// Copyright 2026 MatrixArkAI\n\n"
        "//! GENERATED by tools/gen_proto_converters.py (prefix `%s`, roots %s). DO NOT EDIT.\n//!\n"
        "//! Every struct is destructured without `..`, so a field added to one of these types fails to\n"
        "//! compile until this file is regenerated.\n\n"
        "#![allow(clippy::all, unused_imports, unused_variables, dead_code, unused_parens)]\n\n"
        "use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};\n"
        "use std::rc::Rc;\nuse std::sync::Arc;\n\nuse crate::sdk::v1;\n\n") % (a.prefix, ", ".join(roots))
    checks = []
    for name in sorted(g.done):
        fn = snake(name)
        checks.append(
            "            let v = sample_%s(&mut n, 0);\n"
            "            assert_eq!(json(&v), json(&%s_from_proto(%s_to_proto(&v)).expect(\"%s must decode\")), \"%s\");\n"
            "            checked += 1;\n" % (fn, fn, fn, name, name))
    sample_text = "\n".join(("    " + line) if line else line for sample in g.samples for line in sample.split("\n"))
    tests = (
        "\n#[cfg(test)]\npub(crate) mod generated_round_trips {\n    use super::*;\n\n"
        "    fn next(n: &mut u64) -> u64 {\n        *n += 1;\n        *n\n    }\n\n"
        "    fn json<T: serde::Serialize>(v: &T) -> serde_json::Value {\n        serde_json::to_value(v).expect(\"serializes\")\n    }\n\n"
        + sample_text + "\n\n"
        "    /// EVERY GENERATED TYPE ROUND-TRIPS A VALUE WITH EVERY LEAF SET, across several seeds so every\n"
        "    /// variant of every enum is built. The count is asserted: a loop over nothing would pass.\n"
        "    #[test]\n    fn every_generated_type_round_trips() {\n        let mut checked = 0;\n"
        "        for seed in 0..12u64 {\n            let mut n = seed * 7919;\n"
        + "".join(checks) +
        "        }\n        assert_eq!(checked, %d * 12);\n    }\n}\n" % len(g.done)
    )
    out = header + "\n".join(g.rust) + tests
    io.open(os.path.join(a.repo, a.out_rust), "w", encoding="utf-8", newline="").write(out)
    print("wrote", a.out_rust)


if __name__ == "__main__":
    main()
