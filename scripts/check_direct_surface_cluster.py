#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check object-surface results, including rejected compare-and-swap writes."""
import json


def expectations(prefix, cas=True):
    vector = lambda v: json.dumps(list(v))
    keys = lambda *v: json.dumps([prefix + '/' + k for k in v])
    result = [
        ('get absent', 'err not-found'),
        ('put a', 'ok ()'), ('put b', 'ok ()'), ('put c', 'ok ()'),
        ('get a', 'ok ' + vector(range(100))),
        ('range 10..20', 'ok ' + vector(range(10, 20))),
        ('range ending at the end', 'ok ' + vector(range(90, 100))),
        ('range past the end', 'err range'),
        ('range_from 95', 'ok Some(' + vector(range(95, 100)) + ')'),
        ('range_from at the end', 'ok Some(0)'),
        ('range_from past the end', 'ok None'),
        ('list prefix', 'ok ' + keys('a', 'b', 'c')),
        ('list_after a', 'ok ' + keys('b', 'c')),
        ('append to b', 'ok (6, 11, 11)'),
        ('get b after append', 'ok ' + vector(b'second+tail')),
        ('delete c', 'ok ()'), ('get c after delete', 'err not-found'),
        ('list after delete', 'ok ' + keys('a', 'b')),
        ('delete absent', 'ok ()'),
    ]
    if cas:
        result += [
            ('CAS create', 'ok ()'),
            ('CAS wrong value', 'err precondition'),
            ('get CAS after rejection', 'ok ' + vector(b'first')),
            ('CAS update', 'ok ()'),
            ('get CAS after update', 'ok ' + vector(b'second')),
            ('CAS duplicate create', 'err precondition'),
            ('get CAS after duplicate create', 'ok ' + vector(b'second')),
        ]
    return result


def validate(text, prefix, cas=True):
    rows = []
    for line in text.splitlines():
        if not line.startswith('OBS '):
            continue
        label, separator, payload = line[4:].partition(' => ')
        if not separator:
            raise AssertionError('malformed observation: ' + line)
        rows.append((label, payload))
    expected = expectations(prefix, cas)
    if len(rows) != len(expected):
        raise AssertionError(f'expected {len(expected)} observations, received {len(rows)}')
    failures = [(actual, wanted) for actual, wanted in zip(rows, expected) if actual != wanted]
    if failures:
        raise AssertionError('unexpected object result: ' + repr(failures))
    return rows


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prefix', required=True)
    parser.add_argument('log')
    args = parser.parse_args()
    rows = validate(Path(args.log).read_text(), args.prefix)
    print(f'PASS: {len(rows)} expected object operations including compare-and-swap')
