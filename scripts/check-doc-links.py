#!/usr/bin/env python3
"""Check every relative markdown link, image and heading anchor resolves.

# Why this is a test

These documents cross-reference each other constantly -- the README points at
seven of them, CLAIMS-MAP.md points at a named section for almost every claim it
makes -- and a link that has rotted is indistinguishable from one that works
until somebody clicks it. At milestone 9 `docs/CLAIMS-MAP.md` was linking to
`docs/ARCHITECTURE.md`, which did not exist. Nothing failed; the file simply
promised a reader something that was not there.

Anchors are checked too, not just paths, because `RUNNING.md#containerised`
pointing at a section that has since been renamed is the same defect wearing a
different hat, and it is the more likely one: paths change rarely and headings
get reworded all the time.

Takes about a second and needs nothing installed.

    scripts/check-doc-links.py
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LINK = re.compile(r'(?<!\!)\[[^\]]*\]\(([^)]+)\)')
IMG = re.compile(r'<img[^>]+src="([^"]+)"')

targets = []
for dirpath, dirnames, filenames in os.walk(ROOT):
    dirnames[:] = [d for d in dirnames if d not in (".git", "target", "build", "node_modules")]
    for fn in filenames:
        if fn.endswith(".md"):
            targets.append(os.path.join(dirpath, fn))

bad = 0
checked = 0


def anchorise(text):
    """GitHub's heading -> anchor rule, near enough."""
    a = text.strip().lower()
    a = re.sub(r'[^\w\s-]', '', a)
    return re.sub(r'\s+', '-', a)


# Collect anchors per file.
anchors = {}
for path in targets:
    with open(path, encoding="utf-8") as fh:
        heads = set()
        for line in fh:
            m = re.match(r'^(#{1,6})\s+(.*?)\s*$', line)
            if m:
                heads.add(anchorise(m.group(2)))
            m2 = re.search(r'<a\s+(?:id|name)="([^"]+)"', line)
            if m2:
                heads.add(m2.group(1))
        anchors[os.path.realpath(path)] = heads

for path in targets:
    rel = os.path.relpath(path, ROOT)
    with open(path, encoding="utf-8") as fh:
        body = fh.read()
    for m in list(LINK.finditer(body)) + list(IMG.finditer(body)):
        href = m.group(1).strip()
        if href.startswith(("http://", "https://", "mailto:", "#")):
            if href.startswith("#"):
                checked += 1
                want = href[1:].lower()
                if want and want not in anchors[os.path.realpath(path)]:
                    print("BROKEN ANCHOR  %s -> %s" % (rel, href))
                    bad += 1
            continue
        checked += 1
        frag = None
        if "#" in href:
            href, frag = href.split("#", 1)
        if not href:
            continue
        target = os.path.normpath(os.path.join(os.path.dirname(path), href))
        if not os.path.exists(target):
            print("BROKEN PATH    %s -> %s" % (rel, m.group(1)))
            bad += 1
            continue
        if frag and target.endswith(".md"):
            have = anchors.get(os.path.realpath(target), set())
            if frag.lower() not in have:
                print("BROKEN ANCHOR  %s -> %s" % (rel, m.group(1)))
                bad += 1

if bad:
    print("\ndoc-links: FAIL — %d of %d links broken across %d files"
          % (bad, checked, len(targets)))
else:
    print("doc-links: PASS — %d links across %d files, every one resolves"
          % (checked, len(targets)))
sys.exit(1 if bad else 0)
