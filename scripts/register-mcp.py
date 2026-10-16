#!/usr/bin/env python3
import sys

path = sys.argv[1] if len(sys.argv) > 1 else None
if not path:
    raise SystemExit("usage: register-mcp.py <opencode.json>")

with open(path, "r", encoding="utf-8") as f:
    text = f.read()

entry = '''    "chan": {
      "type": "local",
      "command": ["chan-mcp"],
      "enabled": true
    },
'''

if '"chan": {' in text:
    print("chan MCP entry already present; nothing to do")
    raise SystemExit(0)

needle = '    "memokf": {'
if needle in text:
    text = text.replace(needle, entry + needle, 1)
else:
    # Last resort: insert before the closing of the mcp object's first top-level-ish end.
    # We look for the mcp section and inject right after its opening brace.
    marker = '  "mcp": {'
    idx = text.find(marker)
    if idx < 0:
        raise SystemExit("could not find mcp section or memokf entry")
    pos = text.find("{", idx) + 1
    text = text[:pos] + "\n" + entry + text[pos:]

backup = path + ".bak-chan-install"
with open(backup, "w", encoding="utf-8") as f:
    f.write(open(path, encoding="utf-8").read())
with open(path, "w", encoding="utf-8") as f:
    f.write(text)
print("registered chan MCP server in", path)
"""