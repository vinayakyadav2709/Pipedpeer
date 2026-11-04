"""The campaign env must run the library versions pipedpeer's closures run.
Both use the same nixpkgs revision, but pipedpeer pins some packages to a
wheel (src/internal/nixgen/wheelpin.go); env/flake.nix must carry each pin,
or plain, Ray and Dask run another version than pipedpeer and the result
checks compare two libraries (W4 at the rehearsal's size: scikit-learn 1.8.0
against 1.9.0 gave different best_params)."""
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.normpath(os.path.join(HERE, "..", "..", ".."))


def go_pins():
    src = open(os.path.join(REPO, "src/internal/nixgen/wheelpin.go")).read()
    table = src[src.index("var wheelPins = []wheelPin{"):]
    table = table[:table.index("\n}\n")]
    pins = []
    for entry in re.findall(r"\n\t\{\n(.*?)\n\t\},", table, re.S):
        def field(name, e=entry):
            m = re.search(r"\b%s:\s*((?:\"[^\"]*\"\s*\+?\s*)+)," % name, e)
            return "".join(re.findall(r'"([^"]*)"', m.group(1))) if m else None
        deps = re.search(r"Deps:\s*\[\]string\{([^}]*)\}", entry)
        pins.append({"package": field("Package"), "system": field("System"), "version": field("Version"),
                     "url": field("URL"), "sha256": field("SHA256"),
                     "deps": re.findall(r'"([^"]*)"', deps.group(1)) if deps else []})
    return pins


def test_wheelpin_table_parses():
    pins = go_pins()
    assert pins and all(p["package"] and p["version"] and p["url"] and p["sha256"] for p in pins), pins


def test_campaign_env_carries_every_x86_64_wheel_pin():
    flake = open(os.path.join(REPO, "scripts/campaign/env/flake.nix")).read()
    assert 'system = "x86_64-linux"' in flake
    for p in go_pins():
        if p["system"] != "x86_64-linux":
            continue
        want = ['(ps."%s".overridePythonAttrs' % p["package"], 'version = "%s";' % p["version"],
                'url = "%s";' % p["url"], 'sha256 = "%s";' % p["sha256"]] + p["deps"]
        missing = [w for w in want if w not in flake]
        assert not missing, "env/flake.nix lacks wheelpin.go's %s pin: %s" % (p["package"], missing)
