#!/usr/bin/env python3
"""
Diagnostic for meta-runner / recipe discovery on TeamCity 2025.03+ (recipes rename).

In 2025.03 "meta-runners" were renamed to "recipes" and the admin Recipes page was
rebuilt, so the old scrape of `editProject.html?tab=recipe` for `editRecipeId=<id>` no
longer returns those links. This script probes several candidate endpoints and prints
the HTTP status, content-type, size, and whether known ID patterns appear -- so we can
see exactly where the recipe IDs live on your server and lock the parser to it.

Usage:
    TEAMCITY_URL=https://teamcity.example.com \
    TEAMCITY_TOKEN=xxxxx \
    RECIPES_PROJECT_ID=_Root \
    python3 diagnose_recipes.py

Nothing is written or changed on the server -- all requests are GETs.
"""
import os
import re
import sys
import requests

URL = os.environ.get("TEAMCITY_URL", "").rstrip("/")
TOKEN = os.environ.get("TEAMCITY_TOKEN", "")
PROJECT = os.environ.get("RECIPES_PROJECT_ID") or os.environ.get("PARENT_PROJECT_ID") or "_Root"
TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))

if not URL or not TOKEN:
    sys.exit("Set TEAMCITY_URL and TEAMCITY_TOKEN (and optionally RECIPES_PROJECT_ID).")

AUTH = {"Authorization": f"Bearer {TOKEN}"}
RECIPE_ID_RE = re.compile(r"editRecipeId=([A-Za-z0-9_.\-]+)")
# Recipe ids also surface as build-step "type" values; scan JSON/HTML for likely id tokens.
GENERIC_ID_RE = re.compile(r'"(?:id|internalId|recipeId|metaRunnerId)"\s*:\s*"([A-Za-z0-9_.\-]+)"')

# (label, path, accept). Mix of the old JSP tabs and likely new data endpoints.
CANDIDATES = [
    ("old JSP tab=recipe",       f"/admin/editProject.html?projectId={PROJECT}&tab=recipe",       "text/html"),
    ("old JSP tab=metaRunner",   f"/admin/editProject.html?projectId={PROJECT}&tab=metaRunner",   "text/html"),
    ("old JSP tab=metaRunners",  f"/admin/editProject.html?projectId={PROJECT}&tab=metaRunners",  "text/html"),
    ("old JSP tab=recipes",      f"/admin/editProject.html?projectId={PROJECT}&tab=recipes",      "text/html"),
    ("rest project (json)",      f"/app/rest/projects/id:{PROJECT}",                              "application/json"),
    ("rest metaRunners guess",   f"/app/rest/projects/id:{PROJECT}/metaRunners",                  "application/json"),
    ("rest recipes guess",       f"/app/rest/projects/id:{PROJECT}/recipes",                      "application/json"),
]


def probe(label, path, accept):
    full = f"{URL}{path}"
    try:
        r = requests.get(full, headers={**AUTH, "Accept": accept}, timeout=TIMEOUT)
    except Exception as e:
        print(f"\n### {label}\n  {path}\n  ERROR: {e}")
        return
    body = r.text or ""
    editrecipe = sorted(set(RECIPE_ID_RE.findall(body)))
    generic = sorted(set(GENERIC_ID_RE.findall(body)))
    print(f"\n### {label}")
    print(f"  {path}")
    print(f"  status={r.status_code}  content-type={r.headers.get('Content-Type','?')}  bytes={len(body)}")
    print(f"  editRecipeId= matches: {len(editrecipe)} -> {editrecipe[:20]}")
    print(f"  id-like JSON tokens:   {len(generic)} -> {generic[:20]}")
    # Show a small snippet so we can eyeball how ids are represented.
    snippet = re.sub(r"\s+", " ", body).strip()[:600]
    print(f"  snippet: {snippet}")


if __name__ == "__main__":
    print(f"TeamCity: {URL}")
    print(f"Project : {PROJECT}")
    for label, path, accept in CANDIDATES:
        probe(label, path, accept)
    print("\nDone. Paste the output back so the parser can be locked to the working endpoint.")
