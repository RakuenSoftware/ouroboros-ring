#!/usr/bin/env python3
"""Refuse assistant attribution in incoming commits and pull requests.

Every commit author, committer and message, and every pull request title and
body, must be free of co-author trailers, "Generated with" lines, assistant
session links and assistant identities. The patterns are the ones aimee's
no-coauthor-trailers gate enforces (server-go/modules/git/attribution), so a
change that aimee would refuse is refused here too.

    check_no_attribution.py              check the GitHub event in GITHUB_EVENT_PATH
    check_no_attribution.py --self-test  prove the patterns still refuse what they must

There are no historical exemptions. A push that creates a branch has no
"before" to compare against, so every commit reachable from it is checked.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys

# A trailer or footer may follow any run of non-letter, non-digit characters (the
# standard footer is "🤖 Generated with [Claude Code](...)"); a session link is
# refused anywhere in a line.
ATTRIBUTION_TEXT = re.compile(
    r"(^(?:[^\w\n]|_)*"
    r"(co-authored-by\s*:|(claude|codex|chatgpt|copilot)-session\s*:"
    r"|generated\s+(with|by)\s+\[?(claude|codex|chatgpt|copilot|github copilot)\b)"
    r"|https?://\S*/(code/)?session[_/-])",
    re.IGNORECASE | re.MULTILINE)
ASSISTANT_NAME = re.compile(
    r"^(claude( code)?|codex|chatgpt|(github )?copilot|copilot-swe-agent|aimee-wfe)"
    r"(\[bot\]|[- ]bot|[- ]agent)?$", re.IGNORECASE)
ASSISTANT_EMAIL = re.compile(
    r"^((noreply|claude)@anthropic\.com|(codex|noreply)@openai\.com|wfe@aimee\.local"
    r"|([0-9]+\+)?(claude|claude-code|codex|chatgpt|copilot|copilot-swe-agent)(\[bot\])?"
    r"@users\.noreply\.github\.com)$", re.IGNORECASE)
SHA = re.compile(r"^[0-9a-f]{40}$")
ZERO = "0" * 40


def text_findings(where: str, text: str) -> list[str]:
    return [f"{where}: {match.group(0).strip()!r}" for match in ATTRIBUTION_TEXT.finditer(text or "")]


def identity_findings(where: str, name: str, email: str) -> list[str]:
    found = []
    if ASSISTANT_NAME.match(name.strip()):
        found.append(f"{where} name {name!r}")
    if ASSISTANT_EMAIL.match(email.strip()):
        found.append(f"{where} email {email!r}")
    return found


def commits(revisions: list[str]) -> list[tuple[str, str, str, str, str, str]]:
    out = subprocess.run(
        ["git", "log", "--format=%H%x1f%an%x1f%ae%x1f%cn%x1f%ce%x1f%B%x1e", *revisions],
        capture_output=True, text=True, check=True).stdout
    rows = []
    for record in out.split("\x1e"):
        record = record.strip("\n")
        if record:
            rows.append(tuple(record.split("\x1f", 5)))
    return rows


def commit_findings(revisions: list[str]) -> tuple[int, list[str]]:
    rows = commits(revisions)
    found = []
    for sha, author, author_email, committer, committer_email, body in rows:
        short = sha[:12]
        found += identity_findings(f"commit {short} author", author, author_email)
        found += identity_findings(f"commit {short} committer", committer, committer_email)
        found += text_findings(f"commit {short} message", body)
    return len(rows), found


def revisions_for(event_name: str, event: dict) -> list[str]:
    def pair(base: str, head: str) -> list[str]:
        if not SHA.match(head or ""):
            raise SystemExit(f"{event_name} event has no full head SHA")
        if not base or base == ZERO:
            return [head]  # a new branch: check everything it carries
        if not SHA.match(base):
            raise SystemExit(f"{event_name} event has no full base SHA")
        return [f"{base}..{head}"]

    if event_name == "pull_request":
        pull = event.get("pull_request") or {}
        return pair((pull.get("base") or {}).get("sha", ""), (pull.get("head") or {}).get("sha", ""))
    if event_name == "push":
        return pair(event.get("before", ""), event.get("after", ""))
    if event_name == "merge_group":
        group = event.get("merge_group") or {}
        return pair(group.get("base_sha", ""), group.get("head_sha", ""))
    if event_name == "workflow_dispatch":
        inputs = event.get("inputs") or {}
        return pair(inputs.get("base_sha", ""), inputs.get("head_sha", ""))
    raise SystemExit(f"unsupported event {event_name!r}: refusing rather than skipping")


def check_event() -> int:
    event_name = os.environ.get("GITHUB_EVENT_NAME", "")
    path = os.environ.get("GITHUB_EVENT_PATH", "")
    if not event_name or not path:
        raise SystemExit("GITHUB_EVENT_NAME and GITHUB_EVENT_PATH are required")
    with open(path, encoding="utf-8") as handle:
        event = json.load(handle)
    count, found = commit_findings(revisions_for(event_name, event))
    if event_name == "pull_request":
        pull = event["pull_request"]
        found += text_findings("pull request title", pull.get("title", ""))
        found += text_findings("pull request body", pull.get("body") or "")
    if found:
        print("attribution refused:", file=sys.stderr)
        for line in found:
            print(f"  {line}", file=sys.stderr)
        return 1
    print(f"check_no_attribution: ok ({count} commit(s) and the PR text carry no attribution)")
    return 0


def self_test() -> int:
    refused = [
        ("text", "Fix it\n\nCo-Authored-By: Claude Opus <noreply@anthropic.com>\n"),
        ("text", "Body\n\n🤖 Generated with [Claude Code](https://claude.com/claude-code)\n"),
        ("text", "> co-authored-by: someone <a@b.c>"),
        ("text", "claude-session: abc"),
        ("text", "- Generated with Claude Code"),
        ("text", "_Generated by [Claude Code](https://example.com)_"),
        ("text", "See https://claude.ai/code/session_0123"),
        ("name", "Claude"),
        ("name", "copilot-swe-agent[bot]"),
        ("email", "noreply@anthropic.com"),
        ("email", "198982749+Copilot@users.noreply.github.com"),
    ]
    allowed = [
        ("text", "Teach the ring to say no.\n\nThe co-author check now runs here too.\n"),
        ("text", "Only claude has a harness-memory scope."),
        ("text", "Reject Generated by [Claude Code] footers.\nhttps://claude.ai/docs"),
        ("text", "Generated by the build, not an assistant."),
        ("name", "jbailes"),
        ("email", "jbailes@gmail.com"),
        ("email", "noreply@github.com"),
    ]

    def refuses(kind: str, value: str) -> bool:
        if kind == "text":
            return bool(text_findings("x", value))
        if kind == "name":
            return bool(identity_findings("x", value, ""))
        return bool(identity_findings("x", "", value))

    bad = [f"not refused: {kind} {value!r}" for kind, value in refused if not refuses(kind, value)]
    bad += [f"wrongly refused: {kind} {value!r}" for kind, value in allowed if refuses(kind, value)]
    if bad:
        print("check_no_attribution self-test failed:", *bad, sep="\n  ", file=sys.stderr)
        return 1
    print(f"check_no_attribution self-test: ok ({len(refused)} refused, {len(allowed)} allowed)")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        sys.exit(self_test())
    if len(sys.argv) > 1:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(check_event())
