#!/usr/bin/env python3
"""
Send a CI failure notification to Slack via an incoming webhook.

Usage:
    send_slack_notification.py --webhook-url <URL> --workflow-name <NAME> \
      --conclusion <CONCLUSION> --branch <BRANCH> --run-url <URL> \
      --repo <OWNER/REPO> --repo-url <URL> \
      [--summary-file <FILE>] [--tracker-result <RESULT>]

Requirements: python3 (stdlib only — no third-party packages)
"""
import argparse
import json
import sys
import urllib.request
import urllib.error


def build_payload(
    workflow_name: str,
    conclusion: str,
    branch: str,
    run_url: str,
    repo: str,
    repo_url: str,
    summary: str | None,
    tracker_result: str | None,
) -> dict:
    """Build a Slack Block Kit payload."""
    icon = ":hourglass:" if conclusion == "timed_out" else ":fire:"

    blocks: list[dict] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{icon} *{workflow_name}* {conclusion} on `{branch}`",
            },
        },
    ]

    # Failures detail section
    if tracker_result == "failure":
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":warning: Issue tracking failed — check the <{run_url}|workflow run> for details.",
            },
        })
    elif summary:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": summary,
            },
        })

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"<{repo_url}|{repo}> | <{run_url}|View Failure>",
            }
        ],
    })

    return {"blocks": blocks}


def send_to_slack(webhook_url: str, payload: dict) -> None:
    """POST a JSON payload to the Slack webhook."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            if resp.status != 200:
                print(f"Slack responded with status {resp.status}", file=sys.stderr)
                sys.exit(1)
    except urllib.error.HTTPError as e:
        print(f"Slack webhook failed: {e.code} {e.reason}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--webhook-url", required=True)
    parser.add_argument("--workflow-name", required=True)
    parser.add_argument("--conclusion", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--repo-url", required=True)
    parser.add_argument("--summary-file", default=None)
    parser.add_argument("--tracker-result", default=None)
    args = parser.parse_args()

    summary = None
    if args.summary_file:
        try:
            with open(args.summary_file) as f:
                summary = f.read().strip() or None
        except FileNotFoundError:
            pass

    payload = build_payload(
        workflow_name=args.workflow_name,
        conclusion=args.conclusion,
        branch=args.branch,
        run_url=args.run_url,
        repo=args.repo,
        repo_url=args.repo_url,
        summary=summary,
        tracker_result=args.tracker_result,
    )

    send_to_slack(args.webhook_url, payload)
    print("Slack notification sent.")


if __name__ == "__main__":
    main()
