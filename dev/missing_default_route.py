#!/usr/bin/env python3
"""List providers whose observations failed routing, grouped by integration.

The routing tracebacks in Cloud Logging carry no integration id, so the
provider is recovered from Cloud Trace instead: the root
`gundi_api.process_observation` span carries integration_id / integration_name,
and the `routing_service.transform_and_route_observation` child span carries
the error.

Usage:
  python3 missing_default_route.py [--hours 24] [--project cdip-prod1-78ca]
  python3 missing_default_route.py --start 2026-09-17T20:00:00Z --end 2026-09-18T13:00:00Z

Auth: uses `gcloud auth print-access-token`, so `gcloud auth login` first.

Filters (--filter):
  default, pre-PR-156 (the AttributeError crash):
    span:routing_service.transform_and_route_observation error:Unexpected
  after PR #156 merges (the deliberate discard path):
    span:routing_service.transform_and_route_observation error:Connection
"""
import argparse, json, subprocess, sys, urllib.parse, urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone

DEFAULT_FILTER = "span:routing_service.transform_and_route_observation error:Unexpected"

p = argparse.ArgumentParser()
p.add_argument("--project", default="cdip-prod1-78ca")
p.add_argument("--hours", type=float, default=24.0)
p.add_argument("--start")
p.add_argument("--end")
p.add_argument("--filter", default=DEFAULT_FILTER)
p.add_argument("--max-pages", type=int, default=20)
a = p.parse_args()

now = datetime.now(timezone.utc)
start = a.start or (now - timedelta(hours=a.hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
end = a.end or now.strftime("%Y-%m-%dT%H:%M:%SZ")

token = subprocess.run(["gcloud", "auth", "print-access-token"],
                       capture_output=True, text=True).stdout.strip()
if not token:
    sys.exit("No access token. Run: gcloud auth login")

base = f"https://cloudtrace.googleapis.com/v1/projects/{a.project}/traces"
first, last, cnt, name_of, streams = {}, {}, Counter(), {}, {}
page, pages = None, 0
while pages < a.max_pages:
    q = {"filter": a.filter, "startTime": start, "endTime": end,
         "pageSize": "100", "view": "COMPLETE"}
    if page:
        q["pageToken"] = page
    req = urllib.request.Request(base + "?" + urllib.parse.urlencode(q),
                                 headers={"Authorization": "Bearer " + token})
    try:
        d = json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        sys.exit(f"Cloud Trace API error {e.code}: {e.read().decode()[:300]}")
    for t in d.get("traces", []):
        iid = nm = obs = ts = None
        for s in t.get("spans", []):
            L = s.get("labels", {}) or {}
            iid = iid or L.get("integration_id")
            nm = nm or L.get("integration_name")
            obs = obs or L.get("observation_type")
            st = s.get("startTime")
            if st and (ts is None or st < ts):
                ts = st
        if iid and ts:
            cnt[iid] += 1
            name_of.setdefault(iid, nm)
            streams.setdefault(iid, Counter())[obs or "?"] += 1
            if iid not in first or ts < first[iid]:
                first[iid] = ts
            if iid not in last or ts > last[iid]:
                last[iid] = ts
    pages += 1
    page = d.get("nextPageToken")
    if not page:
        break

print(f"window : {start} .. {end}")
print(f"filter : {a.filter}")
print(f"result : {sum(cnt.values())} failing traces, {len(cnt)} providers"
      f"{' (PAGE CAP HIT - counts are a lower bound)' if page else ''}\n")
if not cnt:
    print("No failing traces in this window.")
    sys.exit(0)
print(f"{'count':>6}  {'first failure':<25} {'last failure':<25} {'streams':<12} provider")
for iid, c in cnt.most_common():
    sm = ",".join(f"{k}:{v}" for k, v in streams[iid].most_common())
    print(f"{c:>6}  {first[iid][:19]+'Z':<25} {last[iid][:19]+'Z':<25} {sm:<12} "
          f"{(name_of[iid] or '(unnamed)')[:34]}  {iid}")
