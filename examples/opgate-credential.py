#!/usr/bin/env python3
"""Run only as an opagent child. Output goes to the proxy's private pipe."""
import json
import os
import sys

prefix = sys.argv[1]
# The vault contains fresh tokens. A rotating OAuth broker can replace this
# adapter and use TEAMCODEX_REFRESH=1 to force renewal before returning JSON.
token = {
    "access_token": os.environ[prefix + "_ACCESS_TOKEN"],
    "account_id": os.environ[prefix + "_ACCOUNT_ID"],
}
expiry = os.environ.get(prefix + "_EXPIRES_AT")
if expiry:
    token["expires_at"] = int(expiry)
sys.stdout.write(json.dumps(token))
