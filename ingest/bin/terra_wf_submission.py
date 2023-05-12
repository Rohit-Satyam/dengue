#! /usr/bin/env python3

import os
import io
from firecloud import fiss
import firecloud.api as fapi
import pandas as pd
import re
import json
import collections
import subprocess, sys, os, re, argparse, textwrap
import csv
import logging
import time

# ========= Terra API
# "https://api.firecloud.org"
terra_namespace = "Nextstrain"
terra_workspace = "Development"
ws_bucket = "gs://fc-29a44672-3ae3-4dc8-8c5c-e0c71c36fda3/"

# ========= Inputs
data_table_name = "ncov_examples"
workflow_namespace = "Custom_Workspace"
workflow_name = "DENGUE_BUILD"


# ========= Get list of samples to run
stat_code = ""
while stat_code != 200:
    try:
        run_contents = fapi.get_entities(
            terra_namespace, terra_workspace, data_table_name
        )
        stat_code = run_contents.status_code
    except Exception as ex:
        print("exception", ex, file=sys.stdout)
        logging.exception(ex)
        time.sleep(5)
        continue
    finally:
        print("status code: ", stat_code, file=sys.stdout)

run_contents = json.loads(run_contents.text)

# ========= Submit to Terra
i = 0
rows = []
for i in range(len(run_contents)):
    rows.append(run_contents[i]["name"])

i = 0
submission_results = []

# === Run Once
stat_code = ""
while stat_code != 201:
    try:
        # https://github.com/broadinstitute/fiss/blob/0cb8dbb74269faa91aa05460421cafa8dadc9025/firecloud/api.py#L1190
        print("submitting:", workflow_namespace, "/", workflow_name)
        submission_results.append(
            fapi.create_submission(
                terra_namespace,
                terra_workspace,
                workflow_namespace,
                workflow_name,
                etype=data_table_name,
                entity=rows[i],
                use_callcache=False
            )
        )
        stat_code = submission_results[i].status_code
        break
    except Exception as ex:
        print("exception", ex, file=sys.stdout)
        logging.exception(ex)
        time.sleep(5)
        continue
    finally:
        print("status code: ", stat_code, file=sys.stdout)
