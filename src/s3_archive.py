import json
import os
from datetime import datetime, timezone
from typing import List, Dict, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()


def upload_bronze_snapshot(symbol: str, records: List[Dict]) -> Tuple[bool, str]:
    """Upload a raw Bronze API snapshot to S3 as a JSON file.

    Returns (success, detail) where detail is the S3 key on success
    or a short reason string on failure.  Failures are non-blocking;
    the caller should log and continue.
    """
    bucket = os.getenv("AWS_S3_BUCKET")
    region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not bucket:
        return False, "missing_bucket_env_var"

    if not records:
        return False, "no_records"

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"bronze_snapshots/symbol={symbol}/snapshot_{timestamp_str}.json"

    try:
        s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        body = json.dumps(records, default=str, indent=2).encode("utf-8")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        return True, key
    except (BotoCoreError, ClientError) as exc:
        return False, str(exc)
