import base64
import time
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd

from app.models.schemas import APIDestination


class APIWriter:
    """
    Write DataFrame rows to a REST API endpoint in batches.
    Supports bearer token, basic auth, and API-key header auth.
    """

    def __init__(
        self,
        destination: APIDestination,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        self.dest = destination
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def write(self, df: pd.DataFrame) -> Dict[str, Any]:
        records = df.to_dict(orient="records")
        batch_size = self.dest.batch_size
        total = len(records)
        sent = 0
        failed = 0
        errors: List[str] = []

        headers = dict(self.dest.headers or {})
        headers["Content-Type"] = "application/json"
        headers.update(self._auth_headers())

        with httpx.Client(timeout=30) as client:
            for i in range(0, total, batch_size):
                batch = records[i : i + batch_size]
                payload = (
                    {self.dest.records_key: batch} if self.dest.records_key else batch
                )

                success = False
                for attempt in range(1, self.max_retries + 1):
                    try:
                        resp = client.request(
                            method=self.dest.method,
                            url=self.dest.url,
                            json=payload,
                            headers=headers,
                        )
                        resp.raise_for_status()
                        sent += len(batch)
                        success = True
                        break
                    except httpx.HTTPStatusError as exc:
                        if attempt < self.max_retries:
                            time.sleep(self.retry_delay * attempt)
                        else:
                            errors.append(
                                f"Batch {i//batch_size + 1}: HTTP {exc.response.status_code}"
                            )
                    except Exception as exc:
                        if attempt < self.max_retries:
                            time.sleep(self.retry_delay * attempt)
                        else:
                            errors.append(f"Batch {i//batch_size + 1}: {exc}")

                if not success:
                    failed += len(batch)

        return {
            "total_records": total,
            "sent": sent,
            "failed": failed,
            "errors": errors,
        }

    def _auth_headers(self) -> Dict[str, str]:
        auth = self.dest.auth
        if not auth:
            return {}

        if auth.type == "bearer" and auth.token:
            return {"Authorization": f"Bearer {auth.token}"}

        if auth.type == "basic" and auth.username and auth.password:
            creds = base64.b64encode(
                f"{auth.username}:{auth.password}".encode()
            ).decode()
            return {"Authorization": f"Basic {creds}"}

        if auth.type == "api_key" and auth.api_key:
            header_name = auth.header_name or "X-API-Key"
            return {header_name: auth.api_key}

        return {}
