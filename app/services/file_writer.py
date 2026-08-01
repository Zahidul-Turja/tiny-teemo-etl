import os
from typing import Any, Dict

import pandas as pd


class FileWriter:
    """Write a DataFrame to CSV, Excel, or Parquet."""

    def write(self, df: pd.DataFrame, output_path: str, fmt: str) -> Dict[str, Any]:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        fmt = fmt.lower()
        if fmt == "csv":
            df.to_csv(output_path, index=False)
        elif fmt == "excel":
            if not output_path.endswith((".xlsx", ".xls")):
                output_path += ".xlsx"
            df.to_excel(output_path, index=False, engine="openpyxl")
        elif fmt == "parquet":
            df.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {fmt}")

        size = os.path.getsize(output_path)
        return {
            "output_path": output_path,
            "rows_written": len(df),
            "file_size_bytes": size,
        }
