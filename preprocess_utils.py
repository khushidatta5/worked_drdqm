# preprocess_utils.py
import pandas as pd
import numpy as np
import re
import os
from io import BytesIO
from sklearn.preprocessing import StandardScaler

def load_dataframe_from_file(file_obj, low_memory=True):
    """
    Load CSV uploaded by user into pandas DataFrame.
    For very large files, consider reading with chunksize (not done automatically here).
    """
    # try default first
    try:
        df = pd.read_csv(file_obj, low_memory=low_memory)
    except Exception:
        # fallback to python engine and utf-8
        df = pd.read_csv(file_obj, engine="python", encoding="utf-8", low_memory=low_memory)
    return df

# ---------- helpers ----------
def to_snake_case(name: str):
    name = str(name)
    name = name.strip()
    # replace spaces and punctuation with underscores
    name = re.sub(r"[^\w]+", "_", name)
    # collapse multiple underscores
    name = re.sub(r"__+", "_", name)
    name = name.strip("_")
    return name.lower()

def basic_summary(df: pd.DataFrame):
    summary = {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "missing_per_column": df.isnull().sum().to_dict(),
        "sample_head": df.head(5).to_dict(orient="records")
    }
    return summary

# ---------- preprocessing steps ----------
def rename_columns_snakecase(df: pd.DataFrame):
    orig = list(df.columns)
    new = [to_snake_case(c) for c in orig]
    df = df.copy()
    df.columns = new
    mapping = dict(zip(orig, new))
    return df, mapping

def remove_duplicates(df: pd.DataFrame):
    before = df.shape[0]
    df2 = df.drop_duplicates()
    after = df2.shape[0]
    removed = before - after
    return df2, removed

def impute_missing(df: pd.DataFrame):
    """
    Numeric: fill with mean
    Categorical (object / category): fill with mode (most frequent)
    Return df and a dict of what was imputed.
    """
    df = df.copy()
    imputed = {"numeric": {}, "categorical": {}}
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            if df[col].isnull().any():
                mean_val = df[col].mean()
                df[col] = df[col].fillna(mean_val)
                imputed["numeric"][col] = float(mean_val) if not pd.isna(mean_val) else None
        else:
            if df[col].isnull().any():
                mode_val = df[col].mode()
                fill_val = mode_val.iloc[0] if (not mode_val.empty) else ""
                df[col] = df[col].fillna(fill_val)
                imputed["categorical"][col] = str(fill_val)
    return df, imputed

def detect_and_remove_outliers_zscore(df: pd.DataFrame, z_thresh=3.0):
    """
    Remove rows where any numeric column has |zscore| > z_thresh.
    Returns cleaned df and number of rows removed.
    """
    df = df.copy()
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if len(numeric_cols) == 0:
        return df, 0
    arr = df[numeric_cols].values.astype(float)
    # compute mean and std per column
    mu = np.nanmean(arr, axis=0)
    sigma = np.nanstd(arr, axis=0, ddof=0)
    # avoid division by zero
    sigma_safe = np.where(sigma == 0, 1e-9, sigma)
    z = np.abs((arr - mu) / sigma_safe)
    mask = (z > z_thresh).any(axis=1)
    before = df.shape[0]
    df2 = df.loc[~mask].reset_index(drop=True)
    after = df2.shape[0]
    removed = int(before - after)
    return df2, removed

def encode_categorical_columns(df: pd.DataFrame, strategy="auto", onehot_threshold=20):
    """
    strategy: 'auto'|'label'|'onehot'
    - auto: if unique values <= onehot_threshold -> one-hot, else label encode (factorize)
    Returns transformed df and encoding metadata.
    """
    df = df.copy()
    encoding_info = {}
    cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
    for col in cat_cols:
        uniques = df[col].nunique(dropna=False)
        if strategy == "onehot" or (strategy == "auto" and uniques <= onehot_threshold):
            # one-hot encode
            dummies = pd.get_dummies(df[col].astype(str), prefix=col, dummy_na=False)
            df = pd.concat([df.drop(columns=[col]), dummies], axis=1)
            encoding_info[col] = {"method": "onehot", "new_columns": dummies.columns.tolist(), "unique_values": int(uniques)}
        else:
            # label encode using factorize (fast, memory-efficient)
            codes, uniques_values = pd.factorize(df[col].astype(str))
            df[col] = codes
            encoding_info[col] = {"method": "label", "mapping": {str(v): int(i) for i, v in enumerate(uniques_values)}, "unique_values": int(uniques)}
    return df, encoding_info

def scale_numeric_columns(df: pd.DataFrame):
    """
    Apply StandardScaler (zero mean, unit variance) to numeric columns.
    Returns scaled df and scaler metadata (mean/std).
    """
    df = df.copy()
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    scaler = StandardScaler()
    if numeric_cols:
        arr = df[numeric_cols].astype(float).values
        transformed = scaler.fit_transform(arr)
        df[numeric_cols] = transformed
        scaler_info = {"columns": numeric_cols, "mean": scaler.mean_.tolist(), "var": scaler.var_.tolist()}
    else:
        scaler_info = {"columns": []}
    return df, scaler_info

# ---------- pipeline ----------
def preprocess_dataframe(
    df: pd.DataFrame,
    run_remove_duplicates=True,
    run_rename_columns=True,
    run_impute_missing=True,
    run_outlier_removal=False,
    zscore_threshold=3.0,
    encoding_strategy="auto",
    onehot_threshold=20,
    run_scale_numeric=True
):
    """
    Run full preprocessing pipeline with options. Returns:
      processed_df, report_dict
    """
    report = {"steps": [], "before_summary": basic_summary(df)}
    working = df.copy()

    # rename columns
    if run_rename_columns:
        working, mapping = rename_columns_snakecase(working)
        report["steps"].append({"rename_columns": mapping})

    # remove duplicates
    if run_remove_duplicates:
        working, removed = remove_duplicates(working)
        report["steps"].append({"duplicates_removed": int(removed)})

    # impute missing values
    if run_impute_missing:
        working, imputed = impute_missing(working)
        report["steps"].append({"imputed": imputed})

    # detect and remove outliers
    if run_outlier_removal:
        working, out_removed = detect_and_remove_outliers_zscore(working, z_thresh=zscore_threshold)
        report["steps"].append({"outliers_removed": int(out_removed), "zscore_threshold": float(zscore_threshold)})

    # encode categorical columns
    working, encoding_info = encode_categorical_columns(working, strategy=encoding_strategy, onehot_threshold=onehot_threshold)
    report["steps"].append({"encoding": encoding_info})

    # scale numeric columns
    if run_scale_numeric:
        working, scaler_info = scale_numeric_columns(working)
        report["steps"].append({"scaler": scaler_info})

    report["after_summary"] = basic_summary(working)
    return working, report

# ---------- report generation ----------
def generate_preprocess_report_html(report: dict, title="Preprocessing Report", out_path=None):
    """
    Generate a simple HTML report summarizing the preprocessing steps and statistics.
    If out_path is provided, saves the HTML file and returns the path. Otherwise returns HTML string.
    """
    html_parts = []
    html_parts.append(f"<html><head><meta charset='utf-8'><title>{title}</title></head><body style='font-family:Arial,Helvetica,sans-serif;padding:20px;'>")
    html_parts.append(f"<h1>{title}</h1>")

    html_parts.append("<h2>Before summary</h2>")
    bs = report.get("before_summary", {})
    html_parts.append(f"<p>Rows: {bs.get('rows')} &nbsp; Columns: {bs.get('columns')}</p>")
    html_parts.append("<h3>Sample (first 5 rows)</h3>")
    html_parts.append(pd.DataFrame(bs.get("sample_head", [])).to_html(index=False))

    html_parts.append("<h2>Preprocessing steps</h2>")
    for step in report.get("steps", []):
        html_parts.append("<div style='border:1px solid #eee;padding:10px;margin:8px 0;border-radius:6px;background:#fafafa;'>")
        for k, v in step.items():
            html_parts.append(f"<h4>{k}</h4>")
            html_parts.append(f"<pre style='white-space:pre-wrap'>{pd.io.json.dumps(v, indent=2, default=str)}</pre>")
        html_parts.append("</div>")

    html_parts.append("<h2>After summary</h2>")
    af = report.get("after_summary", {})
    html_parts.append(f"<p>Rows: {af.get('rows')} &nbsp; Columns: {af.get('columns')}</p>")
    html_parts.append("<h3>Sample (first 5 rows)</h3>")
    html_parts.append(pd.DataFrame(af.get("sample_head", [])).to_html(index=False))
    html_parts.append("</body></html>")

    html = "\n".join(html_parts)
    if out_path:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(html)
        return out_path
    return html

# ---------- helper to create download bytes ----------
def dataframe_to_csv_bytes(df: pd.DataFrame):
    buf = BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return buf.read()
