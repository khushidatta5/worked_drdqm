# import numpy as np
# import streamlit as st
# from scipy import stats
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, mean, stddev, when
# import pandas as pd
# import io
# import tempfile
# import os


# # 🧩 Force PySpark to run in local mode (no Hadoop needed)
# os.environ["SPARK_HOME"] = "C:\\projects\\gpt_ddqm_worked\\venv\\Lib\\site-packages\\pyspark"
# os.environ["HADOOP_HOME"] = "C:\\tmp"  # dummy path
# os.environ["PYSPARK_PYTHON"] = "python"
# os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.16+8"

# from pyspark.sql import SparkSession


# # --- Streamlit Page Setup ---
# st.set_page_config(page_title="DataGuard - Smart Data Cleaner", page_icon="🧼", layout="wide")
# st.title("🧼 DataGuard - Smart Data Cleaning and Quality Enhancer")

# st.write("""
# Upload your dataset (CSV or Excel).  
# Choose between **Pandas (for small data)** or **PySpark (for large data)** cleaning.  
# The app automatically:
# - Fills missing values
# - Removes duplicates
# - Handles outliers  
# """)

# # --- User Selection ---
# mode = st.radio("⚙️ Choose Processing Mode", ["Pandas (Small Data)", "PySpark (Big Data)"])

# uploaded_file = st.file_uploader("📂 Upload your dataset", type=["csv", "xlsx"])

# # --- Cached Spark Session ---
# @st.cache_resource
# def get_spark():
#     return SparkSession.builder \
#         .appName("DataGuardSpark") \
#         .master("local[*]") \
#         .config("spark.ui.showConsoleProgress", "false") \
#         .getOrCreate()

# # =====================================================
# # ===============  PANDAS CLEANING  ===================
# # =====================================================
# def clean_with_pandas(uploaded_file):
#     if uploaded_file.name.endswith('.csv'):
#         df = pd.read_csv(uploaded_file)
#     else:
#         df = pd.read_excel(uploaded_file)

#     st.success("✅ File uploaded successfully!")
#     st.subheader("📊 Raw Dataset Preview")
#     st.dataframe(df.head())

#     original_shape = df.shape

#     # ---- Step 1: Handle Missing Values ----
#     st.subheader("🩹 Step 1: Handling Missing Values")
#     for col in df.columns:
#         if df[col].isnull().sum() > 0:
#             if df[col].dtype in ['int64', 'float64']:
#                 df[col].fillna(df[col].mean(), inplace=True)
#             else:
#                 df[col].fillna(df[col].mode()[0], inplace=True)
#     st.success("✅ Missing values handled.")

#     # ---- Step 2: Remove Duplicates ----
#     st.subheader("🧾 Step 2: Removing Duplicate Rows")
#     dup_count = df.duplicated().sum()
#     if dup_count > 0:
#         df.drop_duplicates(inplace=True)
#         st.warning(f"Removed {dup_count} duplicate rows.")
#     else:
#         st.info("No duplicate rows found.")

#     # ---- Step 3: Handle Outliers ----
#     st.subheader("📉 Step 3: Detecting and Handling Outliers")
#     numeric_cols = df.select_dtypes(include=[np.number]).columns
#     outlier_count = 0
#     for col in numeric_cols:
#         z_scores = np.abs(stats.zscore(df[col]))
#         outlier_mask = z_scores > 3
#         outlier_count += np.sum(outlier_mask)
#         df.loc[outlier_mask, col] = df[col].median()

#     st.warning(f"Detected and replaced {outlier_count} outliers.")
#     st.success("✅ Outlier handling complete.")

#     # ---- Step 4: Summary ----
#     st.subheader("📋 Summary")
#     cleaned_shape = df.shape
#     st.write(f"🧮 **Original Shape:** {original_shape}")
#     st.write(f"📏 **Cleaned Shape:** {cleaned_shape}")

#     # ---- Step 5: Download ----
#     st.subheader("💾 Download Cleaned Data")
#     buffer = io.BytesIO()
#     df.to_csv(buffer, index=False)
#     buffer.seek(0)
#     st.download_button("⬇️ Download Cleaned CSV", buffer, file_name="cleaned_data.csv", mime="text/csv")

#     st.success("🎉 Data cleaned using Pandas!")


# def clean_with_pyspark(uploaded_file):
#     # --- Initialize Spark ---
#     spark = SparkSession.builder \
#         .appName("DataCleanerSpark") \
#         .config("spark.ui.showConsoleProgress", "false") \
#         .getOrCreate()

#     st.set_page_config(page_title="PySpark Data Cleaner", page_icon="🔥", layout="wide")
#     st.title("🔥 Big Data Cleaner with PySpark")

#     try:
#         st.info("Loading dataset using PySpark...")

#         # --- Save uploaded file to a temporary location ---
#         with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
#             tmp_file.write(uploaded_file.getbuffer())
#             tmp_path = tmp_file.name

#         # --- Load CSV into Spark ---
#         df = spark.read.option("header", True).option("inferSchema", True).csv(tmp_path)

#         total_rows = df.count()
#         total_cols = len(df.columns)
#         st.success(f"✅ Loaded {total_rows:,} rows and {total_cols} columns")

#         # --- Preview ---
#         st.subheader("📊 Raw Data Preview")
#         st.dataframe(df.limit(10).toPandas())

#         # --- Step 1: Handle Missing Values ---
#         st.subheader("🩹 Step 1: Handling Missing Values")
#         numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ["double", "int", "float", "long"]]
#         string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]

#         # Fill numeric columns with mean
#         for col_name in numeric_cols:
#             mean_val = df.select(mean(col(col_name))).first()[0]
#             if mean_val is not None:
#                 df = df.fillna({col_name: mean_val})

#         # Fill string columns with mode
#         for col_name in string_cols:
#             mode_val = df.groupBy(col_name).count().orderBy("count", ascending=False).first()
#             if mode_val:
#                 df = df.fillna({col_name: mode_val[0]})

#         st.success("✅ Missing values handled (mean/mode imputation).")

#         # --- Step 2: Remove Duplicates ---
#         st.subheader("🧾 Step 2: Removing Duplicates")
#         before = df.count()
#         df = df.dropDuplicates()
#         after = df.count()
#         st.info(f"Removed {before - after:,} duplicate rows.")

#         # --- Step 3: Handle Outliers ---
#         st.subheader("📉 Step 3: Handling Outliers (Z-score capping)")
#         for col_name in numeric_cols:
#             stats = df.select(mean(col(col_name)).alias("mean"), stddev(col(col_name)).alias("std")).collect()[0]
#             mean_val, std_val = stats["mean"], stats["std"]
#             if std_val and std_val > 0:
#                 lower, upper = mean_val - 3 * std_val, mean_val + 3 * std_val
#                 df = df.withColumn(
#                     col_name,
#                     when(col(col_name) < lower, lower)
#                     .when(col(col_name) > upper, upper)
#                     .otherwise(col(col_name))
#                 )

#         st.success("✅ Outliers handled (capped at ±3σ).")

#         # --- Step 4: Summary ---
#         st.subheader("📋 Summary")
#         st.write(f"**Final Shape:** {df.count():,} rows × {len(df.columns)} columns")

#         # --- Step 5: Download Cleaned Data ---
#         st.subheader("💾 Download Cleaned Data")
#         pandas_df = df.limit(1000000).toPandas()  # convert only up to 1M rows for safety
#         buffer = io.BytesIO()
#         pandas_df.to_csv(buffer, index=False)
#         buffer.seek(0)
#         st.download_button(
#             label="⬇️ Download Cleaned CSV",
#             data=buffer,
#             file_name="cleaned_dataset_pyspark.csv",
#             mime="text/csv"
#         )

#         st.success("🎉 Data cleaned successfully with PySpark!")

#     except Exception as e:
#         st.error(f"❌ Error: {str(e)}")

#     finally:
#         # Cleanup temp file
#         if 'tmp_path' in locals() and os.path.exists(tmp_path):
#             os.remove(tmp_path)


# # =====================================================
# # ==================  MAIN APP FLOW  ==================
# # =====================================================
# if uploaded_file:
#     if mode == "Pandas (Small Data)":
#         clean_with_pandas(uploaded_file)
#     else:
#         clean_with_pyspark(uploaded_file)


# import os
# import io
# import tempfile
# import numpy as np
# import pandas as pd
# import streamlit as st
# from scipy import stats
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, mean, stddev, when

# # =====================================================
# # 🧩 Force PySpark to work without Hadoop (Windows Safe)
# # =====================================================
# os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16+8"
# os.environ["HADOOP_HOME"] = r"C:\tmp"
# os.environ["SPARK_HOME"] = r"C:\projects\gpt_ddqm_worked\venv\Lib\site-packages\pyspark"
# os.environ["PYSPARK_PYTHON"] = "python"

# # Append required paths
# os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")
# os.environ["PATH"] += os.pathsep + os.path.join(os.environ["SPARK_HOME"], "bin")

# # =====================================================
# # --- Streamlit Page Setup ---
# # =====================================================
# st.set_page_config(page_title="DataGuard - Smart Data Cleaner", page_icon="🧼", layout="wide")
# st.title("🧼 DataGuard - Smart Data Cleaning and Quality Enhancer")

# st.write("""
# Upload your dataset (CSV or Excel).  
# Choose between **Pandas (for small data)** or **PySpark (for large data)** cleaning.  
# The app automatically:
# - Fills missing values
# - Removes duplicates
# - Handles outliers  
# """)

# # =====================================================
# # --- User Mode Selection ---
# # =====================================================
# mode = st.radio("⚙️ Choose Processing Mode", ["Pandas (Small Data)", "PySpark (Big Data)"])
# uploaded_file = st.file_uploader("📂 Upload your dataset", type=["csv", "xlsx"])

# # =====================================================
# # --- Cached Spark Session ---
# # =====================================================
# @st.cache_resource
# def get_spark():
#     spark = SparkSession.builder \
#         .appName("DataGuardSpark") \
#         .master("local[*]") \
#         .config("spark.ui.showConsoleProgress", "false") \
#         .getOrCreate()
#     st.info(f"✅ Spark initialized — Version: {spark.version}")
#     return spark

# # =====================================================
# # ===============  PANDAS CLEANING  ===================
# # =====================================================
# def clean_with_pandas(uploaded_file):
#     if uploaded_file.name.endswith('.csv'):
#         df = pd.read_csv(uploaded_file)
#     else:
#         df = pd.read_excel(uploaded_file)

#     st.success("✅ File uploaded successfully!")
#     st.subheader("📊 Raw Dataset Preview")
#     st.dataframe(df.head())

#     original_shape = df.shape

#     # ---- Step 1: Handle Missing Values ----
#     st.subheader("🩹 Step 1: Handling Missing Values")
#     for col in df.columns:
#         if df[col].isnull().sum() > 0:
#             if df[col].dtype in ['int64', 'float64']:
#                 df[col] = df[col].fillna(df[col].mean())
#             else:
#                 df[col] = df[col].fillna(df[col].mode()[0])
#     st.success("✅ Missing values handled.")

#     # ---- Step 2: Remove Duplicates ----
#     st.subheader("🧾 Step 2: Removing Duplicate Rows")
#     dup_count = df.duplicated().sum()
#     if dup_count > 0:
#         df = df.drop_duplicates()
#         st.warning(f"Removed {dup_count} duplicate rows.")
#     else:
#         st.info("No duplicate rows found.")

#     # ---- Step 3: Handle Outliers ----
#     st.subheader("📉 Step 3: Detecting and Handling Outliers")
#     numeric_cols = df.select_dtypes(include=[np.number]).columns
#     outlier_count = 0
#     for col in numeric_cols:
#         z_scores = np.abs(stats.zscore(df[col]))
#         outlier_mask = z_scores > 3
#         outlier_count += np.sum(outlier_mask)
#         df.loc[outlier_mask, col] = df[col].median()

#     st.warning(f"Detected and replaced {outlier_count} outliers.")
#     st.success("✅ Outlier handling complete.")

#     # ---- Step 4: Summary ----
#     st.subheader("📋 Summary")
#     cleaned_shape = df.shape
#     st.write(f"🧮 **Original Shape:** {original_shape}")
#     st.write(f"📏 **Cleaned Shape:** {cleaned_shape}")

#     # ---- Step 5: Download ----
#     st.subheader("💾 Download Cleaned Data")
#     buffer = io.BytesIO()
#     df.to_csv(buffer, index=False)
#     buffer.seek(0)
#     st.download_button("⬇️ Download Cleaned CSV", buffer, file_name="cleaned_data.csv", mime="text/csv")

#     st.success("🎉 Data cleaned using Pandas!")


# # =====================================================
# # ===============  PYSPARK CLEANING  ==================
# # =====================================================
# def clean_with_pyspark(uploaded_file):
#     try:
#         spark = get_spark()
#         st.info("Loading dataset using PySpark...")

#         # --- Save uploaded file temporarily ---
#         with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
#             tmp_file.write(uploaded_file.getbuffer())
#             tmp_path = tmp_file.name

#         # --- Load CSV into Spark ---
#         df = spark.read.option("header", True).option("inferSchema", True).csv(tmp_path)
#         total_rows = df.count()
#         total_cols = len(df.columns)
#         st.success(f"✅ Loaded {total_rows:,} rows and {total_cols} columns")

#         # --- Preview ---
#         st.subheader("📊 Raw Data Preview")
#         st.dataframe(df.limit(10).toPandas())

#         # --- Step 1: Handle Missing Values ---
#         st.subheader("🩹 Step 1: Handling Missing Values")
#         numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ["double", "int", "float", "long"]]
#         string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]

#         # Fill numeric columns with mean
#         for col_name in numeric_cols:
#             mean_val = df.select(mean(col(col_name))).first()[0]
#             if mean_val is not None:
#                 df = df.fillna({col_name: mean_val})

#         # Fill string columns with mode
#         for col_name in string_cols:
#             mode_val = df.groupBy(col_name).count().orderBy("count", ascending=False).first()
#             if mode_val:
#                 df = df.fillna({col_name: mode_val[0]})

#         st.success("✅ Missing values handled (mean/mode imputation).")

#         # --- Step 2: Remove Duplicates ---
#         st.subheader("🧾 Step 2: Removing Duplicates")
#         before = df.count()
#         df = df.dropDuplicates()
#         after = df.count()
#         st.info(f"Removed {before - after:,} duplicate rows.")

#         # --- Step 3: Handle Outliers ---
#         st.subheader("📉 Step 3: Handling Outliers (Z-score capping)")
#         for col_name in numeric_cols:
#             stats_data = df.select(mean(col(col_name)).alias("mean"), stddev(col(col_name)).alias("std")).collect()[0]
#             mean_val, std_val = stats_data["mean"], stats_data["std"]
#             if std_val and std_val > 0:
#                 lower, upper = mean_val - 3 * std_val, mean_val + 3 * std_val
#                 df = df.withColumn(
#                     col_name,
#                     when(col(col_name) < lower, lower)
#                     .when(col(col_name) > upper, upper)
#                     .otherwise(col(col_name))
#                 )
#         st.success("✅ Outliers handled (capped at ±3σ).")

#         # --- Step 4: Summary ---
#         st.subheader("📋 Summary")
#         st.write(f"**Final Shape:** {df.count():,} rows × {len(df.columns)} columns")

#         # --- Step 5: Download Cleaned Data ---
#         st.subheader("💾 Download Cleaned Data")
#         pandas_df = df.limit(1000000).toPandas()
#         buffer = io.BytesIO()
#         pandas_df.to_csv(buffer, index=False)
#         buffer.seek(0)
#         st.download_button(
#             label="⬇️ Download Cleaned CSV",
#             data=buffer,
#             file_name="cleaned_dataset_pyspark.csv",
#             mime="text/csv"
#         )

#         st.success("🎉 Data cleaned successfully with PySpark!")

#     except Exception as e:
#         st.error(f"❌ Error: {str(e)}")

#     finally:
#         if 'tmp_path' in locals() and os.path.exists(tmp_path):
#             os.remove(tmp_path)


# # =====================================================
# # ==================  MAIN APP FLOW  ==================
# # =====================================================
# if uploaded_file:
#     if mode == "Pandas (Small Data)":
#         clean_with_pandas(uploaded_file)
#     else:
#         clean_with_pyspark(uploaded_file)





import os
import io
import tempfile
import numpy as np
import pandas as pd
import streamlit as st
from scipy import stats
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, when

# ============================================
# 🧩 Force PySpark to run in local mode
# ============================================
os.environ["SPARK_HOME"] = "C:\\projects\\gpt_ddqm_worked\\venv\\Lib\\site-packages\\pyspark"
os.environ["HADOOP_HOME"] = "C:\\tmp"  # dummy path
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.16.8-hotspot"

# ============================================
# 🧼 Streamlit Setup
# ============================================
st.set_page_config(page_title="DataGuard - Smart Data Cleaner", page_icon="🧼", layout="wide")
st.title("🧼 DataGuard - Smart Data Cleaning and Quality Enhancer")

st.write("""
Upload your dataset (CSV or Excel).  
Choose between **Pandas (for small data)** or **PySpark (for large data)** cleaning.
The app will automatically:
- Fill missing values  
- Remove duplicates  
- Handle outliers  
""")

mode = st.radio("⚙️ Choose Processing Mode", ["Pandas (Small Data)", "PySpark (Big Data)"])
uploaded_file = st.file_uploader("📂 Upload your dataset", type=["csv", "xlsx"])

# ============================================
# 🪄 Cache Spark Session
# ============================================
@st.cache_resource
def get_spark():
    return (
        SparkSession.builder
        .appName("DataGuardSpark")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

# ============================================
# 🧮 Pandas Cleaning Function
# ============================================
def clean_with_pandas(uploaded_file):
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    st.success("✅ File uploaded successfully!")
    st.subheader("📊 Raw Dataset Preview")
    st.dataframe(df.head())

    # --- Handle Missing Values ---
    for c in df.columns:
        if df[c].isnull().sum() > 0:
            if df[c].dtype in ["float64", "int64"]:
                df[c].fillna(df[c].mean(), inplace=True)
            else:
                df[c].fillna(df[c].mode()[0], inplace=True)
    st.success("✅ Missing values handled.")

    # --- Remove Duplicates ---
    dup = df.duplicated().sum()
    if dup > 0:
        df.drop_duplicates(inplace=True)
        st.warning(f"Removed {dup} duplicate rows.")
    else:
        st.info("No duplicate rows found.")

    # --- Handle Outliers ---
    num_cols = df.select_dtypes(include=[np.number]).columns
    out_count = 0
    for c in num_cols:
        z = np.abs(stats.zscore(df[c]))
        mask = z > 3
        out_count += np.sum(mask)
        df.loc[mask, c] = df[c].median()
    st.warning(f"Replaced {out_count} outliers.")

    # --- Download Cleaned Data ---
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    st.download_button("⬇️ Download Cleaned CSV", buf, "cleaned_data.csv", "text/csv")

# ============================================
# 🔥 PySpark Cleaning Function
# ============================================
# def clean_with_pyspark(uploaded_file):
#     spark = get_spark()
#     st.info("🚀 Initializing PySpark...")

#     try:
#         # ✅ Save uploaded file safely
#         with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
#             tmp.write(uploaded_file.getvalue())
#             tmp_path = tmp.name

#         # ✅ Read into Spark DataFrame
#         df = (
#             spark.read.option("header", True)
#             .option("inferSchema", True)
#             .csv(tmp_path)
#         )

#         st.success(f"✅ Loaded {df.count():,} rows and {len(df.columns)} columns")

#         # --- Handle Missing Values ---
#         num_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ["double", "int", "float", "long"]]
#         str_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]

#         for c in num_cols:
#             mean_val = df.select(mean(col(c))).first()[0]
#             if mean_val is not None:
#                 df = df.fillna({c: mean_val})
#         for c in str_cols:
#             mode_val = df.groupBy(c).count().orderBy("count", ascending=False).first()
#             if mode_val:
#                 df = df.fillna({c: mode_val[0]})
#         st.success("✅ Missing values handled.")

#         # --- Remove Duplicates ---
#         before = df.count()
#         df = df.dropDuplicates()
#         after = df.count()
#         st.info(f"Removed {before - after:,} duplicates.")

#         # --- Handle Outliers ---
#         for c in num_cols:
#             s = df.select(mean(col(c)).alias("m"), stddev(col(c)).alias("s")).first()
#             if s["s"] and s["s"] > 0:
#                 lower, upper = s["m"] - 3 * s["s"], s["m"] + 3 * s["s"]
#                 df = df.withColumn(
#                     c,
#                     when(col(c) < lower, lower)
#                     .when(col(c) > upper, upper)
#                     .otherwise(col(c))
#                 )
#         st.success("✅ Outliers handled.")

#         # --- Convert to Pandas (limit to avoid memory overload) ---
#         st.subheader("📊 Preview Cleaned Data")
#         pandas_df = df.limit(5000).toPandas()
#         st.dataframe(pandas_df.head())

#         # --- Download Cleaned Data ---
#         buffer = io.BytesIO()
#         pandas_df.to_csv(buffer, index=False)
#         buffer.seek(0)
#         st.download_button(
#             label="⬇️ Download Cleaned CSV",
#             data=buffer,
#             file_name="cleaned_dataset_pyspark.csv",
#             mime="text/csv"
#         )

#     except Exception as e:
#         st.error(f"❌ Error: {str(e)}")

#     finally:
#         if "tmp_path" in locals() and os.path.exists(tmp_path):
#             os.remove(tmp_path)



def clean_with_pyspark(uploaded_file):
    import shutil

    # --- Initialize Spark with more memory and faster I/O ---
    spark = SparkSession.builder \
        .appName("DataCleanerSpark") \
        .master("local[*]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

    st.title("🔥 Big Data Cleaner with PySpark")

    try:
        st.info("📂 Loading dataset using PySpark...")

        # --- Save uploaded file temporarily ---
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
            tmp_file.write(uploaded_file.getbuffer())
            tmp_path = tmp_file.name

        # --- Load CSV into Spark ---
        df = spark.read.option("header", True).option("inferSchema", True).csv(tmp_path)

        total_rows = df.count()
        total_cols = len(df.columns)
        st.success(f"✅ Loaded {total_rows:,} rows and {total_cols} columns")

        # --- Preview a few rows ---
        st.subheader("📊 Raw Data Preview")
        st.dataframe(df.limit(10).toPandas())

        # --- Step 1: Handle Missing Values ---
        st.subheader("🩹 Step 1: Handling Missing Values")
        numeric_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ["double", "int", "float", "long"]]
        string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]

        for col_name in numeric_cols:
            mean_val = df.select(mean(col(col_name))).first()[0]
            if mean_val is not None:
                df = df.fillna({col_name: mean_val})

        for col_name in string_cols:
            mode_val = df.groupBy(col_name).count().orderBy("count", ascending=False).first()
            if mode_val:
                df = df.fillna({col_name: mode_val[0]})

        st.success("✅ Missing values handled (mean/mode imputation).")

        # --- Step 2: Remove Duplicates ---
        st.subheader("🧾 Step 2: Removing Duplicates")
        before = df.count()
        df = df.dropDuplicates()
        after = df.count()
        st.info(f"Removed {before - after:,} duplicate rows.")

        # --- Step 3: Handle Outliers ---
        st.subheader("📉 Step 3: Handling Outliers (Z-score capping)")
        for col_name in numeric_cols:
            stats_row = df.select(mean(col(col_name)).alias("mean"), stddev(col(col_name)).alias("std")).collect()[0]
            mean_val, std_val = stats_row["mean"], stats_row["std"]
            if std_val and std_val > 0:
                lower, upper = mean_val - 3 * std_val, mean_val + 3 * std_val
                df = df.withColumn(
                    col_name,
                    when(col(col_name) < lower, lower)
                    .when(col(col_name) > upper, upper)
                    .otherwise(col(col_name))
                )

        st.success("✅ Outliers handled (capped at ±3σ).")

        # --- Step 4: Summary ---
        st.subheader("📋 Summary")
        st.write(f"**Final Shape:** {df.count():,} rows × {len(df.columns)} columns")

        # --- Step 5: Download Cleaned Data ---
        st.subheader("💾 Download Cleaned Data")

        output_dir = tempfile.mkdtemp()
        output_csv_path = os.path.join(output_dir, "cleaned_data")

        df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_csv_path)

        # Find actual file name Spark wrote
        actual_csv = None
        for root, dirs, files in os.walk(output_csv_path):
            for f in files:
                if f.endswith(".csv"):
                    actual_csv = os.path.join(root, f)
                    break

        if actual_csv:
            with open(actual_csv, "rb") as f:
                st.download_button(
                    label="⬇️ Download Cleaned CSV (via Spark)",
                    data=f,
                    file_name="cleaned_dataset_pyspark.csv",
                    mime="text/csv"
                )

        st.success("🎉 Data cleaned successfully with PySpark!")

    except Exception as e:
        st.error(f"❌ Error: {str(e)}")

    finally:
        # Cleanup temp files
        if 'tmp_path' in locals() and os.path.exists(tmp_path):
            os.remove(tmp_path)
        if 'output_dir' in locals() and os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)

# # ============================================
# 🏁 Main Logic
# ============================================
if uploaded_file:
    if mode == "Pandas (Small Data)":
        clean_with_pandas(uploaded_file)
    else:
        clean_with_pyspark(uploaded_file)
