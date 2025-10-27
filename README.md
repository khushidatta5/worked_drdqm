# DataGuard - Automatic Preprocessing (Streamlit)

1. Create virtualenv (recommended):
   python -m venv venv
   Activate:
     venv\Scripts\activate   (Windows)
     source venv/bin/activate  (macOS/Linux)

2. Install:
   pip install -r requirements.txt

3. Run:
   python -m streamlit run app.py

4. Open http://localhost:8501

Features:
- Upload a single raw CSV file
- Configure preprocessing options
- Run automated pipeline (rename columns, remove duplicates, impute missing, encode categorical, scale numeric, remove outliers)
- Download cleaned CSV and view/save HTML preprocessing report
