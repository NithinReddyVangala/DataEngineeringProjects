# 🏎️ Formula 1 Data Engineering Project

This project focuses on extracting Formula 1 racing data from public APIs and loading it into a PostgreSQL database for structured analysis.

## 📊 Data Flow

- The Jupyter notebook `Formula1.ipynb` handles API calls to fetch race data.
- Retrieved data is first stored in a **staging schema** within PostgreSQL.
- After basic transformations and cleaning, the data is moved to a **production (PROD) schema** for analysis.

## 🚀 Future Enhancements

- Integrate **Apache Airflow** to automate API calls and data ingestion workflows.
- Automate SQL script execution using **psycopg** for more seamless updates.
- Optimize the pipeline to **insert/update data incrementally** after each race instead of replacing entire tables.

