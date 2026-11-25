FROM python:3.10-slim

RUN pip install --no-cache-dir \
      "streamlit>=1.39.0" \
      "duckdb>=1.1.0" \
      "pandas>=2.2.0" \
      "pyarrow>=17.0.0" \
      "plotly>=5.24.0"

# Pre-install the DuckDB Delta extension during image build so the UI works offline.
RUN python -c "import duckdb; duckdb.sql('INSTALL delta; LOAD delta;')"

WORKDIR /opt/workspace
ENV PYTHONPATH=/opt/workspace
EXPOSE 8501

CMD ["streamlit", "run", "reporting/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
