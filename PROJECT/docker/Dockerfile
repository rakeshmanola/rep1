FROM python:3.9

# -----------------------------------------Install required packages--------------------------------------
RUN apt update && apt install -y openjdk-17-jdk wget && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV AIRFLOW_HOME=/opt/airflow

# ------------------------------------Load Kaggle credentials from environment variables ---------------------------
ARG KAGGLE_USERNAME
ARG KAGGLE_KEY
ENV KAGGLE_USERNAME=${KAGGLE_USERNAME}
ENV KAGGLE_KEY=${KAGGLE_KEY}

# -------------------------------Working Directory ------------------------------------------
WORKDIR /app

#  --------------------------------------Install Python dependencies -----------------------------------
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install -r /app/requirements.txt \
    && pip install kaggle pyspark boto3 pyarrow matplotlib seaborn tableauserverclient pyathena \
    && pip install --no-cache-dir --upgrade tableauhyperapi

# ----------------------------------Set Hadoop environment ------------------------------
ENV HADOOP_HOME=/opt/hadoop-3.3.4
ENV PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"


# ------------------------------------ Copy project files ----------------------------------------------------
COPY script/ /app/script/
COPY data/ /app/data/
COPY logs/ /app/logs/
COPY dags/ /opt/airflow/dags/
COPY docker-compose.yaml /app/
COPY Dockerfile /app/
COPY requirements.txt /app/
COPY etc/ /app/

# -------------------------------------Handle Kaggle authentication ---------------------------------------------------
COPY kaggle.json /root/.kaggle/kaggle.json
RUN mkdir -p /root/.kaggle && chmod 600 /root/.kaggle/kaggle.json
CMD bash -c "exec python /app/script/1_extract_kaggle.py && python /app/script/2_load_raw_s3.py && python /app/script/3_transform_process.py && python /app/script/4_load_to_athena.py && python /app/script/5_validate_logic.py && python /app/script/6_store_validated_s3.py && python /app/script/7_visualize_tableau.py"

