FROM apache/airflow:2.10.3-python3.11
USER root
RUN apt-get update && apt-get install -y default-jdk && apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow
RUN pip install pyspark==3.5.0 python-dotenv==1.0.1