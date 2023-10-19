FROM apache/airflow:2.7.1 
USER root
# Install OpenJDK-11 and Hadoop AWS dependencies
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install -y wget && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Download Hadoop AWS dependencies
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.995/aws-java-sdk-1.11.995.jar
RUN wget -P /opt/bitnami/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar

USER airflow
ADD requirements.txt . 
ADD .env .
RUN pip install apache-airflow==2.7.1 -r requirements.txt