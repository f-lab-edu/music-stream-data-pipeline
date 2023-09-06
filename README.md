# Streamify : music stream data pipeline

<img width="481" alt="streamify" src="https://github.com/f-lab-edu/music-stream-data-pipeline/assets/49158155/275e6353-ce95-417f-8b68-531b3443dff9">

Building a pipeline to process event data generated by a virtual music streaming site.

---

## Project Description

This project involves building a comprehensive data pipeline to reliably collect event logs generated by a virtual music streaming platform, preprocess and store the data, and provide the capability to visualize the stored data through a dashboard.

### System Architecture

<img width="738" alt="image" src="https://github.com/f-lab-edu/music-stream-data-pipeline/assets/49158155/36e43c79-164d-4496-a665-9753ef8f7ad5">

1. **Data Collection (Orange):**

   - Fluentd collects four types of event data.
   - The collected data is sent to the Data Lake.

2. **Data Processing in Silver Phase (Blue):**

   - In this phase, raw data (Bronze Phase) is filtered and processed.
   - The processed data is stored in the Data Lake in Parquet format.

3. **Data Processing in Gold Phase (Yellow):**

   - Aggregations are performed on the Fact Table.
   - Fact Table is merged with relevant Dimension Table for advanced analysis.

4. **Data Loading and Visualization (Black):**

   - The final processed data is ingested from the Data Lake to Druid.
   - Data is visualized by connecting Druid with Superset.

### Data

[Eventsim](https://github.com/Interana/eventsim) is a program designed to generate event data that simulates page requests for a fictional music website. The resulting data closely resembles real user interactions but is entirely synthetic. The Docker image for Eventsim is sourced from [viirya's fork](https://github.com/viirya/eventsim), which was originally used in the Streamify project [Streamify](https://github.com/ankurchavda/streamify).

Eventsim leverages song data from the [Million Songs Dataset](http://millionsongdataset.com) to generate these events. I used a subset of 10,000 songs from the Million Songs Dataset as the basis for simulating user interactions.

### Tools & Technologies

- Cloud - [**Naver Cloud Platform**](https://www.ncloud.com/)
- Containerization - [**Docker**](https://www.docker.com), [**Kubernetes**](https://kubernetes.io/)
- Data Collection - [**Fluentd**](https://www.fluentd.org/)
- Data Processing - [**Spark**](https://spark.apache.org/)
- Orchestration - [**Airflow**](https://airflow.apache.org)
- Data Lake - [**Naver Cloud Object Storage**](https://www.ncloud.com/product/storage/objectStorage)
- Data Serving - [**Druid**](https://druid.apache.org/)
- Data Visualization - [**SuperSet**](https://superset.apache.org/)
- Language - [**Python**](https://www.python.org)

### Final Results

Still working on it...

---

## Blog

1. Project Description [Link](https://velog.io/@jieun1128/%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8-%EA%B0%9C%EC%9A%94-%EA%B0%80%EC%83%81%EC%9D%98-%EC%9D%8C%EC%9B%90-%EC%82%AC%EC%9D%B4%ED%8A%B8-%EC%9D%B4%EB%B2%A4%ED%8A%B8-%EB%A1%9C%EA%B7%B8-%EB%B6%84%EC%84%9D-%EC%8B%9C%EC%8A%A4%ED%85%9C-%EA%B5%AC%ED%98%84)
2. Data Extraction [Link](https://velog.io/@jieun1128/Data-Extract-2)
3. Data Transformation [Link](https://velog.io/@jieun1128/data-transformation)
4. Data Load [Link](https://velog.io/@jieun1128/Data-Load)

---

### etc

- This project was inspired by [Streamify](https://github.com/ankurchavda/streamify)
- Event server can be found in [Eventsim](https://github.com/Interana/eventsim)
