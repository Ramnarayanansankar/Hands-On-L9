# Ride Sharing Analytics Using Spark Streaming and Spark SQL.

The objective of this project is to build a real-time data analytics pipeline for a ride-sharing service using Apache Spark's Structured Streaming. The pipeline ingests, processes, and analyzes streaming data to derive key business insights in real time.

## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

4. **Java 8+**:
     ```bash
     java -version
     ```

## Repository Structure
The project repository should have the following structure:
```
ride-sharing-analytics/
├── outputs/
│   ├── task1_outputs
│   |    ├── part-00000-....csv.crc
│   |    ├── part-00000-....csv.crc
│   |    ├── part-00000-....csv.crc
│   |    ├── part-00000-....csv
│   |    ├── part-00000-....csv
│   |    └── part-00000-....csv
|   ├── task2_outputs
│   |   ├── batch_0
|   |   |    └── part-00000-....csv
│   │   ├── batch_1
|   |   |    └── part-00000-....csv
│   │   ├── batch_2
|   |   |    └── part-00000-....csv
│   │   ├── batch_3
|   |   |    └── part-00000-....csv
|   └── task3_outputs
│   |   ├── batch_12
|   |   |    └── part-00000-....csv
│   │   ├── batch_34
|   |   |    └── part-00000-....csv
│   │   ├── batch_41
|   |   |    └── part-00000-....csv
│   │   ├── batch_49
|   |   |    └── part-00000-....csv
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

## **Running the Analysis Tasks**

Open two terminals. Generator should be running in one terminal while the tasks are running in the other terminal.

 ### 1) Start the data generator in Terminal 1

```bash
python data_generator.py
# Streams one JSON event per line to localhost:9999
```

### 2) Run the tasks in Terminal 2

Run **one task at a time**:

```bash
# Task 1: Ingestion & parsing → CSV (one file per row)
python task1.py

# Task 2: Aggregations by driver → CSV (one folder per micro-batch)
python task2.py

# Task 3: Windowed sum(fare_amount) over 5-min window (slide window is 1 min, watermark is 1 min) → CSV
python task3.py
```

> **Note:** Task 3 needs a few minutes of running time for the windows to have data.

## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

## **Objectives**

  * **Task 1:** Ingest and parse real-time ride data.
  * **Task 2:** Performing real-time aggregations on driver earnings and trip distances.
  * **Task 3:** Analyze trends over time using a sliding time window.

## **Task 1: Basic Streaming Ingestion and Parsing**

1. Ingest streaming data from the provided socket (e.g., localhost:9999) using Spark Structured Streaming.
2. Parse the incoming JSON messages into a Spark DataFrame with proper columns (trip_id, driver_id, distance_km, fare_amount, timestamp).

### **Implementation Notes:**
1. Create a Spark session.
2. Use spark.readStream.format("socket") to read from localhost:9999.
3. Parse the JSON payload into columns.
4. Writing each row into its own CSV file.
---

## **Task 2: Real-Time Aggregations (Driver-Level)**

Aggregate the data in real time:
  * Grouping by driver_id 
  * Total fare amount using SUM(fare_amount) as total_fare.
  • Average distance using AVG(distance_km) as avg_distance.

### **Implementation Notes:**
1. Convert timestamp → TimestampType (used for consistency).
2. Use outputMode("complete") for grouped aggregations.

## **Task 3: Windowed Time-Based Analytics**

1. Convert the timestamp column to a proper TimestampType.
2. Performing a 5-minute windowed aggregation on fare_amount (sliding by 1 minute and watermarking by 1 minute).

### **Implementation Notes:**

1. Using groupBy(window(event_time, "5 minutes", "1 minute")).
2. Select window.start and window.end into window_start and window_end.

---

## **Sample Output**

### Task 1 — Parsed Events (example row)

Task1 outputs are present in the folder [Task1_Output](./outputs/task1_outputs/)

**Sample Task1 Output:**
```
trip_id,driver_id,distance_km,fare_amount,timestamp
ac6a3544-be6b-4eeb-b06f-b8c79a9e3460,97,29.37,104.72,2025-10-14 21:29:51

```

## Task 2 — Driver Aggregations

Task2 outputs are present in the folder [Task2_Output](./outputs/task2_outputs/)

**Sample Task2 Output:**
```
driver_id,total_fare,avg_distance
51,14.23,33.17
42,129.56,5.91
73,103.67,38.9
98,51.53,47.05
17,91.9,39.75
55,47.98,10.5
33,72.28,26.45
67,77.46,33.95
79,35.4,4.03
32,146.09,16.55
10,33.29,12.56
63,75.05,34.06
65,145.2,38.15
12,122.41,14.17
14,11.05,29.93

```

## Task 3 — Windowed Aggregation (sample from batch_85)

Task3 outputs are present in the folder [Task3_Output](./outputs/task3_outputs/)

**Sample Task3 Output:**
```
window_start,window_end,total_fare
2025-10-14T22:24:00.000Z,2025-10-14T22:29:00.000Z,2787.4

```

## 📬 Submission Checklist

- [&#x2713;] Python scripts 
- [&#x2713;] Output files in the `outputs/` directory  
- [&#x2713;] Completed `README.md`  
- [&#x2713;] Commit everything to GitHub Classroom  
- [&#x2713;] Submit your GitHub repo link on canvas

---