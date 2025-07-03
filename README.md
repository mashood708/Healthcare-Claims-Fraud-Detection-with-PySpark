# Healthcare-Claims-Fraud-Detection-with-PySpark
“Scalable fraud detection pipeline on simulated healthcare claims data using PySpark optimization and MLlib clustering.”

# 🚀 Healthcare Claims Fraud Detection with PySpark

This project demonstrates how to detect potential fraud in healthcare claims data at scale using PySpark. We simulate a large transactional dataset, optimize processing with PySpark techniques, and apply clustering for anomaly detection.

## 📁 Project Features

- Simulated 1 million claims transactions (scalable to 10 million+)
- Small fraud codes lookup broadcast join
- Repartitioning by claim type to optimize data layout
- Caching for repeated queries on high-risk claims
- Applied KMeans clustering using Spark MLlib to detect potential anomalies

## 🛠 Technologies Used

- Apache Spark (PySpark)
- Spark MLlib
- Python
- Google Colab or Databricks

## 🚦 Optimization Techniques Demonstrated

- **Broadcast Join** for dimension table lookups
- **Repartition** to control parallelism and optimize file writes
- **Cache** to store frequently queried high-risk data
- **Catalyst Optimizer** (behind the scenes with Spark explain plans)
- **MLlib** KMeans for scalable unsupervised fraud detection

## 📊 Results

- Labeled suspicious claims by procedure code
- Grouped high-amount suspicious claims for deeper analysis
- Identified anomaly clusters using KMeans for further investigation

## 📌 How to Run

👉 **Google Colab**  
1. Install PySpark  
   ```python
   !pip install pyspark

   👉 Databricks

Paste the code into a Databricks notebook

Attach to a cluster and run




   


