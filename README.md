# Fintech Real-Time Fraud Detection Pipeline

To simulate enterpise-grade data engineering, I wanted to build a highly scalable real-time pipeline capable of handling thousands of financial transactions per second. The core challenge invovled working with high-velocity data feeds where network-induced transaction retries caused duplicate submissions, and poor cellular connectivity introduced significant late-arriving event data. 

## Objective -

1. Ingest thousands of real-time financial transaction records per second from a live public exchange feed (coinbase.com)

2. Enforce exactly-once processing concepts by identifying and dropping network duplicate records.

3. Accounting for network lag by cleanly integrating late-arriving data without causing unbounded 

4. Execute velocity flag high-risk transaction bursts (example - more than 3 distinct swipes per assest card within a 1-minute window)

5. Persist data with fault-tolerant checkpointing and visualize the alerts dynamically.

## Method of Approach - 

I orchestrated a decoupled architecture locally using Docker Compose to run single-node Apache Kafka and Apache Spark 3.5.1

1. Ingestion Implementation - Wrote an asynchronous Python Script (producer.py) ultizing WebSockets to open a persistent live data connection directly to the Coinbase Public Exchange API (https://docs.cdp.coinbase.com/exchange/introduction/rest-quickstart), mapping raw global assest matches into standard transaction schema payloads.

2. Stream And State Development - Developed a PySpark Structured Streaming application (consumer_fraud_detection.py) that applies a strict 10-minute Event-Time Waterwark to automatically purge data arriving too late.

3. Data Quality And Deduplication - Paired the watermarl bounded *dropDuplicates(["transaction_id", "timestamp"])* calls to eliminate network retry duplicates within the state memory store window.

4. Window Aggregations - Grouped transaction events by assest type (*card_number*) over a 1-minute sliding window with a 30-second slide, filtering out and capturing velocity spikes where transaction frequencies crossed the safe thresholds. 

5. Storage And Optimization - Configured the processing output to sink into localized, compressed Parquet format files(https://www.databricks.com/blog/what-is-parquet) combined with strict metadata checkpoint directories for state recovery. 

(There were some internal JVM environment mismatches therefore i explicitly aligned the connector jar coordinates with the environment's native Scala 2.12 core runtime)

6. Observability And Analytics - Built an automated polling dashboard using Streamlit and Plotly that combines the parquet output in real-time, executing basic data validation steps while plotting live threat visual metrics.

## Final Result -

The pipeline successfully absorbed and processed rapid micro-batches under high-volume load conditions, capturing extreme transaction events (example - over 400 trade velocity instances per minute for high-activity assets like BTC-USD).

Also with a state-store memory bounds and fault-tolerance through watermark data purges and file checkpointing. (without compromising on transactional tracking logs).

![Dashboard Screenshot1](/home/sumanth/Pictures/Screenshots/Screenshot from 2026-07-14 13-12-44.png)
![Dashboard Screenshot2](/home/sumanth/Pictures/Screenshots/Screenshot from 2026-07-14 13-12-59.png)