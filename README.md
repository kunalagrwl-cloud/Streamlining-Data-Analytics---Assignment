Tool: Grafana (connected to MySQL)

Description: A three-tier Smart City monitoring suite that tracks real-time environmental telemetry, weather dynamics, and operational alerts across five zones in Delhi.

Charts & Metrics:

1.Geomap — Spatial distribution of city sensor hotspots.

2.Time series (Dual Y-Axis) — Thermal & Moisture Dynamics (Temperature C and Humidity %).

3.Bar gauges (Retro LCD) — Atmospheric Motion (Wind Speed, Peak Gusts, Rainfall) and PM 2.5 exposure by zone.

4.Circular Gauges — Prevailing Wind Heading & Bearing.

5.Donut Chart — Air Quality Severity Distribution.

6.Bar charts — Traffic Flow Volume and Alert Trends by zone.

7.Data Table — Real-time sensor status event log and operational alerts.

Consumer / Data Flow Description

Ingestion: Real-time smart city telemetry (weather and air quality metrics) is streamed into an Apache Kafka topic.

Consumer Logic: A Python-based consumer (consumer.py) continuously listens to the Kafka topic. Upon receiving a message, it deserializes the incoming JSON payloads, extracts the relevant metrics (e.g., temperature, wind speed, PM 2.5), and prepares them for relational storage.

Storage (MySQL): The consumer acts as a bridge, executing SQL INSERT statements to write the structured data into specific MySQL database tables (such as weather and air_quality).

How the Dashboard Reads It: Grafana is configured with MySQL as its primary data source. Rather than reading from Kafka directly, Grafana runs optimized SQL SELECT queries against the MySQL tables. These queries utilize Grafana macros (like $__timeFilter) and dynamic template variables (like$location) to fetch the most recent data. The dashboard automatically refreshes at a set interval (e.g., every 30 seconds) to update the visualizations in real time.
