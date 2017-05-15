# 3esynaptiq-python

Scripts to get PV datasets from 3E Synaptiq web services and store them in an InfluxDB (https://www.influxdata.com/) DB.

**Usage (device level see 3E documentation):**
<pre>
python3 getter_device_data.py -c conf/example_device_inverter.json
</pre>

**Usage (plant level):**
<pre>
python3 getter_plant_data.py -c conf/example_plant.json
</pre>

**Requirements:**
<pre>
python>=3.5.2
jsonschema>=2.6.0
influxdb>=4.0.0
pytz>=2016.10
</pre>

