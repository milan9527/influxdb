import os
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB connection details
INFLUXDB_URL = os.environ.get('INFLUXDB_URL')
INFLUXDB_TOKEN = os.environ.get('INFLUXDB_TOKEN')
INFLUXDB_ORG = os.environ.get('INFLUXDB_ORG')
INFLUXDB_BUCKET = os.environ.get('INFLUXDB_BUCKET')

# Create an InfluxDB client instance
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

# Get the write API
write_api = client.write_api(write_options=SYNCHRONOUS)

def lambda_handler(event, context):
    pressure_value = event.get('pressure')
    viscosity_value = event.get('viscosity')
    device_id = event.get('deviceid')
    client_id = event.get('clientid')
    sensor_id = f"{device_id}{client_id}"

    # Write data to InfluxDB
    write_to_influxdb(pressure_value, viscosity_value, sensor_id)

    return {
        'statusCode': 200,
        'body': json.dumps('Data written to InfluxDB successfully')
    }

def write_to_influxdb(pressure_var, viscosity_var, sensor_var):
    print("Executing InfluxDB insert")

    data = [
        Point("pressure")
        .field("pressureValue", float(pressure_var) if pressure_var is not None else 0.0)
        .field("viscosity", float(viscosity_var) if viscosity_var is not None else 0.0)
        .tag("sensorID", sensor_var)
    ]

    write_api.write(bucket=INFLUXDB_BUCKET, record=data)
    print("Finished executing")
