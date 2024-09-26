import os
import random
import datetime
import json
import asyncio

from dotenv import load_dotenv
from azure.iot.device.aio import IoTHubDeviceClient

# load environment variables from .env file
load_dotenv()

# get the Azure IoT Hub device connection strings from the environment variables
connectionString1 = os.getenv("IOT_CONNECTION_STRING_1")
connectionString2 = os.getenv("IOT_CONNECTION_STRING_2")

async def sendToIotHub(device_client, station_id, water_level):
    try:
        # Generate JSON data packet
        data = {
            "stationId": station_id,
            "waterLevel": water_level,
            "timestamp": str(datetime.datetime.now())
            # str() makes it a string for ease of data manipulation
            # first datetime is the python module that provides classes
            # second datetime is the class within the datetime module to create date and time objects
            # .now: returns current local date and time as an object
        }
        
        # Send the message
        await device_client.send_message(json.dumps(data))
        print(f"Message sent to IoT Hub from {station_id}: {data}")

    except Exception as e:
        print(f"Error from {station_id}:", str(e))

async def simulate_device(device_client, station_id):
    # Connect the client once and keep it open
    await device_client.connect()

    try:
        while True:
            # Generate random water level with 3 decimals
            water_level = round(random.uniform(0, 10), 3)
            await sendToIotHub(device_client, station_id, water_level)
            await asyncio.sleep(10)  # asyncio.sleep for time interval 
    finally:
        # Ensure the client shuts down after the simulation
        await device_client.shutdown()

async def main():
    # Create IoT Hub clients for both devices
    device_client1 = IoTHubDeviceClient.create_from_connection_string(connectionString1)
    device_client2 = IoTHubDeviceClient.create_from_connection_string(connectionString2)

    # Simulate both devices concurrently
    await asyncio.gather(
        simulate_device(device_client1, "test-device-1"),
        simulate_device(device_client2, "test-device-2")
    )

if __name__ == '__main__':
    asyncio.run(main())  # Run the main routine