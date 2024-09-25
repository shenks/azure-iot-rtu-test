import os
import random
import datetime
import json
import asyncio

from dotenv import load_dotenv
from azure.iot.device.aio import IoTHubDeviceClient

# Load environment variables from .env file
load_dotenv()

# Get the Azure IoT Hub device primary connection string from the environment variable
connectionString = os.getenv("IOT_CONNECTION_STRING")

async def sendToIotHub(data):
    try:
        # Create an instance of the IoT Hub Client class
        device_client = IoTHubDeviceClient.create_from_connection_string(connectionString)

        # Connect the device client
        await device_client.connect()

        #Send the message
        await device_client.send_message(data)
        print("Message sent to IoT Hub:", data)

        # Shutdown the client
        await device_client.shutdown()
        
    except Exception as e:
        print("Error:", str(e))

async def main():
    while True:
        # generate float with 3 decimals
        water_level = round(random.uniform(0, 10), 3)
        # generate JSON data packet
        data = {
            "stationId": "test-device-1",
            "waterLevel": water_level,
            "timestamp": str(datetime.datetime.now())
            # str() makes it a string for ease of data manipulation
            # first datetime is the python module that provides classes
            # second datetime is the class within the datetime module to create date and time objects
            # .now: returns current local date and time as an object
        }
        await sendToIotHub(data=json.dumps(data))  # Await the sendToIotHub call
        await asyncio.sleep(10)  # asyncio.sleep for time interval 

if __name__ == '__main__':
    asyncio.run(main())  # Run the main routine