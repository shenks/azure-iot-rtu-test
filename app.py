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
        # Generate random value
        water_level = random.randint(0, 10)
        # Generate JSON data packet
        data = {
            "device_id": "test-device-1",
            "water_level": water_level,
            "edge_time_stamp": str(datetime.datetime.now())
        }
        await sendToIotHub(data=json.dumps(data))  # Await the sendToIotHub call
        await asyncio.sleep(10)  # asyncio.sleep for time interval 

if __name__ == '__main__':
    asyncio.run(main())  # Run the main routine