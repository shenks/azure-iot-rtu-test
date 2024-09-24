from azure.iot.device.aio import IoTHubDeviceClient
import os
from dotenv import load_dotenv

import random
import datetime
import time
import json
import asyncio

# Load environment variables from .env file
load_dotenv()

# Get the Azure IoT Hub connection string from the environment variable
connectionString = os.getenv("IOT_CONNECTION_STRING")

async def sendToIotHub(data):
    try:
        # Create an instance of the IoT Hub Client class
        device_client = IoTHubDeviceClient.create_from_connection_string(connectionString)

        # Connect the device client
        await device_client.connect()

        #Send the message
        device_client.send_message(data)
        print("Message sent to IoT Hub:", data)

        # Shutdown the client
        await device_client.shutdown()
        
    
    except Exception as e:
        print("Error:", str(e))

def main():
    # Run an infinite while loop to send data every 5 seconds
    while True:
        # Generate random value
        water_level = random.randint(0, 10)
        # Generate data packet
        data={
            "device_id":"test-device-1",
            "water_level":water_level,
            "edge_time_stamp":str(datetime.datetime.now())
        }
        asyncio.run(sendToIotHub(data=json.dumps(data)))
        time.sleep(10)

if __name__ == '__main__':
    main()