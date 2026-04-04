import asyncio
from bleak import BleakClient
from bleak.exc import BleakError
from datetime import datetime
import pytz

OUTPUT_FILE = "ble_notifications.txt"
mac = "C9:91:09:7A:2C:B9"
pairingCodeChar = '59da0011-12f4-25a6-7d4f-55961dce4205'
powerpalfreq = '59da0013-12f4-25a6-7d4f-55961dce4205'
powerpalSerialChar = '59DA0010-12F4-25A6-7D4F-55961DCE4205'
notify= "59da0001-12f4-25a6-7d4f-55961dce4205"
my_pairing_code= "774034"
MAX_RETRIES = 20

import struct
from datetime import datetime

def convert_pairing_code(original_pairing_code):
    return int(original_pairing_code).to_bytes(4, byteorder='little')

async def on_notification(sender: int, data: bytearray):
    #print(f"Received notification: {data}")
    """Handle Callback from a Bluetooth (GATT) request."""

    # Extract the first 4 bytes
    first_4_bytes = data[:4]
    pulse = data[5:8]

    int_array = list(data)
    pulse1 = int_array[4]+int_array[5]
    pulse2 = pulse1/0.8
    # Convert the first 4 bytes to an integer (Unix timestamp)
    #timestamp = int.from_bytes(first_4_bytes, byteorder='little')
    timestamp = struct.unpack_from('<I', data, 0)[0]
    #pulse1= struct.unpack_from('<H', data, 5)[0]
    #pulse1= int.from_bytes(pulse, byteorder='little')/1000
# Convert the timestamp to a human-readable date
    #human_readable_date = datetime.utcfromtimestamp(timestamp)
    utc_time = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc)

# Convert to Melbourne time (AEST/AEDT)
    melbourne_tz = pytz.timezone("Australia/Melbourne")
    melbourne_time = utc_time.astimezone(melbourne_tz)


    #print("Unix timestamp:", timestamp)
    print( melbourne_time, pulse1)
    with open(OUTPUT_FILE, "a") as file:
        file.write(f"{timestamp}: {pulse2}\n")
    # The notification handles are off-by-one compared to gattlib and bluepy

def callback(sender: notify, data: bytearray):
        print(f"{data}")

async def main(address):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            client = BleakClient(address)
#    def callback(sender: notify, data: bytearray):
#        print(f"{data}")
            await client.connect(timeout=30.0)
        # 0x421 - 1, copied from eq3bt
            out = client.services
        #print(out)
            await client.write_gatt_char(pairingCodeChar, convert_pairing_code(my_pairing_code), response=True)
            await client.write_gatt_char(powerpalfreq, b'\x01\x00\x00\x00', response=True)
            data = await client.read_gatt_char(notify)
        #print(convert_pairing_code(my_pairing_code))
       # await client.start_notify(notify, on_notification)
        #print(data)
            await client.start_notify(notify, on_notification)
 
            print(data)
            await asyncio.Event().wait()
       # client.start_notify(notify, on_notification)
       # await client.disconnect()
#        print(data)
#        await client.start_notify(notify, callback)
#        await asyncio.sleep(5.0)
#        await client.stop_notify(notify)
            break
        except BleakError as e:
            print(f"Error occurred: {e}")
            retries += 1
            if retries < MAX_RETRIES:
                print(f"Retrying connection ({retries}/{MAX_RETRIES})...")
                await asyncio.sleep(2)  # Wait before retrying
            else:
                print(f"Maximum retries reached. Unable to connect.")
                break
async def main1():
    """Run the script."""
    await main(mac)


if __name__ == "__main__":
     loop = asyncio.get_event_loop()
#   7 loop.set_debug(True)
#   5 loop.asyncio.run(main(mac))
    # loop.run_until_complete(main(mac))
     asyncio.run(main1())
