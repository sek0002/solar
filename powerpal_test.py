import asyncio
import sys
from bleak import BleakClient, BleakError, BleakScanner


PAIRING_CODE_CHAR = "59DA0011-12F4-25A6-7D4F-55961DCE4205"
MEASUREMENT_CHAR = "59DA0001-12F4-25A6-7D4F-55961DCE4205"
READING_BATCH_SIZE_CHAR = "59DA0013-12F4-25A6-7D4F-55961DCE4205"
BATTERY_CHAR = "00002a19-0000-1000-8000-00805f9b34fb"

PULSES_PER_KWH = 1000.0
READ_EVERY_MINUTES = 1
PULSE_MULTIPLIER = (60.0 / READ_EVERY_MINUTES) / PULSES_PER_KWH


def convert_pairing_code(pairing_code):
    return int(pairing_code).to_bytes(4, byteorder="little")


async def resolve_powerpal_device(address):
    print("Scanning for BLE devices for up to 15 seconds...")
    devices = await BleakScanner.discover(timeout=15.0, return_adv=True)

    exact_match = None
    name_match = None
    for _, (device, _) in devices.items():
        device_name = device.name or ""
        if address and device.address.lower() == address.lower():
            exact_match = device
            break
        if "powerpal" in device_name.lower() and name_match is None:
            name_match = device

    if exact_match is not None:
        print(f"Matched requested address via scan: name={exact_match.name!r} address={exact_match.address!r}")
        return exact_match

    if name_match is not None:
        print(f"Matched Powerpal by name: name={name_match.name!r} address={name_match.address!r}")
        return name_match

    raise BleakError("Could not find a Powerpal device during scan.")


def decode_measurement_notification(data):
    if len(data) < 6:
        return f"measurement: too short ({len(data)} bytes)"

    unix_time = int.from_bytes(data[0:4], byteorder="little", signed=False)
    total_pulses = int.from_bytes(data[4:6], byteorder="little", signed=False)
    kw = total_pulses * PULSE_MULTIPLIER
    raw_bytes = " ".join(str(byte) for byte in data)
    return (
        f"Notify callback for characteristic {MEASUREMENT_CHAR} of data length {len(data)}\n"
        f"data: {raw_bytes}\n"
        f"Time: {unix_time}, Pulses: {total_pulses}, Power: {kw} kW"
    )


async def main(address, pairing_code):
    while address is None and len(sys.argv) < 2:
        address = input("Your Powerpal MAC address (or press enter to scan by name): ").strip() or None
        if address is not None and ((address.count(":") != 5) or (len(address) != 17)):
            address = None
            print("Incorrect MAC address formatting, should look like -> 12:34:56:78:9A:BC")

    while pairing_code is None:
        pairing_code = int(input("Your Powerpal pairing code: "))
        if not (0 <= pairing_code <= 999999):
            pairing_code = None
            print("Pairing Code should be 6 digits...")

    input(
        "Please confirm that you are NOT connected to the Powerpal via Bluetooth using any devices, "
        "and that bluetooth is enabled on your computer, and hit enter to continue..."
    )

    resolved_device = await resolve_powerpal_device(address)

    async with BleakClient(resolved_device) as client:
        print(f"Connected: {client.is_connected}")

        try:
            paired = await client.pair()
            print(f"Paired?: {paired}")
        except Exception as exc:
            print(f"Pairing step skipped or not supported on this platform: {exc}")

        print(f"Authenticating with pairing_code: {pairing_code}, converted: {convert_pairing_code(pairing_code)}")
        await client.write_gatt_char(PAIRING_CODE_CHAR, convert_pairing_code(pairing_code), response=False)
        print("Auth Success\n")

        await asyncio.sleep(2.0)

        try:
            await client.write_gatt_char(
                READING_BATCH_SIZE_CHAR,
                int(READ_EVERY_MINUTES).to_bytes(4, byteorder="little"),
                response=False,
            )
            print(f"Configured reading batch size to {READ_EVERY_MINUTES} minute(s).")
        except Exception as exc:
            print(f"Unable to configure reading batch size: {exc}")

        try:
            battery_value = await client.read_gatt_char(BATTERY_CHAR)
            if battery_value:
                print(f"Battery: {int(battery_value[0])}%")
        except Exception as exc:
            print(f"Unable to read battery level: {exc}")

        def measurement_handler(_, data):
            print(decode_measurement_notification(bytearray(data)))

        print("\nSubscribing to live measurement notifications. Press Ctrl+C to stop.\n")
        await client.start_notify(MEASUREMENT_CHAR, measurement_handler)
        try:
            while True:
                await asyncio.sleep(1.0)
        finally:
            await client.stop_notify(MEASUREMENT_CHAR)


if __name__ == "__main__":
    asyncio.run(main((sys.argv[1] if len(sys.argv) >= 2 else None), (sys.argv[2] if len(sys.argv) == 3 else None)))
