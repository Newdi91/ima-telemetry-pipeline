"""Simulates a distributed multi-machine IoT streaming data source, generating

progressively increasing, unique timestamps per device and writing raw JSON
payloads to the landing zone storage.
"""


import json
import time
from datetime import datetime, timedelta, timezone
import random

# Landing zone path for JSON files 
landing_zone_path = "/Volumes/ima/telemetry/landing/"

# Reference master data dictionaries
machine_models = ["IMA_C24", "IMA_SW100", "IMA_BVC", "IMA_TRITON"]
plants = ["Bologna_Plant", "Ozzano_Plant", "Bentivoglio_Plant", "Zola_Predosa_Plant"]

def generate_iot_data(num_machines=10, max_iterations=50):
    """
    Generates simulated IoT telemetry data for a configurable fleet of machines.
    
    Parameters:
    - num_machines (int): Total number of unique devices to simulate (default: 10)
    - max_iterations (int): Number of complete cycles the fleet should run (default: 50)
    """
    
    # Dynamically build the fleet based on the passed parameter 'num_machines'
    fleet_devices = []
    for i in range(1, num_machines + 1):
        # Deterministically assign a model and plant based on index
        model_idx = (i - 1) % len(machine_models)
        plant_idx = (i - 1) % len(plants)
        
        fleet_devices.append({
            "device_id": f"SN-{i:03d}",  # Generates IDs like SN-001, SN-002, etc.
            "machine_model": machine_models[model_idx],
            "plant_location": plants[plant_idx]
        })

    print(f"Fleet successfully initialized: {len(fleet_devices)} active machines.")
    print(f"Starting progressive simulation for {max_iterations} iterations...")

    # Start from a fixed baseline timestamp (e.g., beginning of today UTC)
    current_simulation_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    event_count = 0
    try:
        for iteration in range(max_iterations):
            # Cycle through all devices in the fleet to ensure progressive data flow
            for device in fleet_devices:
                serial_number = device["device_id"]
                machine_id = device["machine_model"]
                plant_location = device["plant_location"]
                
                # Simulate targeted random anomalies (~5% probability)
                is_anomaly = random.random() < 0.05
                
                temperature = round(random.uniform(70.0, 95.0) if not is_anomaly else random.uniform(115.0, 140.0), 2)
                vibration = round(random.uniform(0.5, 3.2) if not is_anomaly else random.uniform(6.5, 12.0), 2)
                rpm = random.randint(1200, 2500) if not is_anomaly else random.randint(3500, 4500)
                pressure = round(random.uniform(4.5, 6.0), 2)
                
                # Format progressive timestamp into ISO 8601 string
                timestamp_str = current_simulation_time.isoformat()
                
                # Construct IoT payload
                telemetry_event = {
                    "device_id": serial_number,
                    "machine_model": machine_id,
                    "plant_location": plant_location,
                    "temperature_c": temperature,
                    "vibration_mm_s": vibration,
                    "speed_rpm": rpm,
                    "pressure_bar": pressure,
                    "status_code": "ERROR_OVERHEAT" if is_anomaly else "OK",
                    "timestamp": timestamp_str
                }
                
                # Write the JSON file to the Unity Catalog volume landing zone
                file_name = f"iot_telemetry_{serial_number}_{int(current_simulation_time.timestamp())}.json"
                full_path = f"{landing_zone_path}{file_name}"
                
                dbutils.fs.put(full_path, json.dumps(telemetry_event), overwrite=True)
                event_count += 1
                
                # Advance simulation time by 1 second for the next event
                current_simulation_time += timedelta(seconds=1)
                
        print(f"Simulation completed successfully. Total events generated: {event_count}")

    except Exception as e:
        print(f"Error during data generation: {str(e)}")
        raise e


if __name__ == "__main__":
    generate_iot_data()