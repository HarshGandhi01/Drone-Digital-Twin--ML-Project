import os
import glob
import pandas as pd
from pymavlink import mavutil

def process_single_tlog(filename):
    """
    Reads a single tlog and returns a cleaned DataFrame.
    """
    print(f"  -> Parsing {filename}...")
    
    try:
        mlog = mavutil.mavlink_connection(filename)
    except Exception as e:
        print(f"  [!] Error opening {filename}: {e}")
        return None

    data_rows = []
    
    while True:
        # Fetch specific messages we need for the ML Project
        msg = mlog.recv_match(type=['SYS_STATUS', 'VFR_HUD', 'ATTITUDE'], blocking=False)
        if not msg:
            break
        
        # Base row with timestamp
        row = {'timestamp': msg._timestamp, 'type': msg.get_type()}
        
        # Extract features based on message type
        if msg.get_type() == 'SYS_STATUS':
            row['voltage'] = msg.voltage_battery / 1000.0  # Convert mV to Volts
            row['current'] = msg.current_battery / 100.0   # Convert cA to Amps
            row['battery_remaining'] = msg.battery_remaining # %
            
        elif msg.get_type() == 'VFR_HUD':
            row['throttle'] = msg.throttle
            row['altitude'] = msg.alt
            row['groundspeed'] = msg.groundspeed
            
        elif msg.get_type() == 'ATTITUDE':
            row['roll'] = msg.roll
            row['pitch'] = msg.pitch

        data_rows.append(row)
        
    if not data_rows:
        print(f"  [!] No usable data found in {filename}")
        return None

    # Convert to DataFrame
    df = pd.DataFrame(data_rows)
    
    # --- DATA ALIGNMENT (The "Magic" Step) ---
    # Tlogs store data in separate rows. We must forward-fill (ffill) 
    # so that a 'throttle' row also knows the most recent 'voltage'.
    df = df.fillna(method='ffill').dropna()
    
    # Filter to keep only VFR_HUD rows (usually ~4Hz) to establish a consistent time step
    df = df[df['type'] == 'VFR_HUD'].drop('type', axis=1)
    
    # Add a column to track which flight this came from (useful for debugging)
    df['source_file'] = os.path.basename(filename)
    
    return df

def main():
    # Find all .tlog files in the current directory
    tlog_files = glob.glob("*.tlog")
    
    if not tlog_files:
        print("No .tlog files found! Make sure they are in this folder.")
        return

    print(f"Found {len(tlog_files)} log files. Starting batch processing...")
    
    all_flights = []
    
    for log_file in tlog_files:
        flight_df = process_single_tlog(log_file)
        if flight_df is not None and not flight_df.empty:
            all_flights.append(flight_df)
            
    if all_flights:
        # Combine all flights into one big dataset
        master_df = pd.concat(all_flights, ignore_index=True)
        
        output_file = "master_drone_dataset.csv"
        master_df.to_csv(output_file, index=False)
        
        print("\n" + "="*40)
        print(f"DONE! Processed {len(all_flights)} flights.")
        print(f"Total Data Points: {len(master_df)}")
        print(f"Saved to: {output_file}")
        print("="*40)
        print(master_df.head())
    else:
        print("Could not process any files.")

if __name__ == "__main__":
    main()