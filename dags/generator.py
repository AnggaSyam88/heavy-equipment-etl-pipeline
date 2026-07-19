import os
import json
import csv
import random
from datetime import datetime, timedelta

# 1. Konfigurasi Sistem
BASE_DIR = "data_lake/raw/sensor_data"
EQUIPMENT = ["EXC-001", "DRL-002", "CRSH-003"] # Ekskavator, Bor, Mesin Giling
START_DATE = datetime(2026, 6, 19)
DAYS = 3

# 2. Fungsi Simulasi "Kekacauan" (Chaos)
def get_sensor_value(base_val, variance, anomaly_chance=0.02, null_chance=0.01):
    """Menghasilkan nilai sensor dengan potensi anomali atau koneksi terputus."""
    if random.random() < null_chance:
        return None  # Simulasi paket data hilang (Null/NaN)
    
    if random.random() < anomaly_chance:
        # Simulasi lonjakan ekstrem (2 hingga 4 kali lipat dari batas normal)
        return round(base_val * random.uniform(2.0, 4.0), 2) 
        
    # Kondisi operasi normal
    return round(base_val + random.uniform(-variance, variance), 2)

# 3. Mesin Utama Generator
def generate_mock_data():
    print("Memulai pabrikasi data SCADA...")
    
    for day_offset in range(DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        date_str = current_date.strftime("%Y-%m-%d")
        
        # Membuat folder partisi ala S3 / Data Lake
        partition_path = os.path.join(BASE_DIR, f"date={date_str}")
        os.makedirs(partition_path, exist_ok=True)
        
        # Simulasi pengiriman log per jam (Batch)
        for hour in range(24):
            for eq in EQUIPMENT:
                json_filename = os.path.join(partition_path, f"{eq}_log_{hour:02d}00.json")
                csv_filename = os.path.join(partition_path, f"{eq}_vibration_{hour:02d}00.csv")
                
                json_records = []
                csv_records = []
                
                # Looping menit (untuk JSON)
                for minute in range(60):
                    timestamp = current_date.replace(hour=hour, minute=minute)
                    
                    # Data JSON (Resolusi 1 Menit)
                    json_records.append({
                        "timestamp": timestamp.isoformat() + "Z",
                        "equipment_id": eq,
                        "metadata": {"firmware": "v2.1.4", "protocol": "OPC-UA"},
                        "metrics": {
                            "engine_temp_c": get_sensor_value(85, 5),     # Suhu ideal 85C
                            "hydraulic_psi": get_sensor_value(3000, 200), # Tekanan 3000 PSI
                            "rpm": get_sensor_value(1800, 50)             # Putaran 1800 RPM
                        }
                    })
                    
                    # Data CSV Getaran (Resolusi Tinggi: 10 Detik = 6 data per menit)
                    for sec in range(0, 60, 10):
                        csv_ts = timestamp.replace(second=sec)
                        csv_records.append([
                            csv_ts.isoformat() + "Z", 
                            eq, 
                            get_sensor_value(50, 10) # Getaran 50 Hz
                        ])
                        
                # Simpan sebagai JSON Lines (satu record per baris)
                with open(json_filename, 'w') as jf:
                    for record in json_records:
                        jf.write(json.dumps(record) + '\n')
                        
                # Simpan sebagai CSV
                with open(csv_filename, 'w', newline='') as cf:
                    writer = csv.writer(cf)
                    writer.writerow(["timestamp", "equipment_id", "vibration_hz"])
                    writer.writerows(csv_records)
                    
        print(f"Data partisi tanggal {date_str} berhasil dibuat.")

if __name__ == "__main__":
    generate_mock_data()
    print(f"Simulasi selesai! Silakan periksa folder: {BASE_DIR}")