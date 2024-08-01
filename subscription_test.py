#!/usr/bin/env python
# coding: utf-8

import mysql.connector
import uuid
import time
import csv
import secrets
import json
from uuid import UUID
import numpy as np
from datetime import datetime
import psutil
import platform

# Database connection
connection = mysql.connector.connect(
    host="127.0.0.1",
    user="test_user",
    password="password",
    database="test_db")
cursor = connection.cursor()

# UUIDv7 generation function
_last_v7_timestamp = None


def uuid7(simulated_time_ns) -> UUID:
    global _last_v7_timestamp
    timestamp_ms = simulated_time_ns // 10 ** 6
    if _last_v7_timestamp is not None and timestamp_ms <= _last_v7_timestamp:
        timestamp_ms = _last_v7_timestamp + 1
    _last_v7_timestamp = timestamp_ms
    uuid_int = (timestamp_ms & 0xFFFFFFFFFFFF) << 80
    uuid_int |= secrets.randbits(76)
    return UUID(int=uuid_int)


def generate_intervals(total, avg_interval):
    if avg_interval > 10 ** 8:
        return np.random.exponential(avg_interval, total)
    else:
        return np.random.poisson(avg_interval, total)


def generate_uuids(total, rpm):
    uuids = []
    avg_time_between_requests = (60 / rpm) * 10 ** 9
    intervals = generate_intervals(total, avg_time_between_requests)
    start_time = time.time_ns()
    current_time = start_time

    for i in range(total):
        current_time += int(intervals[i])
        uuid = uuid7(current_time)
        uuids.append(str(uuid))

        if (i + 1) % 100000 == 0:
            print(f"Generated {i + 1} UUIDs")

    return uuids


# Create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS subscription_user_v4(
    subscription_id VARCHAR(36),
    user_id VARCHAR(36),
    meta_site_id VARCHAR(36),
    billing_reference_id VARCHAR(36),
    cycle_id INT,
    product_id INT,
    status VARCHAR(20),
    subscription JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (subscription_id, user_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS subscription_user_mixed(
    subscription_id VARCHAR(36),
    user_id VARCHAR(36),
    meta_site_id VARCHAR(36),
    billing_reference_id VARCHAR(36),
    cycle_id INT,
    product_id INT,
    status VARCHAR(20),
    subscription JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (subscription_id, user_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_subscription_mixed(
    user_id VARCHAR(36),
    subscription_id VARCHAR(36),
    meta_site_id VARCHAR(36),
    billing_reference_id VARCHAR(36),
    cycle_id INT,
    product_id INT,
    status VARCHAR(20),
    subscription JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, subscription_id)
)
""")


# Helper functions
def clean_tables(cursor):
    tables = [
        'subscription_user_v4',
        'subscription_user_mixed',
        'user_subscription_mixed'
    ]
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table}")
    print("All tables cleaned.")


def get_table_size(cursor, table_name):
    # Get data and index size
    cursor.execute(f"""
    SELECT 
        table_name,
        round((data_length / 1024), 2) as 'Data Size (KB)',
        round((index_length / 1024), 2) as 'Index Size (KB)',
        round(((data_length + index_length) / 1024), 2) as 'Total Size (KB)'
    FROM information_schema.TABLES 
    WHERE table_schema = DATABASE() AND table_name = '{table_name}'
    """)
    result = cursor.fetchone()

    # Get number of rows
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    num_rows = cursor.fetchone()[0]

    return {
        'table_name': result[0],
        'data_size': result[1],
        'index_size': result[2],
        'total_size': result[3],
        'num_rows': num_rows
    }


def get_system_metrics():
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    return {
        'cpu_percent': cpu_percent,
        'memory_percent': memory.percent,
        'disk_percent': disk.percent,
        'system': platform.system(),
        'processor': platform.processor(),
        'python_version': platform.python_version(),
    }


def log_line_progression(i, total):
    if i != 0 and i % 100000 == 0:
        progress_percentage = (i / total) * 100
        print(f"{progress_percentage:.2f}%")


# Test parameters
RECORDS_NUM = 1000000

# Generate UUIDs
print("Generating UUIDs...")
uuidv4_list = [str(uuid.uuid4()) for _ in range(RECORDS_NUM * 4)]  # Generate 4 times as many UUIDs
uuidv7_list = generate_uuids(RECORDS_NUM, 10000)  # Assuming 10000 RPM for UUIDv7
print("UUID generation complete")

results = []

# Test data
dummy_json = json.dumps({"key": "value", "numbers": list(range(10))})

# Test subscription_user_v4
print("Inserting into subscription_user_v4...")
system_metrics = get_system_metrics()
start_time = time.time()
for i in range(RECORDS_NUM):
    log_line_progression(i, RECORDS_NUM)
    cursor.execute("""
    INSERT INTO subscription_user_v4 
    (subscription_id, user_id, meta_site_id, billing_reference_id, cycle_id, product_id, status, subscription) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (uuidv4_list[i], uuidv4_list[i], uuidv4_list[i], uuidv4_list[i], i % 100, i % 10, "active", dummy_json))
connection.commit()
end_time = time.time()
insertion_time = end_time - start_time
table_size = get_table_size(cursor, "subscription_user_v4")
results.append(('subscription_user_v4', insertion_time, table_size, system_metrics))

clean_tables(cursor)

# Test subscription_user_mixed
print("Inserting into subscription_user_mixed...")
system_metrics = get_system_metrics()
start_time = time.time()
for i in range(RECORDS_NUM):
    log_line_progression(i, RECORDS_NUM)
    cursor.execute("""
    INSERT INTO subscription_user_mixed 
    (subscription_id, user_id, meta_site_id, billing_reference_id, cycle_id, product_id, status, subscription) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (uuidv7_list[i], uuidv4_list[i], uuidv4_list[i], uuidv4_list[i], i % 100, i % 10, "active", dummy_json))
connection.commit()
end_time = time.time()
insertion_time = end_time - start_time
table_size = get_table_size(cursor, "subscription_user_mixed")
results.append(('subscription_user_mixed', insertion_time, table_size, system_metrics))

clean_tables(cursor)

# Test user_subscription_mixed
print("Inserting into user_subscription_mixed...")
system_metrics = get_system_metrics()
start_time = time.time()
for i in range(RECORDS_NUM):
    log_line_progression(i, RECORDS_NUM)
    cursor.execute("""
    INSERT INTO user_subscription_mixed 
    (user_id, subscription_id, meta_site_id, billing_reference_id, cycle_id, product_id, status, subscription) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (uuidv4_list[i], uuidv7_list[i], uuidv4_list[i], uuidv4_list[i], i % 100, i % 10, "active", dummy_json))

connection.commit()
end_time = time.time()
insertion_time = end_time - start_time
table_size = get_table_size(cursor, "user_subscription_mixed")
results.append(('user_subscription_mixed', insertion_time, table_size, system_metrics))

clean_tables(cursor)

# Print results
for result in results:
    print(f"{result[0]} insertion time: {result[1]:.2f} seconds")
    print(f"{result[0]} data size: {result[2]['data_size']:.2f} KB")
    print(f"{result[0]} index size: {result[2]['index_size']:.2f} KB")
    print(f"{result[0]} total size: {result[2]['total_size']:.2f} KB")
    print(f"{result[0]} number of rows: {result[2]['num_rows']}")

# Save results to CSV
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"mysql_insertion_subscription_test_results_{RECORDS_NUM}_records_{timestamp}.csv"

with open(csv_filename, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(
        ['Table Name', 'Insertion Time (s)', 'Data Size (KB)', 'Index Size (KB)', 'Total Size (KB)', 'Number of Rows',
         'CPU (%)', 'Memory (%)', 'Disk (%)', 'System', 'Processor', 'Python Version'])
    for result in results:
        table_name, insertion_time, size_info, metrics = result
        csvwriter.writerow([
            table_name,
            f"{insertion_time:.2f}",
            f"{size_info['data_size']:.2f}",
            f"{size_info['index_size']:.2f}",
            f"{size_info['total_size']:.2f}",
            size_info['num_rows'],
            metrics['cpu_percent'],
            metrics['memory_percent'],
            metrics['disk_percent'],
            metrics['system'],
            metrics['processor'],
            metrics['python_version']
        ])

print(f"Results saved to {csv_filename}")

# Clean tables after the test
clean_tables(cursor)

# Close connection
connection.close()
