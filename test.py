#!/usr/bin/env python
# coding: utf-8

# In[3]:


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

# In[4]:


connection = mysql.connector.connect(
    host="127.0.0.1",
    user="test_user",
    password="password",
    database="test_db")
cursor = connection.cursor()

# In[5]:


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
    if avg_interval > 10 ** 8:  # If average interval is greater than 100 seconds
        # Use exponential distribution for large intervals
        return np.random.exponential(avg_interval, total)
    else:
        # Use Poisson distribution for smaller intervals
        return np.random.poisson(avg_interval, total)


def generate_uuids(total, rpm):
    uuids = []
    # Calculate the average time between requests in nanoseconds
    avg_time_between_requests = (60 / rpm) * 10 ** 9
    intervals = generate_intervals(total, avg_time_between_requests)
    start_time = time.time_ns()
    current_time = start_time

    for i in range(total):
        # Simulate the passage of time
        current_time += int(intervals[i])
        uuid = uuid7(current_time)
        uuids.append(str(uuid))

        # Print progress
        if (i + 1) % 100000 == 0:
            print(f"Generated {i + 1} UUIDs")

    return uuids


def clean_tables(cursor):
    tables = [
        'uuidv4',
        'uuidv7_100rpm',
        'uuidv7_1000rpm',
        'uuidv7_5000rpm',
        'uuidv7_10000rpm',
        'uuidv7_10000rpm_large1',
        'uuidv7_10000rpm_large2',
        'uuidv7_10000rpm_large3',
        'uuidv7_10000rpm_large4',
        'uuidv4_large1',
        'uuidv4_large2',
        'uuidv4_large3',
        'uuidv4_large4',
        'incremental',
        'tenant_request_v4',
        'tenant_request_mixed_100rpm',
        'tenant_request_mixed_1000rpm',
        'tenant_request_mixed_5000rpm',
        'tenant_request_mixed_10000rpm',
        'request_tenant_mixed_100rpm',
        'request_tenant_mixed_1000rpm',
        'request_tenant_mixed_5000rpm',
        'request_tenant_mixed_10000rpm'
    ]
    for table in tables:
        cursor.execute(f"TRUNCATE TABLE {table}")
    print("All tables cleaned.")


def get_table_size(cursor, table_name):
    cursor.execute(
        f"SELECT table_name, round(((data_length + index_length) / 1024), 2) as 'Size (KB)' FROM information_schema.TABLES WHERE table_schema = DATABASE() AND table_name = '{table_name}'")
    return cursor.fetchone()[1]


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


# In[6]:


cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv4 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv7_100rpm (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv7_1000rpm (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv7_5000rpm (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv7_10000rpm (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
""")

# Additional tables with increasing row sizes for UUIDv7
cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv7_10000rpm_large1 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    extra_varchar VARCHAR(100)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv7_10000rpm_large2 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    extra_varchar VARCHAR(100),
    extra_text TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv7_10000rpm_large3 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    extra_varchar VARCHAR(100),
    extra_text TEXT,
    extra_blob BLOB
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv7_10000rpm_large4 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    extra_varchar VARCHAR(100),
    extra_text TEXT,
    extra_blob BLOB,
    extra_json JSON
)
""")

# Additional tables with increasing row sizes for UUIDv4
cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv4_large1 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    extra_varchar VARCHAR(100)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv4_large2 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    extra_varchar VARCHAR(100),
    extra_text TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv4_large3 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    extra_varchar VARCHAR(100),
    extra_text TEXT,
    extra_blob BLOB
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS uuidv4_large4 (
    uuid CHAR(36) PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    extra_varchar VARCHAR(100),
    extra_text TEXT,
    extra_blob BLOB,
    extra_json JSON
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS incremental (
    id INT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tenant_request_v4 (
    tenant_id CHAR(36),
    request_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, request_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tenant_request_mixed_100rpm (
    tenant_id CHAR(36),
    request_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, request_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tenant_request_mixed_1000rpm (
    tenant_id CHAR(36),
    request_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, request_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tenant_request_mixed_5000rpm (
    tenant_id CHAR(36),
    request_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, request_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tenant_request_mixed_10000rpm (
    tenant_id CHAR(36),
    request_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, request_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS request_tenant_mixed_100rpm (
    request_id CHAR(36),
    tenant_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (request_id, tenant_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS request_tenant_mixed_1000rpm (
    request_id CHAR(36),
    tenant_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (request_id, tenant_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS request_tenant_mixed_5000rpm (
    request_id CHAR(36),
    tenant_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (request_id, tenant_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS request_tenant_mixed_10000rpm (
    request_id CHAR(36),
    tenant_id CHAR(36),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (request_id, tenant_id)
)
""")

# In[7]:


RECORDS_NUM = 1000000

# In[8]:


# Generate UUIDs in advance
print("Generating UUIDs...")
uuidv4_list = [str(uuid.uuid4()) for _ in range(RECORDS_NUM)]
uuidv7_lists = {
    "100rpm": generate_uuids(RECORDS_NUM, 100),
    "1000rpm": generate_uuids(RECORDS_NUM, 1000),
    "5000rpm": generate_uuids(RECORDS_NUM, 5000),
    "10000rpm": generate_uuids(RECORDS_NUM, 10000)
}
print("UUID generation complete")

results = []

# In[9]:


# Define dummy data
extra_varchar_data = 'a' * 100
extra_text_data = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.' * 10
extra_blob_data = bytes([0] * 1024)
extra_json_data = json.dumps({"key": "value", "numbers": list(range(100))})

# In[10]:


# Test UUIDv4
print("Inserting UUIDv4...")
system_metrics = get_system_metrics()
start_time = time.time()
for uuid_value in uuidv4_list:
    cursor.execute("INSERT INTO uuidv4 (uuid) VALUES (%s)", (uuid_value,))
connection.commit()
end_time = time.time()
uuidv4_time = end_time - start_time
uuidv4_size = get_table_size(cursor, "uuidv4")
results.append(('UUIDv4', uuidv4_time, uuidv4_size, system_metrics))

clean_tables(cursor)

# In[11]:


# Test UUIDv7 at different RPM rates
for rpm, uuidv7_list in uuidv7_lists.items():
    print(f"Inserting UUIDv7 at {rpm}...")
    system_metrics = get_system_metrics()
    start_time = time.time()
    for uuid_value in uuidv7_list:
        cursor.execute(f"INSERT INTO uuidv7_{rpm} (uuid) VALUES (%s)", (uuid_value,))
    connection.commit()
    end_time = time.time()
    insertion_time = end_time - start_time
    table_size = get_table_size(cursor, f"uuidv7_{rpm}")
    results.append((f'UUIDv7_{rpm}', insertion_time, table_size, system_metrics))

clean_tables(cursor)

# In[12]:


# Test Tenant-Request V4
print("Inserting Tenant-Request V4...")
system_metrics = get_system_metrics()
start_time = time.time()
for tenant_id in uuidv4_list:
    request_id = str(uuid.uuid4())
    cursor.execute("INSERT INTO tenant_request_v4 (tenant_id, request_id) VALUES (%s, %s)", (tenant_id, request_id))
connection.commit()
end_time = time.time()
tenant_request_v4_time = end_time - start_time
tenant_request_v4_size = get_table_size(cursor, "tenant_request_v4")
results.append(('Tenant-Request V4', tenant_request_v4_time, tenant_request_v4_size, system_metrics))

clean_tables(cursor)

# In[13]:


# Test Tenant-Request Mixed and Request-Tenant Mixed at different RPM rates
for rpm, uuidv7_list in uuidv7_lists.items():
    print(f"Inserting Tenant-Request Mixed at {rpm}...")
    system_metrics = get_system_metrics()
    start_time = time.time()
    for tenant_id, request_id in zip(uuidv4_list, uuidv7_list):
        cursor.execute(f"INSERT INTO tenant_request_mixed_{rpm} (tenant_id, request_id) VALUES (%s, %s)",
                       (tenant_id, request_id))
    connection.commit()
    end_time = time.time()
    insertion_time = end_time - start_time
    table_size = get_table_size(cursor, f"tenant_request_mixed_{rpm}")
    results.append((f'Tenant-Request Mixed_{rpm}', insertion_time, table_size, system_metrics))

    print(f"Inserting Request-Tenant Mixed at {rpm}...")
    system_metrics = get_system_metrics()
    start_time = time.time()
    for request_id, tenant_id in zip(uuidv7_list, uuidv4_list):
        cursor.execute(f"INSERT INTO request_tenant_mixed_{rpm} (request_id, tenant_id) VALUES (%s, %s)",
                       (request_id, tenant_id))
    connection.commit()
    end_time = time.time()
    insertion_time = end_time - start_time
    table_size = get_table_size(cursor, f"request_tenant_mixed_{rpm}")
    results.append((f'Request-Tenant Mixed_{rpm}', insertion_time, table_size, system_metrics))

clean_tables(cursor)

# In[14]:


# Test for UUIDv4 tables with increasing row sizes
uuidv4_tables = [
    ('uuidv4_large1', ('uuid', 'extra_varchar'), (uuidv4_list, extra_varchar_data)),
    ('uuidv4_large2', ('uuid', 'extra_varchar', 'extra_text'), (uuidv4_list, extra_varchar_data, extra_text_data)),
    ('uuidv4_large3', ('uuid', 'extra_varchar', 'extra_text', 'extra_blob'),
     (uuidv4_list, extra_varchar_data, extra_text_data, extra_blob_data)),
    ('uuidv4_large4', ('uuid', 'extra_varchar', 'extra_text', 'extra_blob', 'extra_json'),
     (uuidv4_list, extra_varchar_data, extra_text_data, extra_blob_data, extra_json_data))
]

for table_name, columns, data in uuidv4_tables:
    print(f"Inserting into {table_name}...")
    system_metrics = get_system_metrics()
    start_time = time.time()
    for i in range(RECORDS_NUM):
        cursor.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                       tuple(d[i] if isinstance(d, list) else d for d in data))
    connection.commit()
    end_time = time.time()
    insertion_time = end_time - start_time
    table_size = get_table_size(cursor, table_name)
    results.append((table_name, insertion_time, table_size, system_metrics))

clean_tables(cursor)

# In[15]:


# Test for UUIDv7 tables with increasing row sizes
uuidv7_tables = [
    ('uuidv7_10000rpm_large1', ('uuid', 'extra_varchar'), (uuidv7_lists["10000rpm"], extra_varchar_data)),
    ('uuidv7_10000rpm_large2', ('uuid', 'extra_varchar', 'extra_text'),
     (uuidv7_lists["10000rpm"], extra_varchar_data, extra_text_data)),
    ('uuidv7_10000rpm_large3', ('uuid', 'extra_varchar', 'extra_text', 'extra_blob'),
     (uuidv7_lists["10000rpm"], extra_varchar_data, extra_text_data, extra_blob_data)),
    ('uuidv7_10000rpm_large4', ('uuid', 'extra_varchar', 'extra_text', 'extra_blob', 'extra_json'),
     (uuidv7_lists["10000rpm"], extra_varchar_data, extra_text_data, extra_blob_data, extra_json_data))
]

for table_name, columns, data in uuidv7_tables:
    print(f"Inserting into {table_name}...")
    system_metrics = get_system_metrics()
    start_time = time.time()
    for i in range(RECORDS_NUM):
        cursor.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                       tuple(d[i] if isinstance(d, list) else d for d in data))
    connection.commit()
    end_time = time.time()
    insertion_time = end_time - start_time
    table_size = get_table_size(cursor, table_name)
    results.append((table_name, insertion_time, table_size, system_metrics))

clean_tables(cursor)

# In[16]:


# Print results
for result in results:
    print(f"{result[0]} insertion time: {result[1]:.2f} seconds")
    print(f"{result[0]} table size: {result[2]:.2f} KB")



timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"mysql_insertion_test_results_{RECORDS_NUM}_records_{timestamp}.csv"

with open(csv_filename, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['ID Type', 'Insertion Time (s)', 'Table Size (KB)'])
    csvwriter.writerows(results)

print(f"Results saved to {csv_filename}")

# In[18]:


# Clean tables after the test
clean_tables(cursor)

# In[19]:


connection.close()

# In[118]:


# In[ ]:
