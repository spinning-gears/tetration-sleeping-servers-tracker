"""
Copyright (c) 2018 Cisco and/or its affiliates.
This software is licensed to you under the terms of the Cisco Sample
Code License, Version 1.0 (the "License"). You may obtain a copy of the
License at
               https://developer.cisco.com/docs/licenses
All use of the material herein must be in accordance with the terms of
the License. All rights not expressly granted by the License are
reserved. Unless required by applicable law or agreed to separately in
writing, software distributed under the License is distributed on an "AS
IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied.
"""

__author__ = "Damien Gouju"
__copyright__ = "Copyright (c) 2018 Cisco and/or its affiliates."
__license__ = "Cisco Sample Code License, Version 1.0"

# pylint: disable=invalid-name
# Configuration and imports
from datetime import datetime, timedelta
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# Tetrations IPs
# 8RU has 6 collectors, 39RU has 16
# You can get this list from Maintenance --> VM Information
collectors = ["10.49.166.8", "10.49.166.9", "10.49.166.10", "10.49.166.11", "10.49.166.12", "10.49.166.13"]
# Collector VIP is the next IP after UI VIP
collector_vip = "10.49.166.5"

# How long in past? (in days)
# 
period = 7
# Maximum % of traffic per server that is only tetration streaming
threshold = 75
# If we want to look at IPv6 too or IPv4 only
ipv6 = False

# Lets collect the global amount of traffic per server of this scope
now = datetime.now()
end_timestamp = now.strftime("%Y%m%d%H00")
start_timestamp = (datetime.strptime(end_timestamp, "%Y%m%d%H00") - timedelta(days=period)).strftime("%Y%m%d%H00")

inventory_data = sc._jvm.com.tetration.apps.IO.read(sqlContext._ssql_ctx, "/tetration/inventory/", "PARQUET", start_timestamp, end_timestamp)
inventory_data.registerTempTable("inventory")

time_start = datetime.now()
if ipv6:
    inventory = sqlContext.sql("SELECT ip_address, sum(rx_byte_count) AS rx_bytes, sum(tx_byte_count) AS tx_bytes FROM inventory WHERE (rx_byte_count > 0 OR rx_byte_count > 0) GROUP BY ip_address").toPandas()
else:
    inventory = sqlContext.sql("SELECT ip_address, sum(rx_byte_count) AS rx_bytes, sum(tx_byte_count) AS tx_bytes FROM inventory WHERE (rx_byte_count > 0 OR rx_byte_count > 0) AND ip_address LIKE '%.%.%.%' GROUP BY ip_address").toPandas()

sqlContext.dropTempTable("inventory")
time_end = datetime.now()

print("Inventory query duration: "+str(time_end-time_start))
#print(inventory)

# Lets collect the amount of Tetration sensors-related traffic per server of this scope
# Building the SQL query with the list of collectors
first = True
for server in collectors:
    if first:
        where_clause = " dst_address = \'"+server+"\' "
        first = False
    else:
        where_clause = where_clause + "OR dst_address = \'"+server+"\' "
where_clause = "(("+where_clause+") AND (dst_port = 5640 OR dst_port = 5660)) OR (dst_address = \'"+collector_vip+"\' AND dst_port = 443)"
#print(where_clause)

flow_data = sc._jvm.com.tetration.apps.IO.read(sqlContext._ssql_ctx, "/tetration/flows/", "PARQUET", start_timestamp, end_timestamp)
flow_data.registerTempTable("flows")

time_start = datetime.now()
if ipv6:
    src_flows = sqlContext.sql("SELECT src_address, sum(rev_bytes) AS rx_bytes, sum(fwd_bytes) AS tx_bytes FROM flows WHERE "+where_clause+" GROUP BY src_address").toPandas()
else:
    src_flows = sqlContext.sql("SELECT src_address, sum(rev_bytes) AS rx_bytes, sum(fwd_bytes) AS tx_bytes FROM flows WHERE "+where_clause+" AND src_address LIKE '%.%.%.%' GROUP BY src_address").toPandas()

sqlContext.dropTempTable("flows")
time_end = datetime.now()

print("Flow query duration: "+str(time_end-time_start))
#print(src_flows)

# Lets compare
for ip in src_flows['src_address']:
    # Inventory bytes can't be 0 as Tetration flows are included and seen...
    tx_ratio = float(int(src_flows[src_flows['src_address'].isin([ip])]['tx_bytes']) / int(inventory[inventory['ip_address'].isin([ip])]['tx_bytes']))
    rx_ratio = float(int(src_flows[src_flows['src_address'].isin([ip])]['rx_bytes']) / int(inventory[inventory['ip_address'].isin([ip])]['rx_bytes']))
    if tx_ratio*100 > float(threshold) and rx_ratio*100 > float(threshold):
        print(ip+" seems inactive, Tetration flows are: "+'%.1f' % (rx_ratio*100)+"% RX "+'%.1f' % (tx_ratio*100)+"% TX")
    else:
        print(ip+" seems active, Tetration flows are: "+'%.1f' % (rx_ratio*100)+"% RX "+'%.1f' % (tx_ratio*100)+"% TX")
