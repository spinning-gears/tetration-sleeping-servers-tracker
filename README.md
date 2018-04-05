# Tetration Analytics User App
## Sleeping servers tracker
### Purpose
This Tetration User App (PySpark) finds servers with a supeciously low network activity. Full visibility software sensor tracks all the traffic of servers, and streams it to the Tetration Cluster. This app compares this streaming traffic with the global traffic, deducing that if the server has almost only traffic with Tetration, it is quite asleep.

### Requirements
This User App (as always) runs in the context of a Scope of servers, that are supposed to run full visibility software sensor.

### Usage
Some configuration have to be provided like your Tetration cluster IPs, threshold percentage, how many days of traffic to look at. *This can take a while to run* (5, 10 minutes) depending on the load of your cluster and the number of days it looks at.

The .py file is only for your reference, the .ipynb (Jupyter notebook) file has to be imported into Tetration User App module

### Sample output
#### Please note that these are lab workloads running on purpose
```
10.49.166.84 seems inactive, Tetration flows are: 99.9% RX 100.0% TX
10.49.166.87 seems active, Tetration flows are: 2.0% RX 1.5% TX
```

### Related information
- PySpark: https://spark.apache.org/docs/1.6.2/api/python/index.html
- Pandas: http://pandas.pydata.org/pandas-docs/stable/
