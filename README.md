# process received_sms

The aim of this project is to process car parking event log.

# How to prepare for running

1. Download main load_received_sms.py file
2. Download and put to top level "input" directory "received_sms.csv" file from https://github.com/vilnius/mparking
3. Run script
4. Results will be find in "output" directory.

### How to run

Use this spark command to run script:

```
{path to spark dir i.e} /spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
   --master yarn \
   --deploy-mode cluster \
   --conf "spark.pyspark.python={path to python i.e.} /spark240python3/bin/python" \
   --conf "spark.pyspark.driver.python={path to python driver i.e.} /spark240python3/bin/python" \
   ``{path to main load_received_sms.py file}``
```
