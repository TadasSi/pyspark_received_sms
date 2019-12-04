# Process received_sms

The aim of this project is to process car parking event log.

When parking your car in Vilnius, you can manage your parking booking with SMS messages:

• you send message "Start X AAA000" to start parking period

• X represents code of the zone (you have these options: M, R, G, Z)

• AAA000 represents your car plate number

• you send message "Stop" to stop parking period


# The main goal is to parse and process input data to calculate parking sessions.

• The session starts with the first Start message from a specific phone number.

• The active session ends with the first Stop message from the same phone, which
started the session.

• The repeating Start messages for any active session should be ignored.

• The user can start new session only after stopping previous session.

• The session only starts if the Start message is received within the billing period for
that day: a) M zone is billed from 8 to 24 hours b) R zone is billed from 8 to 22 hours,
except Sundays c) G zone is billed from 8 to 20 hours, except Sundays d) Z zone is
billed from 8 to 18 hours, except Sundays

• The session also finishes at the end of billing period for that day, even if user did not
send any messages


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
