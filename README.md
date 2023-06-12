## Traffic Counter Application

This application reads a file where each line contains a timestamp in ISO 8601 format(denoting the start of half hour) and the number of cars seen in that half hour.

The application outputs the following:
- The number of cars seen in total
- A sequence of lines where each line contains a date (in yyyy-mm-dd format) and the
number of cars seen on that day (eg. 2016-11-23 289) for all days listed in the input file.
- The top 3 half hours with most cars, in the same format as the input file
- The 1.5 hour period with least cars (i.e. 3 contiguous half hour records)

#### Example Output for valid data:

```
23/06/12 18:06:01 WARN TrafficCounterApp: Total cars for all days:
+----------+
|Total Cars|
+----------+
|       398|
+----------+

23/06/12 18:06:01 WARN TrafficCounterApp: Total cars by date:
+----------+--------------+
|      Date|Number of Cars|
+----------+--------------+
|2021-12-01|           179|
|2021-12-05|            81|
|2021-12-08|           134|
|2021-12-09|             4|
+----------+--------------+

23/06/12 18:06:02 WARN TrafficCounterApp: Top 3 half hours with most cars:
+-------------------+--------------+
|Date               |Number of Cars|
+-------------------+--------------+
|2021-12-01T07:30:00|46            |
|2021-12-01T08:00:00|42            |
|2021-12-08T18:00:00|33            |
+-------------------+--------------+

23/06/12 18:06:02 WARN TrafficCounterApp: 1.5 hour period with least cars:
+-------------------+-------------------+--------------+
|Window start       |Window end         |Number of Cars|
+-------------------+-------------------+--------------+
|2021-12-01 23:00:00|2021-12-02 00:30:00|0             |
+-------------------+-------------------+--------------+


```

#### Example Output for incorrect file path or no data:

```
23/06/12 17:24:23 ERROR TrafficDataApp: Exception occurred: org.apache.spark.sql.AnalysisException: [PATH_NOT_FOUND] Path does not exist: file:/Users/manasibelekar/project/traffic-counter/src/main/resources/foo.csv.
23/06/12 17:24:24 ERROR TrafficDataApp: No data found. Check if your file path is correct or if file has valid data! 
```

### Set up

### How to run this application
1. Run spark cluster by running `auto/start-spark` in your terminal. This is a docker compose set up using bitnami docker image for spark.
2. Application contains data file in `src/main/resources` directory
2. Application can be run using one of the two ways:
   - Application can be run using an editor like IntelliJ. To run application from IntelliJ please add following VM options to the run configuration
     `--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED`
   - From command line using `auto/run`. The `auto/run` is a wrapper around `sbt run`
   
#### To run all the tests:

Run spark cluster prior to running tests:

```
$ auto/start-spark
```

Run unit tests:

```
$ auto/test
```

#### Run Application:

If spark cluster is not running, start it prior to running application:

```
$ auto/start-spark
```

Run application:

```
$ auto/run
```

Stop spark cluster
```
$ auto/stop-spark
```

### Assumptions:
To keep it simple and meet all the requirements, I have made following assumptions:

- Source data will always be in expected format
- Source data will always be clean
- All source data can fit into memory for manipulation

### Technical Design choices:

#### Case classes (models):

- `TrafficData` models a single record from the data file
- `EnrichedTrafficData` models enriched record that has additional fields

#### Data Load:

- `Loader` loads data from the file into a spark dataset

#### Data Aggregation:

- `Aggregator` has methods to perform dataset operations.

#### Application

- `Main` ties together `Loader` and `Aggregator` classes to generate the final output.

#### Tests:
- Unit tests are written using `spark-fast-tests` to cover the functionality.
