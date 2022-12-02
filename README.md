<img src=img/apache_spark_logo.png alt="Apache Spark Logo" width="193" height="100">
<img src=img/apache_flink_logo.png alt="Apache Flink Logo" width="204" height="100">

--------------------------------------------------------------------------------
[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Top Language](https://img.shields.io/github/languages/top/johanneshagspiel/big-data-scripts
)](https://github.com/johanneshagspiel/big-data-scripts)
[![Latest Release](https://img.shields.io/github/v/release/johanneshagspiel/big-data-scripts)](https://github.com/johanneshagspiel/big-data-scripts/releases/)

# Big Data Scripts

This repository contains a collection of Apache Spark scripts used to get familiar with the basics of batch processing of big data and a collection of Apache Flink scripts used to get familiar with the basics of stream processing of big data.

## Features

The Apache Spark scripts cover a range of topics such as:

- manipulating RDDs via:
  - functional programming principles like pattern matching
  - regex
  - functions like:
    - `map`
    - `flatMap`
    - `reduceByKey`
    - `flatten`
    - `filter`
- manipulating DataFrames via:
  - Spark SQL
  - custom aggregation functions using `Window`

The Apache Flink scripts cover a range of topics such as:

- basic manipulation of DataStreams via functions like:
  - `map`
  - `filter`
  - `flatMap`
- working with stateful streams via `keyBy`
- dealing with infinite streams via:
  - different kinds of window assigners like `TumblingEventTimeWindows` or `SlidingEventTimeWindows`
  - keyed and non-keyed windows
  - new `ProcessWindowFunction`

## Tools

| Purpose                                                        | Name                                        |
|----------------------------------------------------------------|---------------------------------------------|
| Programming language                                           | [Scala](https://scala-lang.org/)            |
| Cluster computing framework | [Apache Spark](https://spark.apache.org/), [Apache Flink](https://flink.apache.org/) |

## Installation Process

It is assumed that both a [Java JDK](https://openjdk.org/) and an IDE such as [IntelliJ](https://www.jetbrains.com/idea/) are installed and that the users operating system is Windows.

- Install the Scala support plugin for your IDE.
- Import the corresponding sub folder of this repository as a Maven project and resolve all dependencies.

## Licence

These Big Data scripts are published under the MIT licence, which can be found in the [LICENSE](LICENSE) file. For this repository, the terms laid out there shall not apply to any individual that is currently enrolled at a higher education institution as a student. Those individuals shall not interact with any other part of this repository besides this README in any way by, for example cloning it or looking at its source code or have someone else interact with this repository in any way.

## References

The Apache Spark logo was taken from [Wikipedia](https://upload.wikimedia.org/wikipedia/commons/e/ea/Spark-logo-192x100px.png) and the Apache Flink logo from . 
