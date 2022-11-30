<img src=resources/apache_flink_logo.png alt="Apache Flink Logo" width="256" height="125">

--------------------------------------------------------------------------------

# Apache Flink Scripts

This repository contains a collection of Apache Flink scripts used to get familiar with the basics of stream processing of big data.

## Features

These scripts cover a range of topics of Apache Flink such as:

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

| Purpose                                                        | Name                                      |
|----------------------------------------------------------------|-------------------------------------------|
| Programming language                                           | [Scala](https://scala-lang.org/)          |
| Cluster computing framework | [Apache Flink](https://flink.apache.org/) |

## Installation Process

It is assumed that both a [Java JDK](https://openjdk.org/) and an IDE such as [IntelliJ](https://www.jetbrains.com/idea/) are installed and that the users operating system is Windows.

- Install the Scala support plugin for your IDE.
- Import this repository as a Maven project and resolve all dependencies.

## Contributors

These scripts were created together with Saru.

## Licence

These Apache Flink scripts are published under the MIT licence, which can be found in the [LICENSE](LICENSE) file. For this repository, the terms laid out there shall not apply to any individual that is currently enrolled at a higher education institution as a student. Those individuals shall not interact with any other part of this repository besides this README in any way by, for example cloning it or looking at its source code or have someone else interact with this repository in any way.

## References

The logo was taken from [Wikipedia](https://en.wikipedia.org/wiki/Apache_Flink#/media/File:Apache_Flink_logo.svg). 
