# Debezium Outbox [![GitHub license](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg?style=flat)](http://www.apache.org/licenses/LICENSE-2.0) [![Build Status](https://travis-ci.org/bellotapps/outbox-debezium.svg?branch=master)](https://travis-ci.org/bellotapps/outbox-debezium)

A framework for sending transactional messages using the outbox pattern, relying on debezium.

**Java 11 is required for this framework to be used.**


## Description

The framework consists of several libraries that helps sending messages from a producer to a consumer, using the outbox pattern, relying on debezium as a change data capture system.

## How to use it?

### Get it!

#### Maven central

The artifacts will be available in maven central once the first release is performed. In the meantime, you can build the artifacts from source.

#### Build from source

You can also build your own versions of the libraries.
**Maven is required for this**.

```
$ git clone https://github.com/bellotapps/outbox-debezium.git
$ cd outbox-debezium
$ mvn clean install
```

**Note:** There are several profiles defined in the root's ```pom.xml``` file. The ```local-deploy``` profile will also install the sources and javadoc jars. You can use it like this:

```
$ mvn clean install -P local-deploy
```

**Note:** You can also download the source code from [https://github.com/bellotapps/outbox-debezium/archive/master.zip](https://github.com/bellotapps/outbox-debezium/archive/master.zip)

**Note:** If you just want to get the JAR files, you must use the following command which won't install the libraries in your local repository.

```
$ mvn clean package
```

### Bill of Materials

This project includes a Bill Of Materials in order to make it easier to import the libraries. Include the following in your ```pom.xml```.

```xml
<dependencyManagement>
    <dependencies>
        <!-- ... -->
        <dependency>
            <groupId>com.bellotapps.messaging</groupId>
            <artifactId>outbox-debezium-bom</artifactId>
            <version>${outbox-debezium.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
        <!-- ... -->
    </dependencies>
</dependencyManagement>
```

After adding the ```outbox-debezium-bom``` artifact as an imported managed dependency, you can start using the different libraries in your project.

**Note:** A placeholder is used as ```version``` in the previous example to avoid changing this readme each time a new version is released. Replace the ```${outbox-debezium.version}``` placeholder with the actual version of the ```outbox-debezium``` project.



## Useful Resources

- [https://debezium.io/](https://debezium.io/)
- [https://kafka.apache.org](https://kafka.apache.org)
- [https://microservices.io/patterns/data/application-events.html](https://microservices.io/patterns/data/application-events.html)

## License

Copyright 2019 BellotApps

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
