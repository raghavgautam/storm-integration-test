End to end storm-integration-tests
==================================

Bring up a cluster
------------------
Vagrant setup can be used for bringing up unsecure Storm 1.0
https://github.com/raghavgautam/storm-vagrant

Configs for running 
-------------------
Change following storm.yaml:
storm-integration-test/src/test/resources/storm.yaml

Running tests end to end from Commandline
-----------------------------------------
```sh
mvn clean integration-test
```

Running tests from IDE
----------------------
Make sure that the following is run before tests are launched.
```sh
mvn package -DskipTests
```

Submiting topologies
--------------------
All the topologies can be submitted using `storm jar` command.

Code
----
Start off by looking at file `DemoTest.java`.
