#!/usr/bin/env bash
mvn clean package -Dskiptest
cp ./target/rowCount-1.0-SNAPSHOT.jar /home/liuliumi/tools/hbase-0.98.11-hadoop2/lib
stop-hbase.sh;start-hbase.sh
#./test.sh test1 0 998
