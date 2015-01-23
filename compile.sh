#!/bin/bash
rm -rf $2
rm -rf output2
rm -rf intermediate1
rm -rf intermediate2
rm -rf *.class
hadoop com.sun.tools.javac.Main main/*.java subscriber/*.java phase1/*.java phase2/*.java phase3/*.java
jar cf subscriberRanker.jar main/*.class subscriber/*.class phase1/*.class phase2/*.class phase3/*.class
hadoop jar subscriberRanker.jar main.SubscriberRanker $1 $2
