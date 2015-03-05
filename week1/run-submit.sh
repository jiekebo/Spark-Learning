#!/bin/bash
spark-submit --class org.sparkexample.WordCount target/first-example-1.0-SNAPSHOT.jar src/test/resources/loremipsum.txt target/result
