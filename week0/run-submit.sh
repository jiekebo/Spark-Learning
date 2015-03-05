#!/bin/bash
spark-submit --class info.jiekebo.WordCount target/week0-1.0.0-SNAPSHOT.jar src/test/resources/loremipsum.txt target/result
