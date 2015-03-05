#!/bin/bash
spark-submit --class info.jiekebo.BikeData target/week1-1.0.0-SNAPSHOT.jar src/test/resources/bike_trip_data.csv
