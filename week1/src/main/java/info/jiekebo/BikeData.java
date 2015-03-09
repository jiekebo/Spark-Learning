package info.jiekebo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.joda.time.Days;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class BikeData {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }

        /**
         * Problem 1
         */
        SparkConf conf = new SparkConf().setAppName("info.jiekebo.BikeData").setMaster("local");
        conf.set("spark.hadoop.validateOutputSpecs", "false");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String[]> pipe = context
                .textFile(args[0])
                .filter((line) -> !line.startsWith("Trip"))
                .map((line) -> line.split(","));
        long count = pipe.count();

        Integer total = pipe
                .map((data) -> Integer.parseInt(data[1]))
                .reduce((a, b) -> a + b);

        System.out.println("Average total time " + total / count / 60 + " minutes");

        /**
         * Problem 2
         */
        DateFormat df = new SimpleDateFormat("MM/dd/yy", Locale.ENGLISH);

        List<Tuple2<Date, Integer>> collect = pipe
                .mapToPair((data) ->
                                new Tuple2<>(
                                        df.parse(data[2]),
                                        df.parse(data[5])
                                )
                )
                .mapToPair((interval) ->
                                new Tuple2<>(
                                        interval._1(),
                                        Days.daysBetween(new DateTime(interval._1()), new DateTime(interval._2())).getDays()
                                )
                )
                .flatMap((dateWithDays) -> {
                            Collection<Date> days = new ArrayList<>();
                            Date date = dateWithDays._1();
                            for (int i = 0; i <= dateWithDays._2(); i++) {
                                days.add((new DateTime(date).plusDays(i)).toDate());
                            }
                            return days;
                        }
                )
                .mapToPair((date) ->
                                new Tuple2<>(date, 1)
                )
                .reduceByKey((a, b) -> a + b)
                .collect();

        Collections.sort(collect, (a,b) -> b._2().compareTo(a._2()));

        System.out.println("Most popular day by frequency of " + collect.get(0)._2() + " was " + collect.get(0)._1());

        /**
         * Problem 3
         */
    }
}
