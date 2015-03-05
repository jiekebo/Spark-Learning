package info.jiekebo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class BikeData {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("info.jiekebo.BikeData").setMaster("local");
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
    }
}
