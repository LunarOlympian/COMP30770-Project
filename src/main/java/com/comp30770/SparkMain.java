package com.comp30770;

import com.opencsv.CSVReader;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.awt.*;
import java.io.StringReader;

import org.apache.spark.storage.StorageLevel;
import org.checkerframework.checker.units.qual.A;
import scala.Tuple10;
import scala.Tuple2;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SparkMain {
    public static AtomicLong timing1 = new AtomicLong();
    public static void main(String[] args) {
        // Initialize Spark
        SparkConf conf = new SparkConf().setAppName("Spotify Music Analysis").setMaster("local[*]");
        conf.set("spark.executor.instances", "4");
        conf.set("spark.executor.cores", "4");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("ERROR");

        long startTime = System.currentTimeMillis();

        // Load the dataset
        JavaRDD<String> rawData = sc.textFile("target/classes/csv/Merged_Top_Spotify_Songs.csv");

        // Remove the header (handling different line endings)
        String header = rawData.first().trim();
        JavaRDD<String> data = rawData.filter(line -> !line.trim().equals(header));

        JavaRDD<Long> timings = sc.parallelize(new ArrayList<>(List.of(new Long[]{0L, 1L})));

        // Parse the data into a structured format (e.g., a JavaRDD of String arrays)
        JavaRDD<String[]> validRowsRDD = data.map(line -> {
            try (CSVReader reader = new CSVReader(new StringReader(line))) {
                List<String[]> records = reader.readAll();
                if (!records.isEmpty()) {
                    return records.get(0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return new String[0]; // Return an empty row if parsing fails
        });


        // Compute averages for duration, energy, loudness, and tempo by country
        JavaPairRDD<String, Tuple5<Double, Double, Double, Double, Double>> averages =
                validRowsRDD.mapToPair(row -> new Tuple2<>(row[6].trim(), new Tuple10<>(parseDouble(row[10]), 1, parseDouble(row[14]), 1, parseDouble(row[16]), 1, parseDouble(row[18]), 1, parseDouble(row[23]), 1)))
                .reduceByKey((a, b) -> new Tuple10<>(a._1() + b._1(), a._2() + b._2(), a._3() + b._3(), a._4() + b._4(), a._5() + b._5(),
                        a._6() + b._6(), a._7() + b._7(), a._8() + b._8(), a._9() + b._9(), a._10() + b._10()))

                .mapValues(tuple -> new Tuple5<>(tuple._1() / tuple._2(), tuple._3() / tuple._4(), tuple._5() / tuple._6(),
                tuple._7() / tuple._8(), tuple._9() / tuple._10()));



        // Collect and print the results
        Map<String, Tuple5<Double, Double, Double, Double, Double>> countryAverages = averages.collectAsMap();
        ArrayList<Country> countries = new ArrayList<>();
        for(String c : countryAverages.keySet()) {
            Tuple5<Double, Double, Double, Double, Double> avgs = countryAverages.get(c);
            countries.add(new Country(c, avgs._1(), avgs._2(), avgs._3(), avgs._4(), avgs._5()));
            System.out.println(countries.get(countries.size() - 1));
        }
        // 1742773192933 1742773183150 1742773192503


        // Stop the Spark context
        sc.stop();

        long endTime = System.currentTimeMillis();    // End timer
        long timeDuration = endTime - startTime;
        System.out.println(endTime);
        System.out.println(timing1.get());

        System.out.println("Total Runtime: " + timeDuration + " ms (" + (timeDuration / 1000.0) + " seconds)");
    }

    // Helper function to clean and parse double values
    private static double parseDouble(String value) {
        return Double.parseDouble(value.replaceAll("\"", "").trim());
    }

    // Helper function to check if a value is a valid number
    private static boolean isValidNumeric(String str) {
        try {
            Double.parseDouble(str.replaceAll("\"", "").trim());
            return true;
        } catch (NumberFormatException | NullPointerException e) {
            return false;
        }
    }

    // Helper function to print results
    private static void printResults(String title, List<Tuple2<String, Double>> results) {
        System.out.println(title + ":");
        results.forEach(result -> System.out.println(result._1 + ": " + result._2));
    }
}
