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
import scala.Tuple10;
import scala.Tuple2;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkMain {
    public static void main(String[] args) {
        // Initialize Spark
        SparkConf conf = new SparkConf().setAppName("Spotify Music Analysis").setMaster("local[*]");
        conf.set("spark.executor.instances", "4");
        conf.set("spark.executor.cores", "4");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("ERROR");

        long startTime = System.currentTimeMillis();

        // Load the dataset
        JavaRDD<String> rawData = sc.textFile("target/classes/csv/Top_spotify_songs.csv");

        // Remove the header (handling different line endings)
        String header = rawData.first().trim();
        JavaRDD<String> data = rawData.filter(line -> !line.trim().equals(header));

        // Parse the data into a structured format (e.g., a JavaRDD of String arrays)
        JavaRDD<String[]> parsedData = data.map(line -> {
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


        // Cache the RDD for faster access
        parsedData.cache();

        // Helper function to clean numeric values
        JavaRDD<String[]> validRowsRDD = parsedData
                .filter(row -> row.length > 23) // Ensure the row has enough columns
                .filter(row -> row[6].matches("^[A-Z]{2}$")) // Ensure country is valid
                .filter(row -> isValidNumeric(row[10]) && isValidNumeric(row[14]) && isValidNumeric(row[16]) && isValidNumeric(row[18]) && isValidNumeric(row[23])); // Validate numeric fields
        validRowsRDD.cache();

        // Compute averages for duration, energy, loudness, and tempo by country
        JavaPairRDD<String, Tuple5<Double, Double, Double, Double, Double>> averages =
                validRowsRDD.mapToPair(row -> new Tuple2<>(row[6].trim(), new Tuple10<>(parseDouble(row[10]), 1, parseDouble(row[14]), 1, parseDouble(row[16]), 1, parseDouble(row[18]), 1, parseDouble(row[23]), 1)))
                .reduceByKey((a, b) -> new Tuple10<>(a._1() + b._1(), a._2() + b._2(), a._3() + b._3(), a._4() + b._4(), a._5() + b._5(),
                        a._6() + b._6(), a._7() + b._7(), a._8() + b._8(), a._9() + b._9(), a._10() + b._10()))

                .mapValues(tuple -> new Tuple5<>(tuple._1() / tuple._2(), tuple._3() / tuple._4(), tuple._5() / tuple._6(),
                tuple._7() / tuple._8(), tuple._9() / tuple._10())).cache();


        // Collect and print the results
        List<Tuple2<String, Tuple5<Double, Double, Double, Double, Double>>> countryAverages = averages.collect();
        ArrayList<Country> countries = new ArrayList<>();
        for(Tuple2<String, Tuple5<Double, Double, Double, Double, Double>> c : countryAverages) {
            Tuple5<Double, Double, Double, Double, Double> avgs = c._2;
            countries.add(new Country(c._1, avgs._1(), avgs._2(), avgs._3(), avgs._4(), avgs._5()));
            System.out.println(countries.get(countries.size() - 1));
        }


        // Stop the Spark context
        sc.stop();

        long endTime = System.currentTimeMillis();    // End timer
        long timeDuration = endTime - startTime;

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
