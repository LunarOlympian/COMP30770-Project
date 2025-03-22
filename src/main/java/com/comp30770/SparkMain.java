package com.comp30770;

import com.opencsv.CSVReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.StringReader;
import scala.Tuple2;

import java.util.List;

public class SparkMain {
    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();
        // Initialize Spark
        SparkConf conf = new SparkConf().setAppName("Spotify Music Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("ERROR");

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
                .filter(row -> isValidNumeric(row[10]) && isValidNumeric(row[14]) && isValidNumeric(row[16]) && isValidNumeric(row[23])); // Validate numeric fields

        // Compute averages for duration, energy, loudness, and tempo by country
        JavaPairRDD<String, Tuple2<Double, Integer>> duration = validRowsRDD
                .mapToPair(row -> new Tuple2<>(row[6].trim(), new Tuple2<>(parseDouble(row[10]), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        JavaPairRDD<String, Tuple2<Double, Integer>> energy = validRowsRDD
                .mapToPair(row -> new Tuple2<>(row[6].trim(), new Tuple2<>(parseDouble(row[14]), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        JavaPairRDD<String, Tuple2<Double, Integer>> loudness = validRowsRDD
                .mapToPair(row -> new Tuple2<>(row[6].trim(), new Tuple2<>(parseDouble(row[16]), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        JavaPairRDD<String, Tuple2<Double, Integer>> tempo = validRowsRDD
                .mapToPair(row -> new Tuple2<>(row[6].trim(), new Tuple2<>(parseDouble(row[23]), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

        JavaPairRDD<String, Tuple2<Double, Integer>> speechiness = validRowsRDD
                .mapToPair(row -> new Tuple2<>(row[6].trim(), new Tuple2<>(parseDouble(row[18]), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));


        // Calculate averages
        JavaPairRDD<String, Double> averageDurations = duration.mapValues(tuple -> tuple._1 / tuple._2);
        JavaPairRDD<String, Double> averageEnergy = energy.mapValues(tuple -> tuple._1 / tuple._2);
        JavaPairRDD<String, Double> averageLoudness = loudness.mapValues(tuple -> tuple._1 / tuple._2);
        JavaPairRDD<String, Double> averageTempos = tempo.mapValues(tuple -> tuple._1 / tuple._2);
        JavaPairRDD<String, Double> averageSpeechiness = speechiness.mapValues(tuple -> tuple._1 / tuple._2);


        // Collect and print the results
        printResults("Average Durations by Country", averageDurations.collect());
        printResults("Average Energy by Country", averageEnergy.collect());
        printResults("Average Loudness by Country", averageLoudness.collect());
        printResults("Average Tempos by Country", averageTempos.collect());
        printResults("Average Speechiness by Country", averageSpeechiness.collect());


        // Stop the Spark context
        sc.stop();

        long endTime = System.currentTimeMillis();    // End timer
        long timeDuration = endTime - startTime;

        System.out.println("Total Runtime: " + duration + " ms (" + (timeDuration / 1000.0) + " seconds)");
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
