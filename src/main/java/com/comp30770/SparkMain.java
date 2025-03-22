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


        // Helper function to clean numeric values
        JavaRDD<String[]> validRowsRDD = parsedData
                .filter(row -> row.length > 23) // Ensure the row has enough columns
                .filter(row -> row[6].matches("^[A-Z]{2}$")) // Ensure country is valid
                .filter(row -> isValidNumeric(row[10]) && isValidNumeric(row[14]) && isValidNumeric(row[16]) && isValidNumeric(row[18]) && isValidNumeric(row[23])); // Validate numeric fields
        validRowsRDD.cache();

        JavaPairRDD<String, Tuple2<double[], Integer>> combinedMetrics = validRowsRDD
                .mapToPair(row -> {
                    String country = row[6].trim();
                    double duration = parseDouble(row[10]);
                    double energy = parseDouble(row[14]);
                    double loudness = parseDouble(row[16]);
                    double tempo = parseDouble(row[23]);
                    double speechiness = parseDouble(row[18]);

                    double[] features = new double[]{duration, energy, loudness, tempo, speechiness};
                    return new Tuple2<>(country, new Tuple2<>(features, 1));
                })
                .reduceByKey((a, b) -> {
                    double[] sumA = a._1;
                    double[] sumB = b._1;
                    double[] combined = new double[5];
                    for (int i = 0; i < 5; i++) {
                        combined[i] = sumA[i] + sumB[i];
                    }
                    return new Tuple2<>(combined, a._2 + b._2);
                });

// Print averages per country
        combinedMetrics.foreach(tuple -> {
            String country = tuple._1;
            double[] sums = tuple._2._1;
            int count = tuple._2._2;

            double avgDuration = sums[0] / count;
            double avgEnergy = sums[1] / count;
            double avgLoudness = sums[2] / count;
            double avgTempo = sums[3] / count;
            double avgSpeechiness = sums[4] / count;

            System.out.println("Country: " + country +
                    " | Duration: " + avgDuration +
                    " | Energy: " + avgEnergy +
                    " | Loudness: " + avgLoudness +
                    " | Tempo: " + avgTempo +
                    " | Speechiness: " + avgSpeechiness);
        });


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
