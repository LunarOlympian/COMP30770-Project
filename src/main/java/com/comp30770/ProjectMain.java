package com.comp30770;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ProjectMain {

    public static void main(String[] args) throws URISyntaxException, IOException, CsvException {
        // Loads the csv file
        CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(true).build();
        CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(Path.of(ClassLoader.getSystemResource("csv/vgchartz-2024.csv").toURI())))
                .withCSVParser(parser).withSkipLines(0).build();

        ArrayList<String[]> games = new ArrayList<>(reader.readAll());

        Map<String, Integer> consoles = new HashMap<>();
        for(String[] game : games) {
            if(game.length < 14) continue;
            consoles.put(game[2].trim(), consoles.getOrDefault(game[2].trim(), 0) + 1);
        }

        //games.forEach((game) -> games.get(game));

        SparkSession spark = SparkSession.builder()
                .appName("Simple Spark Application")
                .master("local")
                .getOrCreate();

        for(String console : consoles.keySet()) {
            System.out.println(console);
        }
    }
}
