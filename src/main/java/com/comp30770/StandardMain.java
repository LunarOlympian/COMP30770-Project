package com.comp30770;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class StandardMain {

    public static void main(String[] args) throws URISyntaxException, IOException, CsvException {
        long startTime = System.currentTimeMillis();
        // Loads the csv file
        CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
        CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(Path.of(ClassLoader.getSystemResource("csv/Top_spotify_songs.csv").toURI())))
                .withCSVParser(parser).build();

        ArrayList<String[]> songs = new ArrayList<>(reader.readAll());

        // Need a better way to do this
        Map<String, Pair<Double, Integer>> duration = new HashMap<>(); // 10
        Map<String, Pair<Double, Integer>> energy = new HashMap<>(); // 14
        Map<String, Pair<Double, Integer>> loudness = new HashMap<>(); // 16
        Map<String, Pair<Double, Integer>> spechiness = new HashMap<>(); // 18
        Map<String, Pair<Double, Integer>> tempo = new HashMap<>(); // 23
        for(String[] song : songs) {
            String country = song[6];
            if(country.isEmpty() || country.equalsIgnoreCase("country")) continue;
            Pair<Double, Integer> durationVals = duration.getOrDefault(country, new Pair<>(0d, 0));
            Pair<Double, Integer> energyVals = energy.getOrDefault(country, new Pair<>(0d, 0));
            Pair<Double, Integer> loudnessVals = loudness.getOrDefault(country, new Pair<>(0d, 0));
            Pair<Double, Integer> speechinessVals = spechiness.getOrDefault(country, new Pair<>(0d, 0));
            Pair<Double, Integer> tempoVals = tempo.getOrDefault(country, new Pair<>(0d, 0));

            duration.put(country, new Pair<>(durationVals.getValue0() + Double.parseDouble(song[10]), durationVals.getValue1() + 1));
            energy.put(country, new Pair<>(energyVals.getValue0() + Double.parseDouble(song[14]), energyVals.getValue1() + 1));
            loudness.put(country, new Pair<>(loudnessVals.getValue0() + Double.parseDouble(song[16]), loudnessVals.getValue1() + 1));
            spechiness.put(country, new Pair<>(speechinessVals.getValue0() + Double.parseDouble(song[18]), speechinessVals.getValue1() + 1));
            tempo.put(country, new Pair<>(tempoVals.getValue0() + Double.parseDouble(song[23]), tempoVals.getValue1() + 1));
        }

        ArrayList<Country> countries = new ArrayList<>();

        for(String c : duration.keySet()) {
            countries.add(new Country(c, duration.get(c).getValue0() / duration.get(c).getValue1(),
                    energy.get(c).getValue0() / energy.get(c).getValue1(),
                    loudness.get(c).getValue0() / loudness.get(c).getValue1(),
                    spechiness.get(c).getValue0() / spechiness.get(c).getValue1(),
                    tempo.get(c).getValue0() / tempo.get(c).getValue1()));
        }
        countries.sort(Comparator.comparing(c -> c.country));

        countries.forEach(System.out::println);

        long endTime = System.currentTimeMillis();    // End timer
        long timeDuration = endTime - startTime;

        System.out.println("Total Runtime: " + duration + " ms (" + (timeDuration / 1000.0) + " seconds)");

    }
}
