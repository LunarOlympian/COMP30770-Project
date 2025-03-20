package com.comp30770.cleaning;

import com.opencsv.*;
import com.opencsv.exceptions.CsvException;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class CleaningDataset {

    public static void main(String[] args) throws IOException, CsvException, URISyntaxException {
        // Loads the csv file
        CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
        CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(Path.of(ClassLoader.getSystemResource("csv/games_march2025_cleaned.csv").toURI())))
                .withCSVParser(parser).withSkipLines(0).build();

        ArrayList<String[]> validGames = new ArrayList<>();
        ArrayList<String[]> games = new ArrayList<>(reader.readAll());
        String[] header = games.get(0);
        games.remove(0);


        int validCount = 0;
        for(String[] game : games) {
            try {
                Integer.parseInt(game[33]);
                Integer.parseInt(game[34]);
            }
            catch (Exception e) {continue;}

            if(Integer.parseInt(game[33]) + Integer.parseInt(game[34]) > 10) {
                String[] trimmedGame = new String[43];
                int pos = 0;
                for(int i = 0; i < 47; i++) {
                    if((i >=6 && i <= 13) || header[i].equalsIgnoreCase("screenshots") || header[i].equalsIgnoreCase("movies")) continue;
                    trimmedGame[pos] = game[i];
                    pos++;
                }
                validGames.add(trimmedGame);
            }
        }

        CSVWriter writer = new CSVWriter(new FileWriter("C:\\Users\\sebas\\Documents\\Web Dev\\Project\\22206116\\COMP30770-Project\\src\\main\\resources\\csv\\SteamGamesDataset.csv"));
        writer.writeAll(validGames);
        writer.close();
        System.out.println(validCount);
    }

    private static boolean isNumeric(String num) {
        try {
            Double.parseDouble(num);
        }
        catch (Exception exception) {
            return false;
        }
        return true;
    }
}
