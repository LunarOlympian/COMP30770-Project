package com.comp30770.cleaning;

import com.opencsv.*;
import com.opencsv.exceptions.CsvException;

import java.io.File;
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
        FileWriter[] writers = new FileWriter[]{
                new FileWriter("C:\\Users\\sebas\\Documents\\Web Dev\\Project\\22206116\\COMP30770-Project\\src\\main\\resources\\csv\\Top_spotify_songs1.csv"),
                new FileWriter("C:\\Users\\sebas\\Documents\\Web Dev\\Project\\22206116\\COMP30770-Project\\src\\main\\resources\\csv\\Top_spotify_songs2.csv"),
                new FileWriter("C:\\Users\\sebas\\Documents\\Web Dev\\Project\\22206116\\COMP30770-Project\\src\\main\\resources\\csv\\Top_spotify_songs3.csv"),
                new FileWriter("C:\\Users\\sebas\\Documents\\Web Dev\\Project\\22206116\\COMP30770-Project\\src\\main\\resources\\csv\\Top_spotify_songs4.csv")
        };

        String datasets = Files.readString(new File("C:\\Users\\sebas\\Documents\\Web Dev\\Project\\22206116\\COMP30770-Project\\src\\main\\resources\\csv\\Top_spotify_songs.csv").toPath()) + "\n" +
                Files.readString(new File("C:\\Users\\sebas\\Documents\\Web Dev\\Project\\22206116\\COMP30770-Project\\src\\main\\resources\\csv\\Top_spotify_songs_2.csv").toPath());

        String[] dataset = datasets.split("\n");
        datasets = null;
        System.gc(); // I don't want my laptop to die :(

        int startPos = 0;
        int incrementAmount = dataset.length / 4;

        for(FileWriter writer : writers) {
            StringBuilder writingContent = new StringBuilder();
            for(int i = startPos; i < startPos + incrementAmount; i++) {
                writingContent.append("\n").append(dataset[i]);
            }
            startPos += incrementAmount;

            writer.write(
                    writingContent.toString().trim()
            );
            writer.close();
        }
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
