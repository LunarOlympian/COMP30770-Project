package com.comp30770;

import org.javatuples.Pair;

public class Country {

    public final String country;
    public final double averageDuration;
    public final double energy;
    public final double loudness;
    public final double spechiness;
    public final double tempo;

    public Country(String country, double averageDurations, double energy, double loudness, double spechiness, double tempo) {
        this.country = country;
        this.averageDuration = averageDurations;
        this.energy = energy;
        this.loudness = loudness;
        this.spechiness = spechiness;
        this.tempo = tempo;
    }

    public String toString() {
        return "Country: " + country + ". duration: " + averageDuration + ". Energy: " +
                energy + ". Loudness: " + loudness + ". Speechiness: " + spechiness + ". Tempo: " + tempo;
    }
}
