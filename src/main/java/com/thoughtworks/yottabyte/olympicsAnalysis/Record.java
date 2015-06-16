package com.thoughtworks.yottabyte.olympicsAnalysis;

public class Record {
    String name;
    String country;
    int year;
    String category;
    int gold;
    int silver;
    int bronze;
    int total;

    public Record(String name, String country, int year, String category, int gold, int silver, int bronze, int total) {
        this.name = name;
        this.country = country;
        this.year = year;
        this.category = category;
        this.gold = gold;
        this.silver = silver;
        this.bronze = bronze;
        this.total = total;
    }

    public static Record parse(String input) {
        String[] split = input.split(",");
        return new Record(split[0],
                split[1],
                Integer.parseInt(split[2]),
                split[3],
                Integer.parseInt(split[4]),
                Integer.parseInt(split[5]),
                Integer.parseInt(split[6]),
                Integer.parseInt(split[7]));

    }
}
