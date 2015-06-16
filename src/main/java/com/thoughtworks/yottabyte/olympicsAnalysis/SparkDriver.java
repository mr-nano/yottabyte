package com.thoughtworks.yottabyte.olympicsAnalysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkDriver {
    public static void main(String[] args) {
        String master = args[0];
        String appName = args[1];
        String path = args[2];
        String destination = args[3];

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile(path);


        JavaRDD<Record> records = lines.map(new Function<String, Record>() {
            @Override
            public Record call(String v1) throws Exception {
                return Record.parse(v1);
            }
        });

        //    Which country scored the most medals
        JavaPairRDD<Integer, String> countryMedals = records.mapToPair(new PairFunction<Record, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Record record) throws Exception {
                return new Tuple2<>(record.name, record.total);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2(), tuple._1());
            }
        }).sortByKey();
        System.out.println(countryMedals.first());

//    year in which a country scored most medals
        Tuple2<Integer, Integer> integerIntegerTuple2 = yearWithMostMedals(records, "United States", destination);
        Integer bestYear = integerIntegerTuple2._2();
        Integer bestMedals = integerIntegerTuple2._1();
        System.out.println("bestYear " + bestYear + " bestMedals " + bestMedals);

//    Which country scored the most medals
        countryWithMostMedals(records);

//    Which player scored most medals

        records.mapToPair(new PairFunction<Record, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Record record) throws Exception {
                return new Tuple2<String, Integer>(record.country, record.total);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
    }

    public static void countryWithMostMedals(JavaRDD<Record> records) {
        records.mapToPair(new PairFunction<Record, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Record record) throws Exception {
                return new Tuple2<String, Integer>(record.country, record.total);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
                return new Tuple2<Integer, String>(v1._2(), v1._1());
            }
        }).sortByKey().first();
    }

    public static Tuple2<Integer, Integer> yearWithMostMedals(JavaRDD<Record> records, final String countryName, String destination) {

        JavaRDD<Record> countryMedals = records.filter(new Function<Record, Boolean>() {
            @Override
            public Boolean call(Record v1) throws Exception {
                return v1.country.equals(countryName);
            }
        });

        JavaPairRDD<Integer, Integer> yearMedals = countryMedals.mapToPair(new PairFunction<Record, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Record record) throws Exception {
                return new Tuple2<Integer, Integer>(record.year, record.total);
            }
        });

        JavaPairRDD<Integer, Integer> medalsPerYear = yearMedals.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        Tuple2<Integer, Integer> yearWithMostMedals = medalsPerYear.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1) throws Exception {
                return new Tuple2<Integer, Integer>(v1._2(), v1._1());
            }
        }).sortByKey(false)
                .first();


        final int minMedals = 200;

//        medalsPerYear.cache()

        medalsPerYear.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._2() < minMedals;
            }
        }).saveAsTextFile(destination);

        return yearWithMostMedals;
    }
}
