package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Requête SQL sur un DataFrame chargé depuis un fichier JSON.
 */
public class Workshop5 {

    public static void main(String... args) {
        SparkConf conf = new SparkConf().setAppName("Spark Hadoop").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = Workshop5.class.getResource("/validations-sncf.csv").getPath();
        JavaRDD<String> rdd =  sc.textFile(path);

        //rdd.collect().forEach(System.out::println); //affiche tout

        //rdd.take(20).forEach(System.out::println); //affiche juste les 20 premières lignes

        AtomicInteger index = new AtomicInteger(1);

        rdd.filter(line -> !line.startsWith("JOUR"))
                .map(line -> line.split(";"))
                .map(fields -> (fields[4].toString()))//ne garde que les Arrets
               // .distinct()
        //.take(20).forEach(str -> System.out.println(index.getAndIncrement() + " : " + str)); // affiche les 20 premiers arrets
        .take(20).forEach(System.out::println); // affiche les 20 premiers arrets



    }
}
