package org.devoxx.spark.lab.devoxx2015;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Requête SQL sur un DataFrame chargé depuis un fichier JSON.
 */
public class Workshop4 {

    public static void main(String... args) {
        SparkConf conf = new SparkConf().setAppName("les-arbres-paris").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = Workshop4.class.getResource("/les-arbres.csv").getPath();
        JavaRDD<String> rdd =  sc.textFile(path);
        long count = rdd
                .filter(line -> !line.startsWith("IDBASE"))
                .map(line -> line.split(";"))
                .map(fields -> Float.parseFloat(fields[13]))//Hauteur des arbres (colonne 14 )
                .filter(height -> height > 0)
                .count();
        System.out.println("nombre d'arbres avec hauteur > 0 : " + count);
    }
}
