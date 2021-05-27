package com.epam.data_frames;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;


public class Main {

    public static final String LASTNAME = "lastname";
    public static final String AGE = "age";
    public static final String KEYWORDS = "keywords";
    public static final String KEYWORD = "keyword";

    private static final String SALARY = "salary";
    private static final String NAME = "name";

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf().setAppName("linkedIn").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext spark = new SQLContext(sc);

        DataFrame df = spark.read().json("data/linkedIn/*");

        //print out file content
        df.show();
        //print out the schema
        df.printSchema();

        //print out the column type
        Tuple2<String, String>[] types = df.dtypes();
        Arrays.stream(types).collect(Collectors.toList()).forEach(e -> System.out.println(e._1 + "," + e._2) );


        //for()


        //add column salary the value will be calculated by formula: age *numberOf technologies * 10
        DataFrame DataFrameWithSalary = df.withColumn(SALARY, col(AGE).multiply(size(col(KEYWORDS))).multiply(10));
        DataFrameWithSalary.show();

        //find developers with salary < 1200 and which familiar with most popular technology
        DataFrame keyWordsExploded = DataFrameWithSalary.withColumn(KEYWORD, explode(col(KEYWORDS))).persist();

        DataFrame languageAndFrequency = keyWordsExploded
                .drop(SALARY).drop(KEYWORDS).drop(NAME).drop(AGE)
                .groupBy(KEYWORD).count();

        languageAndFrequency.show();
        languageAndFrequency.persist();


        //sol 1:
        long maxFrequencyValue = languageAndFrequency.agg(max("count")).collectAsList().get(0).getLong(0);
        DataFrame mostFrequentTechnology = languageAndFrequency.filter(col("count").equalTo(maxFrequencyValue));
        mostFrequentTechnology.show();
        String mostTechnologyFrequent = mostFrequentTechnology.collectAsList().get(0).getString(0);

        keyWordsExploded.printSchema();

        keyWordsExploded = keyWordsExploded
                .filter(col(SALARY).$less(1200))
                .filter(col("keyword").equalTo(mostTechnologyFrequent));


        keyWordsExploded.show();

        //sol2:
        Row row = df.withColumn(KEYWORD, explode(col(KEYWORDS))).select(KEYWORD)
                .groupBy(col(KEYWORD)).agg(count(KEYWORD).as("amount")).sort(col("amount").desc()).first();

        String mostPopular = row.getAs(KEYWORD);
        long amount = row.getAs("amount");

        System.out.println(mostPopular);

        DataFrameWithSalary.filter(col(SALARY).leq(1200)
                .and(array_contains(col(KEYWORDS),mostPopular)))
                .show();

        DataFrameWithSalary.registerTempTable("employees");

        String sqlText = "select * from employees where salary < 1200";
        DataFrame dataFrame1 = spark.sql(sqlText);


        //class exercises
        df = df.withColumn(LASTNAME, upper(col("name")));
        df.withColumn("age to live remaining", col(AGE).cast(DataTypes.IntegerType).minus(120).multiply(-1)).show();

        df.show();

        df.withColumn("keyword",explode(col("keywords"))).drop("name").drop("age").show();


        df.registerTempTable("linkedin");

        spark.sql("select * from linkedin where age > 40").show();
        df.where(col("age").$greater(40)).show();



    }
}
