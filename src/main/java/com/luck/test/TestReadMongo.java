package com.luck.test;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Joiner;
import com.luck.util.PropertiesUtil;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import org.bson.types.Binary;
import scala.Tuple2;

import java.io.IOException;
import java.sql.*;
import java.util.*;


/**
 * @Author miracle
 * @Date 2017/11/17 0017 14:40
 */
public class TestReadMongo {


    public static void main(String args[]) throws ClassNotFoundException {
        SparkConf sc = new SparkConf()
                .setMaster("local")
                .setAppName("MongoSparkConnectorTour") //应用名称
                .set("spark.mongodb.input.uri", "mongodb://10.30.50.153:27020/qingqidatadbol.bi")//mongodb input 连接
                .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "bi" + "_" + "201711");

        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        String date = "2017-11-12";
        Document filter = Document.parse("{ $match: { date :'" + date + "'} }");
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, readConfig).withPipeline(Collections.singletonList(filter));

        System.out.println("----------------------------------------" + rdd.count());
        List<Document> list = rdd.collect();
        for (Document document: list){
            System.out.println(document.toJson());
        }
        jsc.close();
    }
}
