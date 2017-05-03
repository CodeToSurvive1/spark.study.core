package io.github.codetosurvive.spark.study.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 本地测试的Wordcount程序
 * 
 * @author lixiaojiao
 *
 */
public class WordCountLocal {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("WordCountLocal").setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> lines = sc.textFile("CONTRIBUTING.md");

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> reduceByKey = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		reduceByKey.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> result) throws Exception {
				System.out.println(result._1+" appears " +result._2 +" times");
			}
		});
		
		
		sc.close();
		
	}
}
