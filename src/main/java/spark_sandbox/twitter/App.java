package spark_sandbox.twitter;

import java.io.File;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;




/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args )
    {
    	System.getProperties().put("hadoop.home.dir", new File(".").getAbsolutePath());

    	SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
    	JavaSparkContext sc = new JavaSparkContext(conf);

//    	JavaRDD<String> input = sc.textFile("C:/apps/spark-1.5.2/README.md");
//    	JavaRDD<String> input = sc.textFile("C:/workspace/sandbox/spark-java/twitter/target/classes/spark_sandbox/twitter/README.txt");
    	JavaRDD<String> input = sc.textFile("README.txt");
    	JavaRDD<String> words = input.flatMap(
    			new FlatMapFunction<String, String>() {
    				public Iterable<String> call(String x) {
    					return Arrays.asList(x.split(" "));
    				}
    			}
    	);

    	JavaPairRDD<String, Integer> counts = words.mapToPair(
    			new PairFunction<String, String, Integer>() {
    				public Tuple2<String, Integer> call(String x){
    					return new Tuple2(x, 1);
    				}
    			}
    	).reduceByKey(new Function2<Integer, Integer, Integer>() {
    		public Integer call(Integer x, Integer y) {return x + y; }
    	});

    	counts.saveAsTextFile("C:/workspace/sandbox/spark-java/twitter/target/README.md.count");

    }
}
