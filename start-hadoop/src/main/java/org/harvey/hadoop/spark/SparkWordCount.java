package org.harvey.hadoop.spark;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.harvey.hadoop.util.HadoopUtil;
import scala.Tuple2;

import java.util.Arrays;

public class SparkWordCount {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("SparkWordCount");
        //conf.setMaster("spark://master:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //String path = "D:\\Work\\GIT\\clb_big_data\\readme.md";
        JavaRDD<String> input = sc.textFile(args[0]);
        JavaRDD<String> words = input.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(",")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair((PairFunction<String, String, Integer>) x -> new Tuple2<>(x, 1)).
                reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);
        HadoopUtil.deletePath(HadoopUtil.getLocalConfiguration(), new Path(args[1]));
        counts.saveAsTextFile(args[1]);
        counts.foreach((VoidFunction<Tuple2<String, Integer>>) t -> System.out.println(t._1 + " appeared " + t._2 + "times"));

        /* * 使用sqlContext读取hdfs文件
         * json文件*/

//        DataFrame df = sqlContext.read().json("hdfs://master:8020/test/province.json");
//        // DataFrame df = sqlContext.read().format("json").load("hdfs://master:8020/test/province.json");
//        df.show();
//        df.registerTempTable("tb_province");

        sc.stop();
    }
}
