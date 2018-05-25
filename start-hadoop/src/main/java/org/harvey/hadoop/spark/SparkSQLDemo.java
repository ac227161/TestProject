package org.harvey.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.hive.HiveContext;

public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(SparkSQLDemo.class.getSimpleName());
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc);
        //Dataset table = sqlContext.table("default.kylin_sales");
        Dataset data = sqlContext.sql("select distinct visit_number,patient_age,discharge_date,dim_disease_rules.* from clb_hospital_dw.fact_mr_medical_record cross join clb_hospital_dw.dim_disease_rules limit 10");
        //data.write().saveAsTable("clb_hospital_dw.test3");  // table not existed
        data.show();


        sc.stop();

    }

}
