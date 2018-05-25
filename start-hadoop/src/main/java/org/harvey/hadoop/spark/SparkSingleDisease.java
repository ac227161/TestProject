package org.harvey.hadoop.spark;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.harvey.hadoop.spark.DiseaseMRMedicalRecord.DiseaseValue;
import org.harvey.hadoop.util.HadoopUtil;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SparkSingleDisease implements Serializable {
    public class DiseasePairFunction implements PairFunction<Row, String, DiseaseValue> {
        @Override
        public Tuple2<String, DiseaseValue> call(Row row) {
            return new Tuple2<>(String.format("%s|%d", row.getString(0), row.getInt(1)), new DiseaseValue(row.getInt(2)));
        }
    }

    public void execute() throws IOException {
        /*
        String path = Thread.currentThread().getContextClassLoader().getResource("disease-sql.json").toString();
        path = path.replace("\\", "/");
        if (path.contains(":")) {
            path = path.replace("file:/", "");
        }
        String input = FileUtils.readFileToString(new File(path), "UTF-8");
        ObjectMapper objectMapper = new ObjectMapper();
        DiseaseSQL diseaseSQL = objectMapper.readValue(input, DiseaseSQL.class);
        */
        SparkSession spark = SparkSession.builder().appName(SparkSingleDisease.class.getSimpleName()).enableHiveSupport().getOrCreate();
        spark.sql("use clb_hospital_dw");
        //Dataset<Row> data = spark.sql(diseaseSQL.getCrossJoinSql());
        String sql = "select distinct visit_number,disease_id,discharge_date,patient_age,disease_name,category_code,category_name,second_category_code,second_category_name,match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3,match_cm3_column_index_cm3_0_operation_code_1_other_3,is_need_icd10_rules,is_need_cm3_rules,age_start,age_end,los_start,los_end,enabled from clb_hospital_dw.fact_mr_medical_record cross join clb_hospital_dw.dim_disease_rules where patient_age between age_start and age_end";
        Dataset<Row> data = spark.sql(sql);
        //data.write().saveAsTable("helper_interate_mr_visitnumber_cross_join_disease"); // table not existed: saveAsTable()
        spark.sql("truncate table helper_interate_mr_visitnumber_cross_join_disease");
        data.repartition(5).write().insertInto("helper_interate_mr_visitnumber_cross_join_disease");

        StorageLevel storageLevel = StorageLevel.MEMORY_ONLY();
        JavaPairRDD allDiseaseRDD = data.javaRDD().mapToPair(new DiseasePairFunction());

        /*
        JavaPairRDD allDiseaseRDD2 = spark.sql("select visit.visit_number,visit.disease_id,visit.discharge_date from helper_interate_mr_visitnumber_cross_join_disease as visit left join fact_mr_medical_record as mr on mr.visit_number=visit.visit_number where visit.disease_id=4 and (not (mr.patient_age>18))")
                .javaRDD().mapToPair(new DiseasePairFunction()); //.foreach((VoidFunction<Tuple2<String, DiseaseValue>>) t -> System.out.println(t._1 + ": " + t._2.getDischargeDate()));
        System.out.println(allDiseaseRDD.subtractByKey(allDiseaseRDD2).count());
        */

        // delete those visit number if not math the customized rules on high level of MR_Medical/MR_Diag/MR_Proc
        String customizedSqlTemplate = "select visit.visit_number,visit.disease_id,visit.discharge_date from helper_interate_mr_visitnumber_cross_join_disease as visit %s where visit.disease_id=%d and (%s)";
        //data = spark.sql(diseaseSQL.getCustomizedRuleSql());
        sql = "select rules_customized.disease_id,rules_customized.disease_name,concat_ws(' ',collect_set(customized_rules_need_join_tables)) as customized_rules_need_join_tables,concat_ws(' or ',collect_set(customized_rules_list)) as customized_rules_list from dim_disease_rules rules join ( select disease_id, disease_name, concat('left join ', case when rule_type_mr_medical_0_mr_diag_1_mr_sp_2=0 then 'fact_mr_medical_record as mr on mr.visit_number=visit.visit_number' when rule_type_mr_medical_0_mr_diag_1_mr_sp_2=1 then 'fact_mr_diag as mr_diag on mr_diag.visit_number=visit.visit_number' when rule_type_mr_medical_0_mr_diag_1_mr_sp_2=2 then 'fact_mr_procedure as mr_sp on mr_sp.visit_number=visit.visit_number' else '' end) as customized_rules_need_join_tables, concat('not (',case when rule_type_mr_medical_0_mr_diag_1_mr_sp_2=0 then 'mr.' when rule_type_mr_medical_0_mr_diag_1_mr_sp_2=1 then 'mr_diag.' when rule_type_mr_medical_0_mr_diag_1_mr_sp_2=2 then 'mr_sp.' else '' end,rule_expression,')') as customized_rules_list from dim_disease_rules_customized where disease_id<>-1) rules_customized on rules.disease_id=rules_customized.disease_id group by rules_customized.disease_id,rules_customized.disease_name";
        data = spark.sql(sql);
        JavaPairRDD customizedDiseaseRDD = data.javaRDD().mapToPair((PairFunction<Row, Integer, String>) x ->
                new Tuple2<>(x.getInt(0), String.format(customizedSqlTemplate, x.getString(2), x.getInt(0), x.getString(3))));
        List<String> customizedSql = customizedDiseaseRDD.values().collect();

        int size = customizedSql.size();
        JavaPairRDD[] tempRDD = new JavaPairRDD[size + 1];
        tempRDD[0] = allDiseaseRDD.partitionBy(new HashPartitioner(10)).persist(storageLevel);
        System.out.println(tempRDD[0].count());
        for (int i = 1; i <= size; i++) {
            System.out.println(i);
            System.out.println(customizedSql.get(i - 1));
            JavaPairRDD sqlRDD = spark.sql(customizedSql.get(i - 1)).javaRDD().mapToPair(new DiseasePairFunction());
            //sqlRDD.foreach((VoidFunction<Tuple2<String, DiseaseValue>>) t -> System.out.println(t._1 + ": " + t._2.getDischargeDate()));
            tempRDD[i] = tempRDD[i - 1].subtractByKey(sqlRDD).persist(storageLevel);
            tempRDD[i - 1].unpersist();
        }
        allDiseaseRDD = tempRDD[size];
        System.out.println(allDiseaseRDD.count());

        // 1. delete doesn't exists included icd10
        // 2. delete does exists exclude icd10
        // 3. delete doesn't exists included cm3
        // 4. delete exists excluded cm3
        Map<String, String> includeExcludeSql = Maps.newLinkedHashMap();
//        includeExcludeSql.put("included_icd10_sql", diseaseSQL.getIncludedIcd10Sql());
//        includeExcludeSql.put("excluded_icd10_sql", diseaseSQL.getExcludedIcd10Sql());
//        includeExcludeSql.put("included_cm3_sql", diseaseSQL.getIncludedCm3Sql());
//        includeExcludeSql.put("excluded_cm3_sql", diseaseSQL.getExcludedCm3Sql());

        includeExcludeSql.put("included_icd10_sql", "select visit.visit_number,visit.disease_id,visit.discharge_date from helper_interate_mr_visitnumber_cross_join_disease visit where is_need_icd10_rules=1 and not exists(    select 1 from fact_mr_diag as dia, dim_disease_rules_include_exclude_icd10 as icd10_rule     where dia.visit_number=visit.visit_number and visit.disease_id=icd10_rule.disease_id     and icd10_rule.exclude_0_include_1=1     and coalesce(icd10_rule.match_dxcategory,'')=trim(coalesce(dia.dx_category,''))     and coalesce(icd10_rule.match_dxsequenceno,'')=trim(coalesce(dia.sequence_no,''))     and ((icd10_rule.match_code_0_or_name_1=0 and         case when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=0 then dia.statistics_code             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=1 then dia.dx_code             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=2 then dia.dxcode_level2             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=3 then dia.dx_icd         else dia.statistics_code end         like icd10_rule.match_value)     or (icd10_rule.match_code_0_or_name_1=1 and         case when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=0 then dia.statistics_name             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=1 then dia.dx_description             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=2 then dia.dxname_level2             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=3 then dia.dx_icd         else dia.statistics_name end         like icd10_rule.match_value))     ) and exists(select 1 from dim_disease_rules_include_exclude_icd10 as icd10_rule where visit.disease_id=icd10_rule.disease_id and icd10_rule.exclude_0_include_1=1)");
        includeExcludeSql.put("excluded_icd10_sql", "select visit.visit_number,visit.disease_id,visit.discharge_date from helper_interate_mr_visitnumber_cross_join_disease visit  where is_need_icd10_rules=1  and exists(     select 1 from fact_mr_diag as dia,dim_disease_rules_include_exclude_icd10 as icd10_rule     where dia.visit_number=visit.visit_number and visit.disease_id=icd10_rule.disease_id     and icd10_rule.exclude_0_include_1=0     and coalesce(icd10_rule.match_dxcategory,'')=trim(coalesce(dia.dx_category,''))     and coalesce(icd10_rule.match_dxsequenceno,'')=trim(coalesce(dia.sequence_no,''))     and ((icd10_rule.match_code_0_or_name_1=0 and         case when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=0 then dia.statistics_code             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=1 then dia.dx_code             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=2 then dia.dxcode_level2             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=3 then dia.dx_icd         else dia.statistics_code end         like icd10_rule.match_value)     or (icd10_rule.match_code_0_or_name_1=1 and         case when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=0 then dia.statistics_name             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=1 then dia.dx_description             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=2 then dia.dxname_level2             when visit.match_diag_column_index_statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3=3 then dia.dx_icd         else dia.statistics_name end         like icd10_rule.match_value))     ) and exists(select 1 from dim_disease_rules_include_exclude_icd10 as icd10_rule where visit.disease_id=icd10_rule.disease_id and icd10_rule.exclude_0_include_1=0)");
        includeExcludeSql.put("included_cm3_sql", "select visit.visit_number,visit.disease_id,visit.discharge_date from helper_interate_mr_visitnumber_cross_join_disease visit  where is_need_cm3_rules=1  and not exists(     select 1 from fact_mr_procedure as sp,dim_disease_rules_include_exclude_cm3 as cm3_rule     where sp.visit_number=visit.visit_number     and visit.disease_id=cm3_rule.disease_id     and cm3_rule.exclude_0_include_1=1     and coalesce(cm3_rule.match_sp_sequenceno,'')=trim(coalesce(sp.sequence_no,''))     and ((cm3_rule.match_code_0_or_name_1=0 and         case when visit.match_cm3_column_index_cm3_0_operation_code_1_other_3=0 then sp.procedure_icd9         when visit.match_cm3_column_index_cm3_0_operation_code_1_other_3=1 then sp.procedure_code         else sp.procedure_code end like cm3_rule.match_value)         or (cm3_rule.match_code_0_or_name_1=1 and sp.procedure_name like cm3_rule.match_value)     ) ) and exists(select 1 from dim_disease_rules_include_exclude_cm3 as cm3_rule where visit.disease_id=cm3_rule.disease_id and cm3_rule.exclude_0_include_1=1)");
        includeExcludeSql.put("excluded_cm3_sql", "select visit.visit_number,visit.disease_id,visit.discharge_date from helper_interate_mr_visitnumber_cross_join_disease visit  where is_need_cm3_rules=1 and  exists(     select 1 from fact_mr_procedure as sp,dim_disease_rules_include_exclude_cm3 as cm3_rule     where sp.visit_number=visit.visit_number and visit.disease_id=cm3_rule.disease_id     and cm3_rule.exclude_0_include_1=0     and coalesce(cm3_rule.match_sp_sequenceno,'')=trim(coalesce(sp.sequence_no,''))     and ((cm3_rule.match_code_0_or_name_1=0 and         case when visit.match_cm3_column_index_cm3_0_operation_code_1_other_3=0 then sp.procedure_icd9         when visit.match_cm3_column_index_cm3_0_operation_code_1_other_3=1 then sp.procedure_code         else sp.procedure_code end like cm3_rule.match_value)         or (cm3_rule.match_code_0_or_name_1=1 and sp.procedure_name like cm3_rule.match_value)     ) ) and exists(select 1 from dim_disease_rules_include_exclude_cm3 as cm3_rule where visit.disease_id=cm3_rule.disease_id and cm3_rule.exclude_0_include_1=0)");
        size = includeExcludeSql.size();
        JavaPairRDD[] tempRDD2 = new JavaPairRDD[size + 1];
        tempRDD2[0] = allDiseaseRDD;
        int i = 1;
        for (Map.Entry<String, String> entry : includeExcludeSql.entrySet()) {
            System.out.println(entry.getKey());
            tempRDD2[i] = tempRDD2[i - 1].subtractByKey(spark.sql(entry.getValue()).javaRDD().mapToPair(new DiseasePairFunction())).persist(storageLevel);
            tempRDD2[i - 1].unpersist();
            i++;
        }
        System.out.println(tempRDD2[size].count());

        String output = "hdfs://master:8020/test/fact_mr_disease";
        HadoopUtil.deletePath(HadoopUtil.getLocalConfiguration(), new Path(output));
        tempRDD2[size].map((Function<Tuple2<String, DiseaseValue>, String>) t -> {
            String key[] = t._1.split("\\|");
            return key[0] + "\t" + key[1] + "\t" + t._2.getDischargeDate();
        }).saveAsTextFile(output);

        sql = "load data inpath '%s/*' overwrite into table fact_mr_disease_spark";
        System.out.println(sql);
        spark.sql(String.format(sql, output));

        tempRDD2[size].unpersist();
        spark.close();
    }

    public static void main(String[] args) throws Exception {
        SparkSingleDisease ssd = new SparkSingleDisease();
        ssd.execute();
    }
}
