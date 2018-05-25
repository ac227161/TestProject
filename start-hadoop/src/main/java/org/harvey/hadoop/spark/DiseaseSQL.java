package org.harvey.hadoop.spark;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DiseaseSQL {
    @JsonProperty("cross_join_sql")
    private String crossJoinSql;

    @JsonProperty("customized_rule_sql")
    private String customizedRuleSql;

    @JsonProperty("included_icd10_sql")
    private String includedIcd10Sql;

    @JsonProperty("excluded_icd10_sql")
    private String excludedIcd10Sql;

    @JsonProperty("included_cm3_sql")
    private String includedCm3Sql;

    @JsonProperty("excluded_cm3_sql")
    private String excludedCm3Sql;

    public String getCrossJoinSql() {
        return crossJoinSql;
    }

    public String getCustomizedRuleSql() {
        return customizedRuleSql;
    }

    public String getIncludedIcd10Sql() {
        return includedIcd10Sql;
    }

    public String getExcludedIcd10Sql() {
        return excludedIcd10Sql;
    }

    public String getIncludedCm3Sql() {
        return includedCm3Sql;
    }

    public String getExcludedCm3Sql() {
        return excludedCm3Sql;
    }
}
