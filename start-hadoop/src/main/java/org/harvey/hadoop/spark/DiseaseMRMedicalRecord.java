package org.harvey.hadoop.spark;

import java.io.Serializable;

public class DiseaseMRMedicalRecord {
    private DiseaseKey diseaseKey;
    private DiseaseValue diseaseValue;

    public DiseaseKey getDiseaseKey() {
        return diseaseKey;
    }

    public void setDiseaseKey(DiseaseKey diseaseKey) {
        this.diseaseKey = diseaseKey;
    }

    public DiseaseValue getDiseaseValue() {
        return diseaseValue;
    }

    public void setDiseaseValue(DiseaseValue diseaseValue) {
        this.diseaseValue = diseaseValue;
    }

    public static class DiseaseKey implements Serializable {
        private String visitNumber;
        private int diseaseId;

        public DiseaseKey(String visitNumber, int diseaseId) {
            this.visitNumber = visitNumber;
            this.diseaseId = diseaseId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DiseaseKey key = (DiseaseKey) obj;
            if (this.visitNumber == null && key.visitNumber != null)
                return false;
            else if (!this.visitNumber.equalsIgnoreCase(key.visitNumber))
                return false;
            else if (this.diseaseId != key.diseaseId)
                return false;
            return true;
        }
    }

    public static class DiseaseValue implements Serializable {
        private int dischargeDate;

        public DiseaseValue(int dischargeDate) {
            this.dischargeDate = dischargeDate;
        }

        public int getDischargeDate() {
            return dischargeDate;
        }
    }

    /*
    private float patientAge;
    private String diseaseName;
    private String categoryCode;
    private String categoryName;
    private String secondCategoryCode;
    private String secondCategoryName;
    private int matchDiagColumnIndex; //statistics_code_0_dxcode_1_dxcode_level2_2_dxicd_3
    private int matchCm3ColumnIndex;  //cm3_0_operation_code_1_other_3
    private int isNeedIcd10Rules;
    private int isNeedCm3Rules;
    private int ageStart;
    private int ageEnd;
    private int losStart;
    private int losEnd;
    */
}
