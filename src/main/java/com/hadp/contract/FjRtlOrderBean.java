package com.hadp.contract;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FjRtlOrderBean implements WritableComparable<FjRtlOrderBean> {

    private int regionCode;
    private int countyCode;
    private int totalNum;
    private int successNum;
    private int failNum;
    private int ignoreNum;
    private int unDealNum;

    public int getRegionCode() {
        return regionCode;
    }

    public void setRegionCode(int regionCode) {
        this.regionCode = regionCode;
    }

    public int getCountyCode() {
        return countyCode;
    }

    public void setCountyCode(int countyCode) {
        this.countyCode = countyCode;
    }

    public int getTotalNum() {
        return totalNum;
    }

    public void setTotalNum(int totalNum) {
        this.totalNum = totalNum;
    }

    public int getSuccessNum() {
        return successNum;
    }

    public void setSuccessNum(int successNum) {
        this.successNum = successNum;
    }

    public int getFailNum() {
        return failNum;
    }

    public void setFailNum(int failNum) {
        this.failNum = failNum;
    }

    public int getIgnoreNum() {
        return ignoreNum;
    }

    public void setIgnoreNum(int ignoreNum) {
        this.ignoreNum = ignoreNum;
    }

    public int getUnDealNum() {
        return unDealNum;
    }

    public void setUnDealNum(int unDealNum) {
        this.unDealNum = unDealNum;
    }

    @Override
    public String toString() {
        return regionCode + "\t" + countyCode + "\t" + totalNum + "\t" + successNum + "\t"
                + failNum + "\t" + ignoreNum + "\t" + unDealNum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(regionCode);
        out.writeInt(countyCode);
        out.writeInt(totalNum);
        out.writeInt(successNum);
        out.writeInt(failNum);
        out.writeInt(ignoreNum);
        out.writeInt(unDealNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        regionCode = in.readInt();
        countyCode = in.readInt();
        totalNum = in.readInt();
        successNum = in.readInt();
        failNum = in.readInt();
        ignoreNum = in.readInt();
        unDealNum = in.readInt();
    }

    @Override
    public int compareTo(FjRtlOrderBean o) {
        if(regionCode - o.getRegionCode() == 0) {
            return countyCode - o.getCountyCode();
        }
        return regionCode - o.getRegionCode();
    }

}
