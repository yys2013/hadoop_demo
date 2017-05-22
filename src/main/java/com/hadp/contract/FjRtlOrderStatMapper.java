package com.hadp.contract;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FjRtlOrderStatMapper extends Mapper<LongWritable, Text, IntWritable, FjRtlOrderBean>{

    private FjRtlOrderBean fjRtlOrderBean = new FjRtlOrderBean();
    IntWritable iCode = new IntWritable(0);
    
    @Override
    protected void map(
            LongWritable key,
            Text value,
            Mapper<LongWritable, Text, IntWritable, FjRtlOrderBean>.Context context)
            throws IOException, InterruptedException {
        String[] fields = StringUtils.split(value.toString(), ",");
        if(fields != null && fields.length > 31) {
            iCode.set(Integer.parseInt(fields[26] + fields[30]));
            fjRtlOrderBean.setRegionCode(Integer.parseInt(fields[26]));
            fjRtlOrderBean.setCountyCode(Integer.parseInt(fields[30]));
            fjRtlOrderBean.setTotalNum(1);
            int iDealSts = Integer.parseInt(fields[22]);
            if(iDealSts == 1) {
                fjRtlOrderBean.setSuccessNum(1);
            }else if(iDealSts == 2 || iDealSts == 3){
                fjRtlOrderBean.setFailNum(1);
            }else if(iDealSts == 4){
                fjRtlOrderBean.setIgnoreNum(1);
            }else if(iDealSts == 0){
                fjRtlOrderBean.setUnDealNum(1);
            }
            context.write(iCode, fjRtlOrderBean);
        }
    }
    
}
