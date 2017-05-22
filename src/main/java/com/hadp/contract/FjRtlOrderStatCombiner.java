package com.hadp.contract;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FjRtlOrderStatCombiner extends Reducer<IntWritable, FjRtlOrderBean, IntWritable, FjRtlOrderBean>{

    private FjRtlOrderBean fjRtlOrderBean = new FjRtlOrderBean();
    
    @Override
    protected void reduce(IntWritable key, Iterable<FjRtlOrderBean> values, Context context)
            throws IOException, InterruptedException {
        for (FjRtlOrderBean val : values) {
            fjRtlOrderBean.setRegionCode(val.getRegionCode());
            fjRtlOrderBean.setCountyCode(val.getCountyCode());
            fjRtlOrderBean.setTotalNum(fjRtlOrderBean.getTotalNum() + val.getTotalNum());
            fjRtlOrderBean.setSuccessNum(fjRtlOrderBean.getSuccessNum() + val.getSuccessNum());
            fjRtlOrderBean.setFailNum(fjRtlOrderBean.getFailNum() + val.getFailNum());
            fjRtlOrderBean.setIgnoreNum(fjRtlOrderBean.getIgnoreNum() + val.getIgnoreNum());
            fjRtlOrderBean.setUnDealNum(fjRtlOrderBean.getUnDealNum() + val.getUnDealNum());
        }
        context.write(key, fjRtlOrderBean);
    }
    
}
