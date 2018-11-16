package com.example.demo.service.impl;



import com.example.demo.service.CreditPolicyService;
import com.example.demo.util.HbaseUtil;



import java.util.List;
import java.util.Map;


public class CreditPolicyServiceImpl implements CreditPolicyService {
    /**
     * 通过rowkey查询记录
     * @param rowkey
     * @return
     */
    @Override
    public List<Map<String,String>> getQueryByRowKey(String rowkey) {
        List<Map<String,String>> mapList = HbaseUtil.getList(rowkey);
        return mapList;
    }
}
