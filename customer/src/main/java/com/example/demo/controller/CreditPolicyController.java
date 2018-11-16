package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.service.CreditPolicyService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/Controller")
public class CreditPolicyController {
    @Autowired
    CreditPolicyService creditPolicyService;

    @RequestMapping(value = "/getResult", method = RequestMethod.POST)
    public Map<String,Object> getResult(@RequestBody String json){
        //参数集合
        Map<String,Object> params = JSON.parseObject(json,Map.class);
        //结果集合
        Map<String,Object> resultMap = new HashMap<String,Object>();

        try{
            if(params.get("customerNo") == null || params.get("productId") == null){
                resultMap.put("resultCode","100");
                resultMap.put("resultDesc","参数异常");
                return resultMap;
            }
            String customerNo = params.get("customerNo").toString();
            String productId = params.get("productId").toString();
            //反转客户号 customerNo
            String rowKey = CreditPolicyController.ReserverStr(customerNo,productId);
            List<Map<String,String>> mapList = creditPolicyService.getQueryByRowKey(rowKey);
            //成功
            if(mapList.size() > 0){
                resultMap.put("resultCode","000");
                resultMap.put("resultDesc","成功");
                resultMap.put("rules",mapList);

            }
        }catch (Exception e){
            resultMap.put("resultCode","500");
            resultMap.put("resultDesc","系统内部异常");
            e.printStackTrace();
            return resultMap;
        }

        return resultMap;
    }
    /**
     * 客户号反转+产品号
     */
    public static String ReserverStr(String customerNo, String produceId){
        StringBuilder stringBuilder = new StringBuilder(customerNo);
        String resCustomerNo = stringBuilder.reverse().toString();
        StringBuilder stringBuilder1 = new StringBuilder(resCustomerNo);
        String str = stringBuilder1.append("_"+produceId).toString();
        return str;
    }
}
