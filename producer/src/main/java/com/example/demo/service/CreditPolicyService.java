package com.example.demo.service;

import java.util.List;
import java.util.Map;

public interface CreditPolicyService {
    List<Map<String,String>> getQueryByRowKey(String rowkey);
}
