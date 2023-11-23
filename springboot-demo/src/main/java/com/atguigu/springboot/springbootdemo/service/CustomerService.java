package com.atguigu.springboot.springbootdemo.service;

import com.alibaba.fastjson.JSON;

import java.sql.SQLException;

/**
 * 业务层接口
 */
public interface CustomerService {
    JSON findData(String indexName, String fieldName) throws SQLException;

    JSON searchTable() throws SQLException;

    String insertData(
            String indexName,
            Float data,
            Long ts,
            String radartype,
            String dt,
            String hr) throws SQLException;

    String trainData(String indexName,Float data,Long ts,String radartype,String traintype);

    String TestReceiveData();

}
