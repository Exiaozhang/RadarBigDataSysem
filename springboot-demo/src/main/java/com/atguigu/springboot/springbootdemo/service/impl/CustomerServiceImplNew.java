package com.atguigu.springboot.springbootdemo.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.springboot.springbootdemo.mapper.CustomerMapper;
import com.atguigu.springboot.springbootdemo.service.CustomerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

@Service
public class CustomerServiceImplNew implements CustomerService {

    @Autowired
    CustomerMapper customerMapper;


    @Override
    public JSON findData(String indexName, String fieldName) throws SQLException {
        return customerMapper.searchFiled(indexName, fieldName);
    }

    @Override
    public JSON searchTable() throws SQLException {
        return customerMapper.searchTable();
    }

    ;

    @Override
    public String insertData(
            String indexName,
            Float data,
            Long ts,
            String radartype,
            String dt,
            String hr) throws SQLException {
        return customerMapper.bulkSave(indexName, data, ts, radartype, dt, hr);
    }

    @Override
    public String trainData(String indexName, Float data, Long ts, String radartype, String traintype) {
        customerMapper.trainData(indexName, data, ts, radartype, traintype);

        return "Train Message Send";
    }

    @Override
    public String TestReceiveData() {

        return "You Are Success";
    }
}
