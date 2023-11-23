package com.atguigu.springboot.springbootdemo.mapper;

import com.alibaba.fastjson.JSON;
import com.atguigu.springboot.springbootdemo.bean.Customer;

import java.sql.SQLException;
import java.util.List;

/**
 * 数据层接口
 * <p>
 * 目前，数据层一般都是基于MyBatis实现的。MyBatis的玩法是只写接口+SQL即可，不需要自己写实现类.
 */
public interface CustomerMapper {


    String bulkSave(String indexName,
                    Float data,
                    long ts,
                    String radartype,
                    String dt,
                    String hr) throws SQLException;

   JSON searchFiled(String indexName, String fieldName) throws SQLException;

   JSON searchTable() throws SQLException;

   void trainData(String indexName,Float data,Long ts,String radartype,String traintype);

}
