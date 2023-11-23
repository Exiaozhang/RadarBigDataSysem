package com.atguigu.springboot.springbootdemo.mapper.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.springboot.springbootdemo.bean.Customer;
import com.atguigu.springboot.springbootdemo.mapper.CustomerMapper;
import com.atujn.radar.app.DatabaseProduce;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.RequestParam;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据层组件
 */
@Repository  //标识成数据层组件(Spring)
public class CustomerMapperImpl implements CustomerMapper {

    /**
     * Sql客户端对象
     */
    static Connection connection;

    public CustomerMapperImpl() {
        connection = build();
    }


    /**
     * 创建Sql客户端对象
     */
    Connection build() {
        // 连接到端口号为3306的mysql数据库
        String url = "jdbc:mysql://hadoop102:3306/radar";
        String driver = "com.mysql.jdbc.Driver";
        String username = "root";
        String password = "000000";

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            System.out.println(e);
        }

        return connection;
    }

    /**
     * 批写
     */
    @Override
    public String bulkSave(String indexName,
                           Float data,
                           long ts,
                           String radartype,
                           String dt,
                           String hr) throws SQLException {

        try {
            String sql = "INSERT INTO `" + indexName + "` (`data`, `ts`,`radartype`,`dt`,`hr`) VALUES (?, ?,?,?,?)";
            PreparedStatement prep = connection.prepareStatement(sql);
            prep.setFloat(1, data);
            prep.setLong(2, ts);
            prep.setString(3, radartype);
            prep.setString(4, dt);
            prep.setString(5, hr);
            prep.executeUpdate();
        } catch (Exception e) {

            System.out.println(e);
            String sql = "CREATE TABLE `" + indexName + "`( `data` FLOAT, `ts` BIGINT, `radartype` TEXT, `dt` TEXT, `hr` TEXT )";
            PreparedStatement prep = connection.prepareStatement(sql);
            prep.executeUpdate();
            bulkSave(indexName, data, ts, radartype, dt, hr);
        }
        return "Success";
    }


    /**
     * 查询指定的字段
     */
    @Override
    public JSON searchFiled(String indexName, String fieldNames) throws SQLException {

        Statement statement = connection.createStatement();
        Object message = null;
        String sql = "SELECT " + fieldNames + " FROM `" + indexName + "`";
        ResultSet resultSet = statement.executeQuery(sql);
        String[] teststrs = fieldNames.split(",");
        List<JSON> radarMessage = new ArrayList<>();
        JSONObject messages = new JSONObject();
        while (resultSet.next()) {
            JSONObject json = new JSONObject();
            for (int i = 0; i < teststrs.length; i++) {
                json.put(teststrs[i], resultSet.getObject(teststrs[i]));
                //messages.put(teststrs[i],resultSet.getObject(teststrs[i]));
            }
            radarMessage.add(json);

        }

        messages.put("radarMessage", radarMessage);
        
        return (JSON) messages;
    }

    /**
     * 查询列表
     */
    @Override
    public JSON searchTable() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        ResultSet tables = metaData.getTables(null, null, "%", null);

        List<String> tableNames = new ArrayList<>();

        JSONObject messages = new JSONObject();

        while (tables.next()) {
            tableNames.add(tables.getString(3));
        }
        messages.put("tableNames", tableNames);

        return messages;
    }


    /**
     * 计算数据
     */
    @Override
    public void trainData(String indexName, Float data, Long ts, String radartype, String traintype) {

//        DatabaseProduce.producedata(indexName, data, ts, radartype, traintype);
    }


}

