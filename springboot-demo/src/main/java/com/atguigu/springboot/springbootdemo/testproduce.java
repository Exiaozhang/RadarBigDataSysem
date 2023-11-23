package com.atguigu.springboot.springbootdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atujn.radar.app.DatabaseProduce;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class testproduce {


    /**
     * Sql客户端对象
     */
    static Connection connection;


    /**
     * 创建Sql客户端对象
     */
    static Connection build() {
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

    public static void main(String[] args) throws SQLException {

        String str = "MyWorld";
        String str2 = "Hello {str}";
        System.out.println(str2);
    }

    public static String searchFiled(String indexName, String fieldNames) throws SQLException {

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
            }
            radarMessage.add(json);
        }

        messages.put("radarMessage", radarMessage);

        System.out.println(messages);
        return messages.toString();
    }


    public static void searchTable() throws SQLException {

        DatabaseMetaData metaData = connection.getMetaData();

        ResultSet tables = metaData.getTables(null, null, "%", null);

        while (tables.next()) {
            System.out.println(tables.getString(3));
        }
    }

}
