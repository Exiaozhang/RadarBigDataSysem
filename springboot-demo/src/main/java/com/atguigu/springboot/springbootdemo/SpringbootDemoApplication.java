package com.atguigu.springboot.springbootdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.text.MessageFormat;

@SpringBootApplication
public class SpringbootDemoApplication {

    public static void main(String[] args) {
        String indexName ="indexName";
        String fieldName = "fieldName";
        String filedValue = "fieldValue";

        String sql = MessageFormat.format("INSERT INTO {0} ({1}) VALUES ({2})",indexName,fieldName,filedValue);

        System.out.println(sql);

        SpringApplication.run(SpringbootDemoApplication.class, args);
    }
}

