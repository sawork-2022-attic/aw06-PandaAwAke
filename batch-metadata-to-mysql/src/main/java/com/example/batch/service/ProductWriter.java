package com.example.batch.service;

import com.example.batch.config.BatchConfig;
import com.example.batch.model.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemWriter;

import java.io.ByteArrayOutputStream;
import java.sql.*;
import java.util.List;

public class ProductWriter implements ItemWriter<Product>, StepExecutionListener {

    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/Amazon?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    static final String USER = "root";
    static final String PASS = "123456";

    private Connection connection;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @SneakyThrows
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        connection.close();
        return null;
    }

    @Override
    public void write(List<? extends Product> list) throws Exception {
//        list.stream().forEach(System.out::println);
//        System.out.println("chunk written");
        ObjectMapper objectMapper = new ObjectMapper();

        PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO `" + BatchConfig.DATABASE_TABLE_NAME + "`(main_cat, title, asin, category, imageURLHighRes) VALUES (?, ?, ?, ?, ?)"
        );

        for (Product product : list) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            objectMapper.writeValue(outputStream, product.getCategory());
            String categoryStr = outputStream.toString();
            outputStream.close();

            outputStream = new ByteArrayOutputStream();
            objectMapper.writeValue(outputStream, product.getImageURLHighRes());
            String imageURLHighResStr = outputStream.toString();
            outputStream.close();

            stmt.setString(1, product.getMain_cat());
            stmt.setString(2, product.getTitle());
            stmt.setString(3, product.getAsin());
            stmt.setString(4, categoryStr);
            stmt.setString(5, imageURLHighResStr);

            stmt.execute();
        }
        stmt.close();

    }
}
