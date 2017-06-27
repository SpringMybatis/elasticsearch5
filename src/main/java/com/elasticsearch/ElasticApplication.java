package com.elasticsearch;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by Administrator on 2017/6/27.
 */
@SpringBootApplication
public class ElasticApplication {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ElasticApplication.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
    }
}
