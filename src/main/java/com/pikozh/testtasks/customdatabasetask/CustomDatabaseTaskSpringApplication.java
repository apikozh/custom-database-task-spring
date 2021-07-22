package com.pikozh.testtasks.customdatabasetask;

import com.pikozh.testtasks.customdatabasetask.configs.DatabaseProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(DatabaseProperties.class)
public class CustomDatabaseTaskSpringApplication {

	public static void main(String[] args) {
		SpringApplication.run(CustomDatabaseTaskSpringApplication.class, args);
	}

}
