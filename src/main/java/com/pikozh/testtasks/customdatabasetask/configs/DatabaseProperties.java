package com.pikozh.testtasks.customdatabasetask.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author a.pikozh
 */
@Data
@ConfigurationProperties(prefix = "database")
public class DatabaseProperties {

	private String location;
	private int maxRowsInMemory;

}
