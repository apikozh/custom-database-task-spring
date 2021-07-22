package com.pikozh.testtasks.customdatabasetask;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.RestTemplate;

/**
 * @author a.pikozh
 */

@SpringBootTest(
		webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
		classes = {CustomDatabaseTaskSpringApplication.class, BaseIntegrationTest.TestConfiguration.class})
@ActiveProfiles("integrationtest")
public class BaseIntegrationTest {

	@LocalServerPort
	int localserverPort;

	@Configuration
	public static class TestConfiguration {
		@Bean
		public RestTemplate restTemplate() {
			return new RestTemplate();
		}
	}

}
