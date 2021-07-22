package com.pikozh.testtasks.customdatabasetask;

import com.pikozh.testtasks.customdatabasetask.configs.DatabaseProperties;
import com.pikozh.testtasks.customdatabasetask.model.ErrorMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author a.pikozh
 */
@Slf4j
public class DatabaseRestControllerIntegrationTests extends BaseIntegrationTest {

	@Autowired
	private RestTemplate restTemplate;

	@Autowired
	private DatabaseProperties properties;

	@BeforeAll
	public static void prepare(@Autowired DatabaseProperties properties) throws IOException {
		// Remove existing test database
		File dbDir = new File(properties.getLocation());
		if (dbDir.exists()) {
			log.info("Deleting old files in '{}'", dbDir.getAbsolutePath());
			Stream.of(dbDir.listFiles()).peek(f -> log.info("Delete file: {}", f.getAbsolutePath())).forEach(File::delete);
//			dbDir.delete();
		} else {
			boolean success = dbDir.mkdirs();
			log.info("Creating dir '{}': {}", dbDir.getAbsolutePath(), success);
		}

		// Fill test database with template
		URL url = DatabaseRestControllerIntegrationTests.class.getClassLoader().getResource("test_db_template");
		File dbTemplateDir;
		try {
			dbTemplateDir = new File(url.toURI());
		} catch (URISyntaxException | NullPointerException e) {
			dbTemplateDir = new File(url.getPath());
		}
		for (File tplFile : dbTemplateDir.listFiles()) {
			Path tplFilePath = tplFile.toPath();
			Path toPath = dbDir.toPath().resolve(tplFilePath.getFileName());
			log.info("Copy file '{}' to '{}'", tplFilePath.getFileName(), toPath);
			Files.copy(tplFilePath, toPath);
		}
	}

	private <T> T get(String tableName, int rowId, Class<T> clazz) {
		String url = String.format("http://localhost:%d/database/%s/%d", localserverPort, tableName, rowId);
		return restTemplate.getForObject(url, clazz);
	}

	@SuppressWarnings("unchecked")
	private <T> T getUnsafe(String tableName, int rowId, Class<?> clazz) {
		String url = String.format("http://localhost:%d/database/%s/%d", localserverPort, tableName, rowId);
		return (T) restTemplate.getForObject(url, clazz);
	}

	private void put(String tableName, int rowId, Object object) {
		String url = String.format("http://localhost:%d/database/%s/%d", localserverPort, tableName, rowId);
		restTemplate.put(url, object);
	}

	private Integer post(String tableName, Object object) {
		String url = String.format("http://localhost:%d/database/%s", localserverPort, tableName);
		return restTemplate.postForObject(url, object, Integer.class);
	}

	@Test
	public void testEmptyDatabase() {
		Assertions.assertThrows(HttpClientErrorException.NotFound.class, () -> {
			String error = get("empty", 0, String.class);
			log.info("result: {}", error);
		}, "Empty database is not empty");
	}

	@Test
	public void testSelect() throws Exception {
		final String tableName = "test";

		List<String> result = getUnsafe(tableName, 0, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Arrays.asList("1", "2", "3"), result, "Select result is not same for ID=0");

		result = getUnsafe(tableName, 1, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Arrays.asList("", "a", "b", "c", ""), result, "Select result is not same for ID=1");

		result = getUnsafe(tableName, 2, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Collections.emptyList(), result, "Select result is not same for ID=2");

		result = getUnsafe(tableName, 3, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Collections.singletonList(""), result, "Select result is not same for ID=3");

		result = getUnsafe(tableName, 4, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Arrays.asList("", "", "A"), result, "Select result is not same for ID=4");

		Assertions.assertThrows(HttpClientErrorException.NotFound.class, () -> {
			String error = get(tableName, 5, String.class);
			log.info("result: {}", error);
		}, "Select must return 404 for ID=5");
	}

	@Test
	public void testUpdate() throws Exception {
		final String tableName = "test-update";

		put(tableName, 0, Arrays.asList("3", "2", "1"));

		List<String> result = getUnsafe(tableName, 0, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Arrays.asList("3", "2", "1"), result, "Select result is not updated for ID=0");

		result = getUnsafe(tableName, 1, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Arrays.asList("", "a", "b", "c", ""), result, "Select result is not same for ID=1");

		result = getUnsafe(tableName, 2, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Collections.emptyList(), result, "Select result is not same for ID=2");

		result = getUnsafe(tableName, 3, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Collections.singletonList(""), result, "Select result is not same for ID=3");

		result = getUnsafe(tableName, 4, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(Arrays.asList("", "", "A"), result, "Select result is not same for ID=4");

		Assertions.assertThrows(HttpClientErrorException.NotFound.class, () -> {
			String error = get(tableName, 5, String.class);
			log.info("result: {}", error);
		}, "Select must return 404 for ID=5");
	}

	@Test
	public void testInsert() throws Exception {
		final String tableName = "test-insert";

		log.info("Test different data insertion");
		List<String> data = Arrays.asList("sadsadgdf", "erwegwg", "1", "2", "3", "", "\n\r \t var\" , \\", "");
		Integer id = post(tableName, data);
		Assertions.assertEquals(5, id, "ID is not same");

		List<String> result = getUnsafe(tableName, id, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(data, result, "Select result is not inserted for ID=" + id);

		log.info("Test empty list insertion");
		data = Collections.emptyList();
		id = post(tableName, data);
		Assertions.assertEquals(6, id, "ID is not same");

		result = getUnsafe(tableName, id, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(data, result, "Select result is not inserted for ID=" + id);

		log.info("Test single empty value insertion");
		data = Collections.singletonList("");
		id = post(tableName, data);
		Assertions.assertEquals(7, id, "ID is not same");

		result = getUnsafe(tableName, id, List.class);
		log.info("result: {}", result);
		Assertions.assertEquals(data, result, "Select result is not inserted for ID=" + id);

		Assertions.assertThrows(HttpClientErrorException.NotFound.class, () -> {
			String error = get(tableName, 8, String.class);
			log.info("result: {}", error);
		}, "Select must return 404 for ID=8");
	}

	@Test
	public void testBigTableUpdate() throws Exception {
		final String tableName = "bigtable";

		List<List<String>> dataLists = new ArrayList<>(20);
		for (int i = 1; i <= 20; i++) {
			List<String> data = IntStream.rangeClosed(1, i).boxed().map(String::valueOf).collect(Collectors.toList());
			Integer id = post(tableName, data);
			Assertions.assertEquals(i - 1, id, "ID is not same");
			dataLists.add(data);
		}

		List<String> newData = Arrays.asList("A", "B", "C", "D", "E", "F");
		put(tableName, 10, newData);
		dataLists.set(10, newData);

		for  (int id = 0; id < dataLists.size(); id++) {
			List<String> data = dataLists.get(id);
			List<String> result = getUnsafe(tableName, id, List.class);
			log.info("result[{}]: {}", id, result);
			Assertions.assertEquals(data, result, "Select result is not same for ID=" + id);
		}

		Assertions.assertThrows(HttpClientErrorException.NotFound.class, () -> {
			String error = get(tableName, 20, String.class);
			log.info("result: {}", error);
		}, "Select must return 404 for ID=20");
	}


}
