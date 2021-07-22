package com.pikozh.testtasks.customdatabasetask.services;

import com.pikozh.testtasks.customdatabasetask.configs.DatabaseProperties;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * @author a.pikozh
 */

@Slf4j
@AllArgsConstructor
@Service
public class FileDatabase implements Database {

	private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("[\\w_.-]+");

	private final DatabaseProperties properties;

	private final ConcurrentMap<String, TableFile> tables = new ConcurrentHashMap<>();

	// There must be a cleaning code that prevents memory overflow in case of huge number of tables by removing
	// TableFile entries that not used for a long time. Skipped nor now.

	private TableFile getTable(String tableName) {
		if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
			log.debug("Invalid table name: '{}'", tableName);
			throw new IllegalArgumentException("Invalid table name: " + tableName);
		}
		return tables.computeIfAbsent(tableName, t -> new TableFile(t, properties));
	}

	@PostConstruct
	private void construct() {
		log.info("Constructing FileDatabase with pros: {}", properties);
	}

	@Override
	public int insert(@NonNull String tableName, @NonNull List<String> values) {
		return getTable(tableName).insert(values);
	}

	@Override
	public void update(@NonNull String tableName, int rowId, @NonNull List<String> values) {
		getTable(tableName).update(rowId, values);
	}

	@Override
	public List<String> select(@NonNull String tableName, int rowId) {
		return getTable(tableName).select(rowId);
	}

}
