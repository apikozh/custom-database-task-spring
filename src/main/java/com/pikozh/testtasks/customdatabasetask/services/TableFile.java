package com.pikozh.testtasks.customdatabasetask.services;

import com.pikozh.testtasks.customdatabasetask.configs.DatabaseProperties;
import com.pikozh.testtasks.customdatabasetask.model.exceptions.DatabaseException;
import com.pikozh.testtasks.customdatabasetask.model.exceptions.NotFoundException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author a.pikozh
 */
@Slf4j
class TableFile {

	/**
	 * Helper lock class which can be used in try-with-resources block instead of classic
	 * try-finally block. As for me it is safer in terms of human mistake, but creates additional wrapper object.
	 */
	static class LockWrapper implements AutoCloseable {
		final Lock lock;

		private LockWrapper(Lock lock) {
			this.lock = lock;
			lock.lock();
		}

		@Override
		public void close() {
			lock.unlock();
		}
	}

	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final String tableName;
	private final Path filePath;
	private final DatabaseProperties properties;
	private int nextRowId; // count

	TableFile(@NonNull final String tableName,
			  @NonNull final DatabaseProperties properties) {
		log.info("Creating new TableFile instance for '{}' (props:{})", tableName, properties);
		this.tableName = tableName;
		this.properties = properties;
		String location = properties.getLocation();
		String fileName = location.endsWith("/") ? location + tableName : location + "/" + tableName;
		File file = new File(fileName);
		this.filePath = file.toPath();

		try {
			if (!file.exists()) {
				log.info("File '{}' not exists, creating new", file.getAbsolutePath());
				file.getParentFile().mkdirs();
				file.createNewFile();
				nextRowId = 0;
			} else {
				log.info("File '{}' exists, checking contents", file.getAbsolutePath());
				// Here must be check for file integrity. Skipped for now
				try (Stream<String> lineStream = Files.lines(filePath)) {
					nextRowId = (int) lineStream.count();
				}
				log.info("File '{}' contains {} rows", file.getAbsolutePath(), nextRowId);
			}
			log.info("TableFile instance successfully created");
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			throw createInternalError(e);
		}
	}


	private LockWrapper acquireWriteLock() {
		return new LockWrapper(readWriteLock.writeLock());
	}

	private LockWrapper acquireReadLock() {
		return new LockWrapper(readWriteLock.readLock());
	}

	int insert(List<String> values) {
		try (LockWrapper lock = acquireWriteLock()) {
			Files.writeString(
					filePath,
					packValues(values) + System.lineSeparator(),
					StandardOpenOption.CREATE, StandardOpenOption.APPEND
			);
			return nextRowId++;
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			throw createInternalError(e);
		}
	}

	void update(int rowId, List<String> values) {
		try (LockWrapper lock = acquireWriteLock()) {
			if (rowId >= nextRowId) {
				throw createNotFound(rowId);
			}
			if (nextRowId < properties.getMaxRowsInMemory()) {
				List<String> lines = Files.readAllLines(filePath);
				if (lines.size() != nextRowId) {
					throw createCorruptedTableError();
				}
				lines.set(rowId, packValues(values));
				Files.writeString(
						filePath,
						String.join(System.lineSeparator(), lines) + System.lineSeparator()
				);
			} else {
				updateWithTmpTable(rowId, values);
			}
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			throw createInternalError(e);
		}
	}

	private void updateWithTmpTable(int rowId, List<String> values) throws IOException {
		final Path tmpTablePath = Path.of(filePath.toAbsolutePath() + "_$tmp");
		try (
				BufferedReader reader = Files.newBufferedReader(filePath);
				BufferedWriter writer = Files.newBufferedWriter(tmpTablePath)
		) {
			moveLines(reader, writer, rowId);

			String line = reader.readLine();
			if (line == null) throw createCorruptedTableError();
			line = packValues(values);
			writer.write(line + System.lineSeparator());

			moveLines(reader, writer, nextRowId - rowId - 1);

			line = reader.readLine();
			if (line != null) throw createCorruptedTableError();
		} catch (IOException e) {
			Files.deleteIfExists(tmpTablePath);
			throw e;
		}
		Files.move(tmpTablePath, filePath, StandardCopyOption.REPLACE_EXISTING);
	}

	private void moveLines(BufferedReader reader, BufferedWriter writer, int count) throws IOException {
		for (int i = 0; i < count; i++) {
			String line = reader.readLine();
			if (line == null) throw createCorruptedTableError();
			writer.write(line + System.lineSeparator());
		}
	}

	List<String> select(int rowId) throws NotFoundException {
		try (LockWrapper lock = acquireReadLock()) {
			if (rowId >= nextRowId) {
				throw createNotFound(rowId);
			}
			try (Stream<String> lineStream = Files.lines(filePath)) {
				return unpackValues(lineStream.skip(rowId).findFirst().orElseThrow(this::createCorruptedTableError));
			} catch (IOException e) {
				log.error(e.getMessage(), e);
				throw createInternalError(e);
			}
		}
	}

	private NotFoundException createNotFound(int rowId) {
		return new NotFoundException(String.format("Record with ID=%d not found in '%s'", rowId, tableName));
	}

	private DatabaseException createInternalError(String msg) {
		return new DatabaseException("Database internal error: " + msg);
	}

	private DatabaseException createCorruptedTableError() {
		return new DatabaseException("Database internal error: Corrupted table file: " + tableName);
	}

	private DatabaseException createInternalError(Throwable e) {
		return new DatabaseException("Database internal error: " + e.getMessage(), e);
	}

	/**
	 * Escape some characters with "\" and add quotes for empty strings (""):
	 *   \     -> \\
	 *   ,     -> \c
	 *   "     -> \"
	 *   (CR)  -> \r
	 *   (LF)  -> \n
	 *   (TAB) -> \t
	 * Other characters (even unprintable) currently not touched.
	 *
	 * @param value unescaped string
	 * @return escaped string
	 */
	private static String escape(String value) {
		if (value.isEmpty()) return "\"\"";
		return value
				.replace("\\", "\\\\")
				.replace(",", "\\c")
				.replace("\"", "\\\"")
				.replace("\r", "\\r")
				.replace("\n", "\\n")
				.replace("\t", "\\t");
	}

	/**
	 * Unescape characters, that escaped with {@link #escape(String) escape} method, and remove quotes if needed.
	 * Unknown escape sequences replaced with escape code (e.g \x -> x).
	 *
	 * @param value escaped string
	 * @return unescaped string
	 */
	private static String unescape(String value) {
		if (value.startsWith("\"") && value.endsWith("\"")) {
			value = value.substring(1, value.length() - 1);
		}
		final char ESC = '\\';
		StringBuilder builder = new StringBuilder(value.length());
		for (int i = 0; i < value.length(); i++) {
			if (value.charAt(i) != ESC) {
				builder.append(value.charAt(i));
			} else if (++i < value.length()) {
				char escVal = value.charAt(i);
				if (escVal == 'c')
					builder.append(',');
				else if (escVal == 'r')
					builder.append('\r');
				else if (escVal == 'n')
					builder.append('\n');
				else if (escVal == 't')
					builder.append('\t');
				else
					builder.append(escVal);
			}
		}
		return builder.toString();
	}

	private static List<String> unpackValues(@NonNull String data) {
		if (data.isEmpty()) {
			return Collections.emptyList();
		}
		return Stream.of(data.split(",", -1)).map(TableFile::unescape).collect(Collectors.toList());
	}

	private static String packValues(List<String> values) {
		return values.stream().map(TableFile::escape).collect(Collectors.joining(","));
	}

}
