package com.pikozh.testtasks.customdatabasetask.model.exceptions;

/**
 * @author a.pikozh
 */
public class DatabaseException extends RuntimeException {
	public DatabaseException() {
	}

	public DatabaseException(String message) {
		super(message);
	}

	public DatabaseException(String message, Throwable cause) {
		super(message, cause);
	}

	public DatabaseException(Throwable cause) {
		super(cause);
	}
}
