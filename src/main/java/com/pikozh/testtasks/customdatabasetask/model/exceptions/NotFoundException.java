package com.pikozh.testtasks.customdatabasetask.model.exceptions;

/**
 * Exception thrown when a table name / row ID combination does not match an existing row.
 */
@SuppressWarnings("serial")
public class NotFoundException extends RuntimeException {
	/**
	 * Constructs a new exception with the specified detail message.
	 *
	 * @param message the detail message
	 */
	public NotFoundException(String message) {
		super(message);
	}
}
