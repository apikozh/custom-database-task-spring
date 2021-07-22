package com.pikozh.testtasks.customdatabasetask.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

/**
 * @author a.pikozh
 */
@Data
@AllArgsConstructor
public class ErrorMessage {
	private int statusCode;
	private Date timestamp;
	private String message;
	private String description;
}