package com.pikozh.testtasks.customdatabasetask.controllers;

import com.pikozh.testtasks.customdatabasetask.model.ErrorMessage;
import com.pikozh.testtasks.customdatabasetask.model.exceptions.NotFoundException;
import com.pikozh.testtasks.customdatabasetask.services.Database;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;

import java.util.Date;
import java.util.List;

/**
 * @author a.pikozh
 */
@Slf4j
@RestController
@RequestMapping("/database")
public class DatabaseRestController {

	private final Database db;

	@Autowired
	public DatabaseRestController(Database db) {
		this.db = db;
	}

	@PostMapping("/{tableName}")
	public int insert(
			@PathVariable("tableName") String tableName,
			@RequestBody List<String> data) {
		return db.insert(tableName, data);
	}

	@GetMapping("/{tableName}/{rowId}")
	public List<String> select(
			@PathVariable(value = "tableName") String tableName,
			@PathVariable(value = "rowId") Integer rowId) {

		return db.select(tableName, rowId);
	}

	@PutMapping("/{tableName}/{rowId}")
	public void update(
			@PathVariable(value = "tableName") String tableName,
			@PathVariable(value = "rowId") int rowId,
			@RequestBody List<String> data) {

		db.update(tableName, rowId, data);
	}

	@ExceptionHandler(NotFoundException.class)
	@ResponseStatus(value = HttpStatus.NOT_FOUND)
	public ErrorMessage resourceNotFoundException(NotFoundException ex, WebRequest request) {
		ErrorMessage message = new ErrorMessage(
				HttpStatus.NOT_FOUND.value(),
				new Date(),
				ex.getMessage(),
				request.getDescription(false));
		log.error("REST Not Found error reported: {}", message);
		return message;
	}

	@ExceptionHandler(Exception.class)
	@ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
	public ErrorMessage globalExceptionHandler(Exception ex, WebRequest request) {
		ErrorMessage message = new ErrorMessage(
				HttpStatus.INTERNAL_SERVER_ERROR.value(),
				new Date(),
				ex.getMessage(),
				request.getDescription(false));

		log.error("REST General error reported: {}", message);
		return message;
	}
}
