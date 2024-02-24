package com.thirdyearproject.changedatacaptureapplication.api;

import com.thirdyearproject.changedatacaptureapplication.engine.exception.PipelineConflictException;
import com.thirdyearproject.changedatacaptureapplication.engine.exception.PipelineNotRunningException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice
@Slf4j
public class ApiExceptionHandler {

  @ResponseStatus(HttpStatus.CONFLICT)
  @ExceptionHandler(PipelineConflictException.class)
  public ResponseEntity<ErrorMessage> handleConflict(PipelineConflictException ex) {
    var errorMessage = ErrorMessage.builder().message(ex.getMessage()).build();
    return new ResponseEntity<>(errorMessage, HttpStatus.CONFLICT);
  }

  @ResponseStatus(HttpStatus.NOT_FOUND)
  @ExceptionHandler(PipelineNotRunningException.class)
  public ResponseEntity<ErrorMessage> handleConflict(PipelineNotRunningException ex) {
    var errorMessage = ErrorMessage.builder().message(ex.getMessage()).build();
    return new ResponseEntity<>(errorMessage, HttpStatus.NOT_FOUND);
  }
}
