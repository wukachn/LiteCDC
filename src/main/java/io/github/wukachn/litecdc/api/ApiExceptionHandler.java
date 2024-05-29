package io.github.wukachn.litecdc.api;

import io.github.wukachn.litecdc.engine.exception.PipelineConflictException;
import io.github.wukachn.litecdc.engine.exception.PipelineNotRunningException;
import io.github.wukachn.litecdc.engine.exception.ValidationException;
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
  public ResponseEntity<ErrorMessage> handleNotFound(PipelineNotRunningException ex) {
    var errorMessage = ErrorMessage.builder().message(ex.getMessage()).build();
    return new ResponseEntity<>(errorMessage, HttpStatus.NOT_FOUND);
  }

  @ResponseStatus(HttpStatus.BAD_REQUEST)
  @ExceptionHandler(ValidationException.class)
  public ResponseEntity<ErrorMessage> handleBadRequest(ValidationException ex) {
    var causeMessage =
        (ex.getCause() != null)
            ? ex.getCause().getMessage()
            : "No cause provided, please view logs.";
    var errorMessage = ErrorMessage.builder().message(ex.getMessage() + " " + causeMessage).build();
    return new ResponseEntity<>(errorMessage, HttpStatus.BAD_REQUEST);
  }
}
