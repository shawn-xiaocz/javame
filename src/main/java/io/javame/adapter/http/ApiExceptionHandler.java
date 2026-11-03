package io.javame.adapter.http;

import io.javame.client.MatchingContracts.ErrorResult;
import io.javame.service.MatchingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.transaction.TransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/** Keeps SQL, connection details and raw input out of public error responses. */
@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(MatchingException.class)
    public ResponseEntity<ErrorResult> business(MatchingException ex) {
        HttpStatus status = switch (ex.code()) {
            case "INVALID_ARGUMENT" -> HttpStatus.BAD_REQUEST;
            case "NOT_FOUND" -> HttpStatus.NOT_FOUND;
            default -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(new ErrorResult(ex.code(), ex.getMessage()));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class, MissingServletRequestParameterException.class})
    public ResponseEntity<ErrorResult> invalid(Exception ex) {
        return ResponseEntity.badRequest().body(new ErrorResult("INVALID_ARGUMENT", "Request parameters are invalid"));
    }

    @ExceptionHandler({DataAccessException.class, TransactionException.class})
    public ResponseEntity<ErrorResult> unavailable(RuntimeException ex) {
        LOG.error("Matching API storage transaction failed", ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResult("UNAVAILABLE", "Storage unavailable; retry using the same order ID"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResult> unexpected(Exception ex) {
        LOG.error("Matching API request failed", ex);
        return ResponseEntity.internalServerError().body(new ErrorResult("INTERNAL", "Request could not be processed"));
    }
}
