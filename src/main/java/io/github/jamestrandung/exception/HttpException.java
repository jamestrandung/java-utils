package io.github.jamestrandung.exception;

import org.apache.commons.lang3.exception.ExceptionUtils;

public class HttpException extends RuntimeException {
  public HttpException(int failingStatusCode, String responseBody) {
    super(String.format("HTTP request failed with status code: %s, body: %s", failingStatusCode, responseBody));
  }

  public HttpException(Throwable ex) {
    super(String.format("HTTP request failed, cause: %s", ExceptionUtils.getStackTrace(ex)));
  }
}
