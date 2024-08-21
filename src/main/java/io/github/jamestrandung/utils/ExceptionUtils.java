package io.github.jamestrandung.utils;

import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExceptionUtils extends org.apache.commons.lang3.exception.ExceptionUtils {
  public static void executeSwallowingException(Runnable task) {
    try {
      task.run();

    } catch (Exception anotherEx) {
      log.error("ExceptionUtils.executeSwallowingException - error：{}", ExceptionUtils.getStackTrace(anotherEx));
    }
  }

  public static <T extends Throwable> T wrap(Exception ex, Function<Exception, T> wrapper) {
    T wrappedEx = wrapper.apply(ex);

    if (wrappedEx.getClass().isInstance(ex)) {
      return (T) ex;
    }

    return wrappedEx;
  }
}
