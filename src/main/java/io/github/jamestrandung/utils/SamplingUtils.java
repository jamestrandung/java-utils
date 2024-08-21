package io.github.jamestrandung.utils;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class SamplingUtils {
  static final Runnable DO_NOTHING_RUNNABLE = () -> {
  };

  public static void run(double percentage, Runnable r) {
    runOrElse(percentage, r, DO_NOTHING_RUNNABLE);
  }

  public static void runOrElse(double percentage, Runnable r, Runnable elseR) {
    Callable<Void> c = () -> {
      r.run();
      return null;
    };

    Callable<Void> elseC = () -> {
      elseR.run();
      return null;
    };

    try {
      callOrElse(percentage, c, elseC);

    } catch (Exception ex) {
      throw new RuntimeException(ex);

    }
  }

  public static <V> V supplyOrElse(double percentage, Supplier<V> s, Supplier<V> elseS) {
    try {
      return callOrElse(percentage, s::get, elseS::get);

    } catch (Exception ex) {
      throw new RuntimeException(ex);

    }
  }

  public static <V> V callOrElse(double percentage, Callable<V> c, Callable<V> elseC) throws Exception {
    if (percentage == 0) {
      return elseC.call();
    }

    if (percentage < 0 || percentage >= 100) {
      return c.call();
    }

    double randDouble = new Random(System.currentTimeMillis()).nextDouble(100);
    if (randDouble > percentage) {
      return elseC.call();
    }

    return c.call();
  }
}

