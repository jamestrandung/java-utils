package io.github.jamestrandung.batching;

import org.springframework.integration.aggregator.CorrelationStrategy;
import org.springframework.messaging.Message;

public class BatchAllCorrelationStrategy implements CorrelationStrategy {
  private static final String CORRELATION_KEY = "BATCH_ALL_CORRELATION_KEY";

  public static BatchAllCorrelationStrategy create() {
    return new BatchAllCorrelationStrategy();
  }

  @Override
  public Object getCorrelationKey(Message<?> message) {
    return CORRELATION_KEY;
  }
}
