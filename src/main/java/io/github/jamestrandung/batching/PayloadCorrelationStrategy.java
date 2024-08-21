package io.github.jamestrandung.batching;

import java.util.function.Function;
import lombok.Builder;
import org.springframework.integration.aggregator.CorrelationStrategy;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

@Builder
public class PayloadCorrelationStrategy<T> implements CorrelationStrategy {
  private final Function<T, Object> correlationKeyExtractor;

  public static <T> PayloadCorrelationStrategy<T> create(Function<T, Object> correlationKeyExtractor) {
    Assert.notNull(correlationKeyExtractor, "Correlation key extractor is required");

    return PayloadCorrelationStrategy.<T>builder()
        .correlationKeyExtractor(correlationKeyExtractor)
        .build();
  }

  @Override
  public Object getCorrelationKey(Message<?> message) {
    return correlationKeyExtractor.apply((T) message.getPayload());
  }
}
