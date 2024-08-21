package io.github.jamestrandung.batching;

import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.integration.aggregator.ReleaseStrategy;
import org.springframework.integration.store.MessageGroup;
import org.springframework.util.Assert;

public class DynamicMessageCountReleaseStrategy implements ReleaseStrategy {
  private static final int DEFAULT_BATCH_SIZE = 500;
  private final Supplier<Integer> thresholdSupplier;

  public DynamicMessageCountReleaseStrategy() {
    this(() -> DEFAULT_BATCH_SIZE);
  }

  public DynamicMessageCountReleaseStrategy(Supplier<Integer> thresholdSupplier) {
    Assert.notNull(thresholdSupplier, "Threshold supplier is required");
    this.thresholdSupplier = thresholdSupplier;
  }

  public boolean canRelease(MessageGroup group) {
    return group.size() >= Objects.requireNonNullElse(this.thresholdSupplier.get(), DEFAULT_BATCH_SIZE);
  }
}
