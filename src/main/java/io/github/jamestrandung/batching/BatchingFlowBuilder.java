package io.github.jamestrandung.batching;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.integration.aggregator.CorrelationStrategy;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.store.MessageGroup;
import org.springframework.integration.store.SimpleMessageStore;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

@Slf4j
@Builder(access = AccessLevel.PRIVATE)
public class BatchingFlowBuilder {
  private String id;
  private MessageChannel channel;
  private Supplier<Integer> thresholdSupplier;
  private Supplier<Long> timeoutSupplier;
  private UnaryOperator<AggregatorSpec> aggregator;

  public static BatchingFlowBuilder create(String id, MessageChannel channel) {
    return BatchingFlowBuilder.builder()
        .id(id)
        .channel(channel)
        .aggregator(aggregator -> aggregator)
        .build();
  }

  public BatchingFlowBuilder withBatchingStrategy(
      CorrelationStrategy correlationStrategy,
      Supplier<Integer> thresholdSupplier,
      Supplier<Long> timeoutSupplier
  ) {
    this.thresholdSupplier = thresholdSupplier;
    this.timeoutSupplier = timeoutSupplier;

    UnaryOperator<AggregatorSpec> currentAggregator = this.aggregator;

    this.aggregator = aggregator -> currentAggregator.apply(aggregator)
        .correlationStrategy(correlationStrategy)
        .releaseStrategy(new DynamicMessageCountReleaseStrategy(thresholdSupplier)) // Max batch size
        .groupTimeout(msg -> timeoutSupplier.get()); // Timeout for releasing the batch

    return this;
  }

  public <P, K, T, R> BatchingFlowBuilder withKeyedResultBatchProcessor(
      Function<P, K> payloadKeyExtractor,
      Function<List<P>, T> payloadBatcher,
      Function<T, Map<K, R>> batchProcessor
  ) {
    UnaryOperator<AggregatorSpec> currentAggregator = this.aggregator;

    this.aggregator = aggregator -> currentAggregator.apply(aggregator)
        .outputProcessor(group -> this.doExecuteBatchWithKeyedResult(group, payloadKeyExtractor, payloadBatcher, batchProcessor));

    return this;
  }

  <P, K, T, R> Message<?> doExecuteBatchWithKeyedResult(
      MessageGroup messageGroup,
      Function<P, K> payloadKeyExtractor,
      Function<List<P>, T> payloadBatcher,
      Function<T, Map<K, R>> batchProcessor
  ) {
    log.info(
        "BatchingFlowBuilder.doExecuteBatchWithKeyedResult started, flow ID: {}, threshold: {}, timeout: {}ms, message count: {}",
        this.id, this.thresholdSupplier.get(), this.timeoutSupplier.get(), CollectionUtils.size(messageGroup.getMessages())
    );

    Instant start = Instant.now();

    try {
      T batchedPayload = StreamEx.of(messageGroup.getMessages())
          .map(message -> (P) message.getPayload())
          .distinct(payloadKeyExtractor)
          .toListAndThen(payloadBatcher);

      Map<K, R> results = batchProcessor.apply(batchedPayload);

      List<Pair<Object, R>> payload = StreamEx.of(messageGroup.getMessages())
          .map(message ->
                   Pair.of(
                       message.getHeaders().getReplyChannel(),
                       results.get(payloadKeyExtractor.apply((P) message.getPayload()))
                   )
          ).toList();

      return MessageBuilder.withPayload(payload).build();

    } catch (Exception ex) {
      List<Pair<Object, Exception>> payload = StreamEx.of(messageGroup.getMessages())
          .map(message -> Pair.of(message.getHeaders().getReplyChannel(), ex))
          .toList();

      return MessageBuilder.withPayload(payload).build();

    } finally {
      log.info(
          "BatchingFlowBuilder.doExecuteBatchWithKeyedResult completed, flow ID: {}, timeout: {}ms, message count: {}, taken: {}ms",
          this.id, this.timeoutSupplier.get(),
          CollectionUtils.size(messageGroup.getMessages()),
          Duration.between(start, Instant.now()).toMillis()
      );
    }
  }

  public <P, T, R> BatchingFlowBuilder withIdenticalResultBatchProcessor(
      Function<List<P>, T> payloadBatcher,
      Function<T, R> batchProcessor
  ) {
    UnaryOperator<AggregatorSpec> currentAggregator = this.aggregator;

    this.aggregator = aggregator -> currentAggregator.apply(aggregator)
        .outputProcessor(group -> this.doExecuteBatchWithIdenticalResult(group, payloadBatcher, batchProcessor));

    return this;
  }

  <P, T, R> Message<?> doExecuteBatchWithIdenticalResult(
      MessageGroup messageGroup,
      Function<List<P>, T> payloadBatcher,
      Function<T, R> batchProcessor
  ) {
    log.info(
        "BatchingFlowBuilder.doExecuteBatchWithIdenticalResult started, flow ID: {}, threshold: {}, timeout: {}ms, message count: {}",
        this.id, this.thresholdSupplier.get(), this.timeoutSupplier.get(), CollectionUtils.size(messageGroup.getMessages())
    );

    Instant start = Instant.now();

    try {
      T batchedPayload = StreamEx.of(messageGroup.getMessages())
          .map(message -> (P) message.getPayload())
          .toListAndThen(payloadBatcher);

      R result = batchProcessor.apply(batchedPayload);

      List<Pair<Object, R>> payload = StreamEx.of(messageGroup.getMessages())
          .map(message -> Pair.of(message.getHeaders().getReplyChannel(), result))
          .toList();

      return MessageBuilder.withPayload(payload).build();

    } catch (Exception ex) {
      List<Pair<Object, Exception>> payload = StreamEx.of(messageGroup.getMessages())
          .map(message -> Pair.of(message.getHeaders().getReplyChannel(), ex))
          .toList();

      return MessageBuilder.withPayload(payload).build();

    } finally {
      log.info(
          "BatchingFlowBuilder.doExecuteBatchWithIdenticalResult completed, flow ID: {}, timeout: {}ms, message count: {}, taken: {}ms",
          this.id, this.timeoutSupplier.get(),
          CollectionUtils.size(messageGroup.getMessages()),
          Duration.between(start, Instant.now()).toMillis()
      );
    }
  }

  public <P, T> BatchingFlowBuilder withNoResultBatchProcessor(
      Function<List<P>, T> payloadBatcher,
      Consumer<T> batchProcessor
  ) {
    UnaryOperator<AggregatorSpec> currentAggregator = this.aggregator;

    this.aggregator = aggregator -> currentAggregator.apply(aggregator)
        .outputProcessor(group -> this.doExecuteBatchWithNoResult(group, payloadBatcher, batchProcessor));

    return this;
  }

  <P, T> Message<?> doExecuteBatchWithNoResult(
      MessageGroup messageGroup,
      Function<List<P>, T> payloadBatcher,
      Consumer<T> batchProcessor
  ) {
    log.info(
        "BatchingFlowBuilder.doExecuteBatchWithNoResult started, flow ID: {}, threshold: {}, timeout: {}ms, message count: {}",
        this.id, this.thresholdSupplier.get(), this.timeoutSupplier.get(), CollectionUtils.size(messageGroup.getMessages())
    );

    Instant start = Instant.now();

    try {
      T batchedPayload = StreamEx.of(messageGroup.getMessages())
          .map(message -> (P) message.getPayload())
          .toListAndThen(payloadBatcher);

      batchProcessor.accept(batchedPayload);

      List<Pair<Object, Object>> payload = StreamEx.of(messageGroup.getMessages())
          .map(message -> Pair.of(message.getHeaders().getReplyChannel(), null))
          .toList();

      return MessageBuilder.withPayload(payload).build();

    } catch (Exception ex) {
      List<Pair<Object, Exception>> payload = StreamEx.of(messageGroup.getMessages())
          .map(message -> Pair.of(message.getHeaders().getReplyChannel(), ex))
          .toList();

      return MessageBuilder.withPayload(payload).build();

    } finally {
      log.info(
          "BatchingFlowBuilder.doExecuteBatchWithNoResult completed, flow ID: {}, timeout: {}ms, message count: {}, taken: {}ms",
          this.id, this.timeoutSupplier.get(),
          CollectionUtils.size(messageGroup.getMessages()),
          Duration.between(start, Instant.now()).toMillis()
      );
    }
  }

  public IntegrationFlow build() {
    return IntegrationFlows.from(this.channel)
        //        .log()
        .aggregate(
            aggregator -> this.aggregator.apply(aggregator)
                .messageStore(new SimpleMessageStore())
                .expireGroupsUponCompletion(true)
                .expireGroupsUponTimeout(true)
                .sendPartialResultOnExpiry(true)
        )
        //        .log()
        .split()
        //        .log()
        .handle(Pair.class, (payload, headers) ->
            MessageBuilder.withPayload(payload.getRight())
                .copyHeaders(headers)
                .setReplyChannel((MessageChannel) payload.getLeft())
                .setErrorChannel((MessageChannel) payload.getLeft())
                .build()
        )
        //        .log()
        .handle((payload, headers) -> payload)
        .get();
  }
}

