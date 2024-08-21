package io.github.jamestrandung.kafka;

import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

@Slf4j
public abstract class BaseConsumer<K, V> {
  protected <T> void consumeAndAckBeforeExecuteSwallowingException(
      ConsumerRecord<K, V> record,
      Acknowledgment acknowledgment,
      Function<ConsumerRecord<K, V>, T> parser,
      Consumer<T> executor
  ) {
    acknowledgment.acknowledge();

    this.consumeAndExecuteSwallowingException(record, acknowledgment, parser, executor);
  }

  protected <T> void consumeAndExecuteSwallowingException(
      ConsumerRecord<K, V> record,
      Acknowledgment acknowledgment,
      Function<ConsumerRecord<K, V>, T> parser,
      Consumer<T> executor
  ) {
    try {
      this.consumeAndExecute(record, acknowledgment, parser, executor);

    } catch (Exception ex) {
      log.error(
          "[{}] BaseConsumer.consumeAndExecuteSwallowingException - error, message: {}, error: {}",
          ConsumerMessage.generateLogID(record), record.value(), ExceptionUtils.getStackTrace(ex)
      );

    } finally {
      acknowledgment.acknowledge();
    }
  }

  protected <T> void consumeAndExecute(
      ConsumerRecord<K, V> record,
      Acknowledgment acknowledgment,
      Function<ConsumerRecord<K, V>, T> parser,
      Consumer<T> executor
  ) {
    String logId = ConsumerMessage.generateLogID(record);

    if (record.value() == null) {
      acknowledgment.acknowledge();
      return;
    }

    log.debug("[{}] BaseConsumer.consumeAndExecute - start: message: {}", logId, record);

    T parsed = parser.apply(record);
    executor.accept(parsed);

    log.debug("[{}] BaseConsumer.consumeAndExecute - end: message: {}", logId, record);

    acknowledgment.acknowledge();
  }
}
