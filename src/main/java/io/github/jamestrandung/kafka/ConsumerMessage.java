package io.github.jamestrandung.kafka;

import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerMessage<T> {
  static String generateLogID(ConsumerRecord<?, ?> record) {
    return String.format("%s-%d-%d", record.topic(), record.partition(), record.offset());
  }

  T parse(Predicate<T> validator);

  T parse(Consumer<T> finalizer, Predicate<T> validator);
}
