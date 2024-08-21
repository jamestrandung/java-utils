package io.github.jamestrandung.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.jamestrandung.exception.InvalidPayloadException;
import io.github.jamestrandung.utils.FormatUtils;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class JsonMessage<T> implements ProducerMessage<String, String>, ConsumerMessage<T> {
  @Getter
  private String topic;
  @Getter
  private T producerData;
  private Function<T, String> keyExtractor;
  private ConsumerRecord<String, String> consumerRecord;
  private Class<T> consumerClazz;

  public static <T> JsonMessage<T> produce(String topic, T producerData) {
    var result = new JsonMessage<T>();
    result.topic = topic;
    result.producerData = producerData;
    return result;
  }

  public static <T> JsonMessage<T> produce(String topic, T producerData, Function<T, String> keyExtractor) {
    var result = new JsonMessage<T>();
    result.topic = topic;
    result.producerData = producerData;
    result.keyExtractor = keyExtractor;
    return result;
  }

  public static <T> JsonMessage<T> consume(ConsumerRecord<String, String> consumerRecord, Class<T> consumerClazz) {
    var result = new JsonMessage<T>();
    result.consumerRecord = consumerRecord;
    result.consumerClazz = consumerClazz;
    return result;
  }

  @Override
  public String getKey() {
    if (keyExtractor == null) {
      return ProducerMessage.super.getKey();
    }

    return keyExtractor.apply(producerData);
  }

  public String getPayload() {
    if (producerData instanceof String s) {
      return s;
    }

    return FormatUtils.toJsonString(producerData);
  }

  public T parse(Predicate<T> validator) {
    return this.parse(null, validator);
  }

  public T parse(Consumer<T> finalizer, Predicate<T> validator) {
    T obj = FormatUtils.fromJsonString(consumerRecord.value(), new TypeReference<T>() {
    });

    if (finalizer != null && obj != null) {
      finalizer.accept(obj);
    }

    if (validator != null && !validator.test(obj)) {
      throw new InvalidPayloadException(String.format("invalid payload - key: %s, payload: %s", consumerRecord.key(), consumerRecord.value()));
    }

    return obj;
  }
}
