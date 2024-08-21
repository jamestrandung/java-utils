package io.github.jamestrandung.kafka;

import io.github.jamestrandung.utils.ExceptionUtils;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class KafkaProducer<K, V> {
  private KafkaTemplate<K, V> kafkaTemplate;

  public void sendAndForget(@NonNull ProducerMessage<K, V> message) {
    try {
      this.send(message);

    } catch (Exception ex) {
      ExceptionUtils.executeSwallowingException(() -> {
        log.error(
            "KafkaProducer.sendAndForget failed, topic: {}, payload: {}, error：{}",
            message.getTopic(), message.getPayload(), ExceptionUtils.getStackTrace(ex)
        );
      });
    }
  }

  public CompletableFuture<SendResult<K, V>> send(@NonNull ProducerMessage<K, V> message) {
    return this.send(message.getTopic(), message.getPartition(), message.getKey(), message.getPayload());
  }

  public void sendAndForget(String topic, int partition, @Nullable K key, @Nullable V payload) {
    try {
      this.send(topic, partition, key, payload);

    } catch (Exception ex) {
      log.error(
          "KafkaProducer.sendAndForget failed, topic: {}, key: {}, payload: {}, error：{}",
          topic, key, payload, ExceptionUtils.getStackTrace(ex)
      );
    }
  }

  public CompletableFuture<SendResult<K, V>> send(String topic, int partition, @Nullable K key, @Nullable V payload) {
    log.debug("KafkaProducer.send start, topic: {}, key: {}, payload: {}", topic, key, payload);

    CompletableFuture<SendResult<K, V>> future = this.doSend(topic, partition, key, payload);

    future.whenComplete((result, ex) -> {
      if (ex == null) {
        log.debug("KafkaProducer.send success, topic: {}, key: {}, payload: {}, result：{}", topic, key, payload, result.toString());
        return;
      }

      log.error(
          "KafkaProducer.send failed, topic: {}, key: {}, payload: {}, error：{}",
          topic, key, payload, ExceptionUtils.getStackTrace(ex)
      );
    });

    return future;
  }

  private CompletableFuture<SendResult<K, V>> doSend(String topic, int partition, K key, V payload) {
    if (partition >= 0) {
      return kafkaTemplate.send(topic, partition, key, payload);
    }

    if (key != null) {
      return kafkaTemplate.send(topic, key, payload);
    }

    return kafkaTemplate.send(topic, payload);
  }
}
