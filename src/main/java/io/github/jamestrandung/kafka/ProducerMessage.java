package io.github.jamestrandung.kafka;

public interface ProducerMessage<K, V> {
  int NO_PARTITION = -1;

  String getTopic();

  default int getPartition() {
    return NO_PARTITION;
  }

  default K getKey() {
    // null key will result in round-robin distribution across partitions
    return null;
  }

  V getPayload();
}

