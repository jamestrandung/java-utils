package io.github.jamestrandung.utils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.lang.NonNull;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StreamUtils {
  public static <T, V> List<V> toList(
      Collection<T> data,
      @NonNull Function<T, V> extractor
  ) {
    return extract(data, extractor, Collectors.toList());
  }

  public static <T, V, A, R> R extract(
      Collection<T> data,
      @NonNull Function<T, V> extractor,
      @NonNull Collector<? super V, A, R> collector
  ) {
    return StreamEx.of(CollectionUtils.emptyIfNull(data))
        .map(extractor)
        .collect(collector);
  }

  public static <T, K> Map<K, T> toMap(
      Collection<T> data,
      @NonNull Function<T, K> keyExtractor
  ) {
    return toMap(data, keyExtractor, Function.identity());
  }

  public static <T, K, V> Map<K, V> toMap(
      Collection<T> data,
      @NonNull Function<T, K> keyExtractor,
      @NonNull Function<T, V> valueExtractor
  ) {
    return StreamEx.of(CollectionUtils.emptyIfNull(data))
        .toMap(keyExtractor, valueExtractor);
  }
}
