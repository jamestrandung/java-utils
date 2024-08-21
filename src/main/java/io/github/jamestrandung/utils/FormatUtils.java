package io.github.jamestrandung.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.NonNull;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FormatUtils {
  private static final ObjectMapper OBJECT_MAPPER;
  private static final String COMMA_SEPARATOR = ",";

  static {
    OBJECT_MAPPER = new ObjectMapper();
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    OBJECT_MAPPER.registerModule(new JavaTimeModule());
  }

  public static <T> boolean isValidJsonString(String json, TypeReference<T> expectedTypeReference) {
    try {
      if (StringUtils.isBlank(json)) {
        return true;
      }

      OBJECT_MAPPER.readValue(json, expectedTypeReference);
      return true;

    } catch (Exception ex) {
      return false;
    }
  }

  public static <T> T fromJsonString(String json, TypeReference<T> typeReference) {
    try {
      if (StringUtils.isBlank(json)) {
        return null;
      }

      return OBJECT_MAPPER.readValue(json, typeReference);

    } catch (Exception ex) {
      log.error("FormatUtils.parse failed, error: {}", ex.getMessage());
      throw new RuntimeException(ex);
    }
  }

  public static String toJsonString(Object object) {
    try {
      if (object == null) {
        return null;
      }

      return OBJECT_MAPPER.writeValueAsString(object);

    } catch (Exception ex) {
      log.error("FormatUtils.toString failed, error: {}", ex.getMessage());
      throw new RuntimeException(ex);
    }
  }

  public static boolean isValidDateTimeString(String date, @NonNull DateTimeFormatter expectedFormat) {
    try {
      if (date == null) {
        return true;
      }

      if (date.isEmpty()) {
        return false;
      }

      expectedFormat.parse(date);
      return true;

    } catch (DateTimeParseException ex) {
      return false;
    }
  }

  public static <T> List<T> fromCommaSeparatedString(
      String str,
      @NonNull Function<String, T> converter
  ) {
    return fromCommaSeparatedString(str, converter, Collectors.toUnmodifiableList());
  }

  public static <T, A, R> R fromCommaSeparatedString(
      String str,
      @NonNull Function<String, T> converter,
      @NonNull Collector<? super T, A, R> collector
  ) {
    String[] parts = StringUtils.split(str, COMMA_SEPARATOR);

    return StreamEx.of(ArrayUtils.nullToEmpty(parts))
        .map(converter)
        .collect(collector);
  }

  public static <T> String toCommaSeparatedString(Collection<T> collection) {
    if (CollectionUtils.isEmpty(collection)) {
      return null;
    }

    return StreamEx.of(collection)
        .joining(COMMA_SEPARATOR);
  }
}
