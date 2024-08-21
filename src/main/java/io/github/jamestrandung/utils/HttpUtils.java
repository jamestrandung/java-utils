package io.github.jamestrandung.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.jamestrandung.enums.RequestMethod;
import io.github.jamestrandung.exception.HttpException;
import io.github.jamestrandung.exception.InvalidPayloadException;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

@Slf4j
public class HttpUtils {
  public static <R> R execute(
      RequestMethod method,
      String uri,
      String authorization,
      Class<R> responseClazz
  ) {
    return execute(method, uri, authorization, null, responseClazz);
  }

  public static <T, R> R execute(
      RequestMethod method,
      String uri,
      String authorization,
      T payload,
      Class<R> responseClazz
  ) {
    log.info("HttpUtils.execute start, method: {}, url: {}, payload: {}", method, uri, payload);

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      String jsonPayload = FormatUtils.toJsonString(payload);
      HttpEntity body = payload == null ? null : new StringEntity(StringUtils.defaultString(jsonPayload), ContentType.APPLICATION_JSON);

      HttpUriRequest request = RequestBuilder.create(method.name())
          .setUri(uri)
          .setHeader(HttpHeaders.AUTHORIZATION, authorization)
          .setEntity(body)
          .build();

      HttpResponse response = client.execute(request);
      if (response.getStatusLine().getStatusCode() != 200) {
        log.info(
            "HttpUtils.execute unexpected response, method: {}, url: {}, payload: {}, response: {}",
            method, uri, payload, response
        );

        throw new HttpException(response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity()));
      }

      if (responseClazz == Void.class) {
        return null;
      }

      String responseBody = EntityUtils.toString(response.getEntity());
      if (StringUtils.isBlank(responseBody)) {
        throw new InvalidPayloadException("empty response body");
      }

      try {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.registerModule(new JavaTimeModule());

        return objectMapper.readValue(responseBody, responseClazz);

      } catch (Exception ex) {
        throw new InvalidPayloadException(
            String.format("cannot parse response body, content: %s, error: %s", responseBody, ExceptionUtils.getStackTrace(ex))
        );
      }

    } catch (IOException ex) {
      throw new HttpException(ex);
    }
  }
}
