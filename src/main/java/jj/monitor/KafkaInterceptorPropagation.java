package jj.monitor;

import brave.propagation.Propagation.Getter;
import brave.propagation.Propagation.Setter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * Propagation utilities to inject and extract context from {@link Header}
 */
final class KafkaInterceptorPropagation {
  static final Charset UTF_8 = StandardCharsets.UTF_8;

  static final Setter<Headers, String> HEADER_SETTER = (carrier, key, value) -> {
    carrier.remove(key);
    carrier.add(key, value.getBytes(UTF_8));
  };

  static final Getter<Headers, String> HEADER_GETTER = (carrier, key) -> {
    Header header = carrier.lastHeader(key);
    if (header == null) return null;
    return new String(header.value(), UTF_8);
  };

  KafkaInterceptorPropagation() {
  }
}
