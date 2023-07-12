package jj.monitor;

import brave.Span;
import brave.Tracing;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;

/**
 * Record traces when records are sent to a Kafka Topic.
 * <p>
 * Extract context from incoming Record, if exist injected in its header, and use it to link it to
 * the Span created by the interceptor.
 */
public class TracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {
  static final String SPAN_SEND_NAME = "send";
  static final String SPAN_ACK_NAME = "ack";

  TracingConfiguration configuration;
  Tracing tracing;
  String remoteServiceName;
  TraceContext.Injector<Headers> injector;
  TraceContext.Extractor<Headers> extractor;

  @Override public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
    TraceContextOrSamplingFlags traceContextOrSamplingFlags = extractor.extract(record.headers());
    Span span = tracing.tracer().nextSpan(traceContextOrSamplingFlags);
    tracing.propagation().keys().forEach(key -> record.headers().remove(key));
    injector.inject(span.context(), record.headers());
    if (!span.isNoop()) {
      if (record.key() instanceof String && !"".equals(record.key())) {
        span.tag(KafkaInterceptorTagKey.KAFKA_KEY, record.key().toString());
      }
      span
          .tag(KafkaInterceptorTagKey.KAFKA_TOPIC, record.topic())
          .tag(KafkaInterceptorTagKey.KAFKA_CLIENT_ID,
              configuration.getString(ProducerConfig.CLIENT_ID_CONFIG))
          .name(SPAN_SEND_NAME)
          .kind(Span.Kind.PRODUCER)
          .remoteServiceName(remoteServiceName)
          .start()
          .finish();
    }
    return record;
  }

  @Override public void onAcknowledgement(RecordMetadata recordMetadata, Exception exception) {

//    if(recordMetadata != null)  {
////      final KafkaInterceptorTagKey
//
//      recordMetadata.
//
//      if (!span.isNoop()) {
//        if (record.key() instanceof String && !"".equals(recordMetadata.key())) {
//          span.tag(KafkaInterceptorTagKey.KAFKA_KEY, recordMetadata.key().toString());
//        }
//        span
//            .tag(KafkaInterceptorTagKey.KAFKA_TOPIC, recordMetadata.topic())
//            .tag(KafkaInterceptorTagKey.KAFKA_CLIENT_ID, configuration.getString(ProducerConfig.CLIENT_ID_CONFIG))
//            .name(SPAN_SEND_NAME)
//            .kind(Span.Kind.PRODUCER)
//            .remoteServiceName(remoteServiceName)
//            .start()
//            .finish();
//      }
//    } else {
//      //TODO handle error
//    }
  }

  @Override public void close() {
    tracing.close();
  }

  @Override public void configure(Map<String, ?> configs) {
    configuration = new TracingConfiguration(configs);
    remoteServiceName =
        configuration.getStringOrDefault(TracingConfiguration.REMOTE_SERVICE_NAME_CONFIG,
            TracingConfiguration.REMOTE_SERVICE_NAME_DEFAULT);
    tracing = new TracingBuilder(configuration).build();
    extractor = tracing.propagation().extractor(KafkaInterceptorPropagation.HEADER_GETTER);
    injector = tracing.propagation().injector(KafkaInterceptorPropagation.HEADER_SETTER);
  }
}
