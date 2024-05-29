package io.github.wukachn.litecdc.api.model.response;

import io.github.wukachn.litecdc.engine.metrics.TableCRUD;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GetMetricsResponse {
  long pipelineStartTime;
  Long dbProducerTimeLagMs;
  Long producerConsumerTimeLagMs;
  long numOfTransactions;
  long totalProduced;
  long totalConsumed;
  List<TableCRUD> tables;
}
