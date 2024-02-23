package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.KafkaConsumerManager;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.PipelineStatus;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming.Streamer;
import java.io.Closeable;
import java.io.IOException;
import lombok.Builder;

@Builder
public class Pipeline implements Closeable, Runnable {

  PipelineConfiguration pipelineConfiguration;
  Snapshotter snapshotter;
  Streamer streamer;
  MetricsService metricsService;

  @Override
  public void close() throws IOException {
    metricsService.clear();
  }

  @Override
  public void run() {
    try {
      metricsService.setPipelineStatus(PipelineStatus.STARTING);
      var tables = pipelineConfiguration.getSourceConfig().getTables();
      if (pipelineConfiguration.getDestinationConfig() != null) {
        var bootstrapServer = pipelineConfiguration.getKafkaConfig().getBootstrapServer();
        var topicPrefix = pipelineConfiguration.getKafkaConfig().getTopicPrefix();
        var eventProcessor =
            pipelineConfiguration.getDestinationConfig().createChangeEventProcessor();
        KafkaConsumerManager.createConsumers(bootstrapServer, topicPrefix, tables, eventProcessor);
      }

      metricsService.setPipelineStatus(PipelineStatus.SNAPSHOTTING);
      snapshotter.snapshot(tables);

      metricsService.setPipelineStatus(PipelineStatus.STREAMING);
      streamer.stream();
    } finally {
      metricsService.setPipelineStatus(PipelineStatus.NOT_RUNNING);
    }
  }
}
