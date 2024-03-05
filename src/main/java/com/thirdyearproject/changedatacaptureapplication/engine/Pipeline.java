package com.thirdyearproject.changedatacaptureapplication.engine;

import com.thirdyearproject.changedatacaptureapplication.api.model.request.PipelineConfiguration;
import com.thirdyearproject.changedatacaptureapplication.engine.kafka.KafkaConsumerManager;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.MetricsService;
import com.thirdyearproject.changedatacaptureapplication.engine.metrics.PipelineStatus;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.snapshot.Snapshotter;
import com.thirdyearproject.changedatacaptureapplication.engine.produce.streaming.Streamer;
import java.io.Closeable;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class Pipeline implements Closeable, Runnable {

  PipelineConfiguration pipelineConfiguration;
  Snapshotter snapshotter;
  Streamer streamer;
  MetricsService metricsService;

  @Override
  public void close() {
    log.info("Closing pipeline.");
    metricsService.clear();
  }

  @Override
  public void run() {
    try {
      metricsService.startingPipeline();
      metricsService.setPipelineStatus(PipelineStatus.STARTING);
      pipelineConfiguration.validate();
      var tables = pipelineConfiguration.getSourceConfig().getTables();
      if (pipelineConfiguration.getDestinationConfig() != null) {
        var bootstrapServer = pipelineConfiguration.getKafkaConfig().getBootstrapServer();
        var topicPrefix = pipelineConfiguration.getKafkaConfig().getTopicPrefix();
        var eventProcessor = pipelineConfiguration.getDestinationConfig().createChangeEventSink();
        var topicStrategy = pipelineConfiguration.getKafkaConfig().getTopicStrategy();
        KafkaConsumerManager.createConsumers(
            bootstrapServer, topicPrefix, tables, eventProcessor, metricsService, topicStrategy);
      }

      metricsService.setPipelineStatus(PipelineStatus.SNAPSHOTTING);
      snapshotter.snapshot(tables);

      metricsService.setPipelineStatus(PipelineStatus.STREAMING);
      streamer.stream();
    } catch (Exception e) {
      log.error("Pipeline had stopped unexpectedly.", e);
      close();
    } finally {
      metricsService.setPipelineStatus(PipelineStatus.NOT_RUNNING);
    }
  }
}
