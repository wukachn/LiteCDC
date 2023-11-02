package com.thirdyearproject.changedatacaptureapplication;

import com.thirdyearproject.changedatacaptureapplication.engine.change.ChangeEventProducer;
import com.thirdyearproject.changedatacaptureapplication.engine.change.KafkaProducerService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ChangeDataCaptureApplication {

  public static void main(String[] args) {
    SpringApplication.run(ChangeDataCaptureApplication.class, args);
  }

  @Bean
  ChangeEventProducer changeEventProducer(KafkaProducerService kafkaProducerService) {
    return new ChangeEventProducer(kafkaProducerService);
  }
}
