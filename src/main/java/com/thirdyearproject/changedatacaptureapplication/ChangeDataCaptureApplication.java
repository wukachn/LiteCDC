package com.thirdyearproject.changedatacaptureapplication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class ChangeDataCaptureApplication {

  public static void main(String[] args) {
    SpringApplication.run(ChangeDataCaptureApplication.class, args);
  }
}
