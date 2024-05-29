package io.github.wukachn.litecdc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class LiteCDC {

  public static void main(String[] args) {
    SpringApplication.run(LiteCDC.class, args);
  }
}
