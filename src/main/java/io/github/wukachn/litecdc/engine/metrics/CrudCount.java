package io.github.wukachn.litecdc.engine.metrics;

import io.github.wukachn.litecdc.engine.change.model.CRUD;
import lombok.Builder;
import lombok.Getter;

@Builder
public class CrudCount {
  @Getter private long create;
  @Getter private long read;
  @Getter private long update;
  @Getter private long delete;

  public void incrementOperation(CRUD op) {
    switch (op) {
      case CREATE -> this.create = this.create + 1;
      case READ -> this.read = this.read + 1;
      case UPDATE -> this.update = this.update + 1;
      case DELETE -> this.delete = this.delete + 1;
    }
  }
}
