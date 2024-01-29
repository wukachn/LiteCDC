package com.thirdyearproject.changedatacaptureapplication.engine.change.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

@Value
@Builder
@Jacksonized
public class ChangeEvent implements Comparable<ChangeEvent> {
  @NonNull Metadata metadata;
  @Nullable List<ColumnWithData> before;
  @Nullable List<ColumnWithData> after;

  @Override
  public int compareTo(ChangeEvent otherEvent) {
    return Long.compare(this.getMetadata().getOffset(), otherEvent.getMetadata().getOffset());
  }

  public List<ColumnDetails> getAfterColumnDetails() {
    List<ColumnDetails> columnDetails = new ArrayList<>();
    if (after == null) {
      return columnDetails;
    }
    for (var afterCol: after) {
      columnDetails.add(afterCol.getDetails());
    }
    return columnDetails;
  }
}
