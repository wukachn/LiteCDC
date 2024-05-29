package io.github.wukachn.litecdc.engine.change.model;

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
    var theseParts = this.getMetadata().getOffset().split("~");
    var otherParts = otherEvent.getMetadata().getOffset().split("~");
    for (var i = 1; i < theseParts.length; i++) {
      var result = Long.compare(Long.valueOf(theseParts[i]), Long.valueOf(otherParts[i]));
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  public List<ColumnDetails> getAfterColumnDetails() {
    List<ColumnDetails> columnDetails = new ArrayList<>();
    if (after == null) {
      return columnDetails;
    }
    for (var afterCol : after) {
      columnDetails.add(afterCol.getDetails());
    }
    return columnDetails;
  }
}
