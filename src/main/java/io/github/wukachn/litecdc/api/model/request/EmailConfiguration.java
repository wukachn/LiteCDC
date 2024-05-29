package io.github.wukachn.litecdc.api.model.request;

import io.github.wukachn.litecdc.engine.EmailHandler;
import io.github.wukachn.litecdc.util.EnvironmentVariableHandler;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@Builder
public class EmailConfiguration {
  @NonNull String senderEmail;
  @NonNull String senderPassword;
  @NonNull List<String> recipients;

  public EmailHandler getEmailHandler() {
    return new EmailHandler(senderEmail, senderPassword, recipients);
  }

  public void validate() {
    EnvironmentVariableHandler.get(senderPassword);
  }
}
