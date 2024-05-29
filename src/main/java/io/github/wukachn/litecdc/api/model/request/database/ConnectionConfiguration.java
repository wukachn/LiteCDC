package io.github.wukachn.litecdc.api.model.request.database;

import java.util.Properties;

public interface ConnectionConfiguration {
  String getJdbcUrl();

  Properties getJdbcProperties();
}
