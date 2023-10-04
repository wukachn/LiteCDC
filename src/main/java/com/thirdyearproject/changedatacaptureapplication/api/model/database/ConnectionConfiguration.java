package com.thirdyearproject.changedatacaptureapplication.api.model.database;

import java.util.Properties;

public interface ConnectionConfiguration {
  String getJdbcUrl();

  Properties getBasicJdbcProperties();
}
