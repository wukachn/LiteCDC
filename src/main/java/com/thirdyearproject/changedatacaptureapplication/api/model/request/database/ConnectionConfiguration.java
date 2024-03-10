package com.thirdyearproject.changedatacaptureapplication.api.model.request.database;

import java.util.Properties;

public interface ConnectionConfiguration {
  String getJdbcUrl();

  Properties getJdbcProperties();
}
