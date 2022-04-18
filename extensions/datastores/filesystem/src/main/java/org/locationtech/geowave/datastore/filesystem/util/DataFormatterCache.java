/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatterRegistry;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class DataFormatterCache {
  private static DataFormatterCache singletonInstance;

  public static synchronized DataFormatterCache getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new DataFormatterCache();
    }
    return singletonInstance;
  }

  private final LoadingCache<FormatterKey, FileSystemDataFormatter> formatterCache =
      Caffeine.newBuilder().build(formatterKey -> {
        return FileSystemDataFormatterRegistry.getDataFormatterRegistry().get(
            formatterKey.format).createFormatter(formatterKey.visibilityEnabled);
      });

  public FileSystemDataFormatter getFormatter(
      final String format,
      final boolean visibilityEnabled) {
    return formatterCache.get(new FormatterKey(format, visibilityEnabled));
  }

  private static class FormatterKey {
    private final String format;
    private final boolean visibilityEnabled;

    public FormatterKey(final String format, final boolean visibilityEnabled) {
      super();
      this.format = format;
      this.visibilityEnabled = visibilityEnabled;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((format == null) ? 0 : format.hashCode());
      result = (prime * result) + (visibilityEnabled ? 1231 : 1237);
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final FormatterKey other = (FormatterKey) obj;
      if (format == null) {
        if (other.format != null) {
          return false;
        }
      } else if (!format.equals(other.format)) {
        return false;
      }
      if (visibilityEnabled != other.visibilityEnabled) {
        return false;
      }
      return true;
    }
  }
}
