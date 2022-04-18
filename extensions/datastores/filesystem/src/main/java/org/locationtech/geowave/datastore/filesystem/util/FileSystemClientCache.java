/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

public class FileSystemClientCache {
  private static FileSystemClientCache singletonInstance;

  public static synchronized FileSystemClientCache getInstance() {
    if (singletonInstance == null) {
      singletonInstance = new FileSystemClientCache();
    }
    return singletonInstance;
  }

  private final LoadingCache<ClientKey, FileSystemClient> clientCache =
      Caffeine.newBuilder().build(clientInfo -> {
        return new FileSystemClient(
            clientInfo.directory,
            clientInfo.format,
            clientInfo.visibilityEnabled);
      });

  protected FileSystemClientCache() {}

  public FileSystemClient getClient(
      final String directory,
      final String format,
      final boolean visibilityEnabled) {
    return clientCache.get(new ClientKey(directory, format, visibilityEnabled));
  }

  public synchronized void close(
      final String directory,
      final String format,
      final boolean visibilityEnabled) {
    final ClientKey key = new ClientKey(directory, format, visibilityEnabled);
    final FileSystemClient client = clientCache.getIfPresent(key);
    if (client != null) {
      clientCache.invalidate(key);
    }
  }

  public synchronized void closeAll() {
    clientCache.invalidateAll();
  }

  private static class ClientKey {
    private final String directory;
    private final String format;
    private final boolean visibilityEnabled;

    public ClientKey(final String directory, final String format, final boolean visibilityEnabled) {
      super();
      this.directory = directory;
      this.format = format;
      this.visibilityEnabled = visibilityEnabled;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((directory == null) ? 0 : directory.hashCode());
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
      final ClientKey other = (ClientKey) obj;
      if (directory == null) {
        if (other.directory != null) {
          return false;
        }
      } else if (!directory.equals(other.directory)) {
        return false;
      }
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
