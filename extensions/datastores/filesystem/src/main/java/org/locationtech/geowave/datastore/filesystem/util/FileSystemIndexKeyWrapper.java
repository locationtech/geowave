/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import java.util.Arrays;
import org.locationtech.geowave.datastore.filesystem.FileSystemDataFormatter.FileSystemIndexKey;

class FileSystemIndexKeyWrapper implements FileSystemKey {
  private final byte[] sortOrderKey;
  private final FileSystemIndexKey key;
  private final String fileName;

  public FileSystemIndexKeyWrapper(final FileSystemIndexKey key, final String fileName) {
    super();
    sortOrderKey = key.getSortOrderKey();
    this.key = key;
    this.fileName = fileName;
  }

  @Override
  public byte[] getSortOrderKey() {
    return sortOrderKey;
  }

  @Override
  public String getFileName() {
    return fileName;
  }

  public FileSystemIndexKey getOriginalKey() {
    return key;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((fileName == null) ? 0 : fileName.hashCode());
    result = (prime * result) + Arrays.hashCode(sortOrderKey);
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
    final FileSystemIndexKeyWrapper other = (FileSystemIndexKeyWrapper) obj;
    if (fileName == null) {
      if (other.fileName != null) {
        return false;
      }
    } else if (!fileName.equals(other.fileName)) {
      return false;
    }
    if (!Arrays.equals(sortOrderKey, other.sortOrderKey)) {
      return false;
    }
    return true;
  }

}
