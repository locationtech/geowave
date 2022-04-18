/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.filesystem.util;

import com.google.common.primitives.UnsignedBytes;

interface FileSystemKey extends Comparable<FileSystemKey> {
  byte[] getSortOrderKey();

  String getFileName();

  @Override
  default int compareTo(final FileSystemKey o) {
    return UnsignedBytes.lexicographicalComparator().compare(
        getSortOrderKey(),
        o.getSortOrderKey());
  }
}
