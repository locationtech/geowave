/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;

public class EnumSearch implements Persistable {

  private String searchTerm;

  public EnumSearch() {}

  public EnumSearch(final String searchTerm) {
    this.searchTerm = searchTerm;
  }

  public String getSearchTerm() {
    return searchTerm;
  }

  @Override
  public byte[] toBinary() {
    return StringUtils.stringToBinary(searchTerm);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    searchTerm = StringUtils.stringFromBinary(bytes);
  }
}
