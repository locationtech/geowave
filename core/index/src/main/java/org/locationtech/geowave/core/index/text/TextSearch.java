/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import com.google.common.primitives.Bytes;

public class TextSearch implements TextConstraints {
  private TextSearchType type;
  private CaseSensitivity caseSensitivity;
  private String searchTerm;

  public TextSearch() {}

  public TextSearch(
      final TextSearchType type,
      final CaseSensitivity caseSensitivity,
      final String searchTerm) {
    this.type = type;
    this.caseSensitivity = caseSensitivity;
    this.searchTerm = searchTerm;
  }

  public TextSearchType getType() {
    return type;
  }

  public String getSearchTerm() {
    return searchTerm;
  }

  public CaseSensitivity getCaseSensitivity() {
    return caseSensitivity;
  }

  @Override
  public byte[] toBinary() {
    return Bytes.concat(
        VarintUtils.writeUnsignedInt(type.ordinal()),
        VarintUtils.writeUnsignedInt(caseSensitivity.ordinal()),
        StringUtils.stringToBinary(searchTerm));
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    type = TextSearchType.values()[VarintUtils.readUnsignedInt(buf)];
    caseSensitivity = CaseSensitivity.values()[VarintUtils.readUnsignedInt(buf)];
    final byte[] searchTermBytes = new byte[buf.remaining()];
    buf.get(searchTermBytes);
    searchTerm = StringUtils.stringFromBinary(searchTermBytes);
  }

  @Override
  public QueryRanges getQueryRanges(
      final EnumSet<TextSearchType> supportedSearchTypes,
      final int nCharacterGrams) {
    return TextIndexUtils.getQueryRanges(
        searchTerm,
        type,
        caseSensitivity,
        supportedSearchTypes,
        nCharacterGrams);
  }
}
