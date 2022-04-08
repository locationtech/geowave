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
import java.util.Arrays;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnumIndexStrategy<E> implements CustomIndexStrategy<E, EnumSearch> {
  private static Logger LOGGER = LoggerFactory.getLogger(EnumIndexStrategy.class);
  private String[] exactMatchTerms;
  private TextIndexEntryConverter<E> converter;

  public EnumIndexStrategy() {}

  public EnumIndexStrategy(
      final TextIndexEntryConverter<E> converter,
      final String[] exactMatchTerms) {
    super();
    this.converter = converter;
    Arrays.sort(exactMatchTerms);
    this.exactMatchTerms = exactMatchTerms;
  }

  @Override
  public Class<EnumSearch> getConstraintsClass() {
    return EnumSearch.class;
  }

  @Override
  public byte[] toBinary() {
    final byte[] converterBytes = PersistenceUtils.toBinary(converter);
    final byte[] termsBytes = StringUtils.stringsToBinary(exactMatchTerms);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(termsBytes.length)
                + termsBytes.length
                + converterBytes.length);
    VarintUtils.writeUnsignedInt(termsBytes.length, buf);
    buf.put(termsBytes);
    buf.put(converterBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    fromBinary(ByteBuffer.wrap(bytes));
  }

  protected void fromBinary(final ByteBuffer buf) {
    final byte[] termsBytes = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(termsBytes);
    exactMatchTerms = StringUtils.stringsFromBinary(termsBytes);
    final byte[] converterBytes = new byte[buf.remaining()];
    buf.get(converterBytes);
    converter = (TextIndexEntryConverter<E>) PersistenceUtils.fromBinary(converterBytes);
  }

  @Override
  public InsertionIds getInsertionIds(final E entry) {
    final String str = entryToString(entry);
    if (str == null) {
      LOGGER.warn("Cannot index null enum, skipping entry");
      return new InsertionIds();
    }
    final int index = Arrays.binarySearch(exactMatchTerms, str);
    if (index < 0) {
      LOGGER.warn("Enumerated value not found for insertion '" + str + "'");
      return new InsertionIds();
    }
    return new InsertionIds(
        new SinglePartitionInsertionIds(null, VarintUtils.writeUnsignedInt(index)));
  }

  @Override
  public QueryRanges getQueryRanges(final EnumSearch constraints) {
    final int index = Arrays.binarySearch(exactMatchTerms, constraints.getSearchTerm());
    final byte[] sortKey = VarintUtils.writeUnsignedInt(index);
    if (index < 0) {
      LOGGER.warn("Enumerated value not found for search '" + constraints.getSearchTerm() + "'");
      // the sort key shouldn't match so let's pass through (alternatives to giving an unused sort
      // key such as null or empty queries result in all rows)
    }
    return new QueryRanges(new ByteArrayRange(sortKey, sortKey));
  }

  protected String entryToString(final E entry) {
    return converter.apply(entry);
  }
}
