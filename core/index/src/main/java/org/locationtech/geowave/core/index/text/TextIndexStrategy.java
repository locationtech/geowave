/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

public class TextIndexStrategy<E> implements CustomIndexStrategy<E, TextSearch> {
  private EnumSet<TextSearchType> supportedSearchTypes;
  private EnumSet<CaseSensitivity> supportedCaseSensitivity;
  private TextIndexEntryConverter<E> converter;
  private int nCharacterGrams;

  public TextIndexStrategy() {}

  public TextIndexStrategy(final TextIndexEntryConverter<E> converter) {
    this(EnumSet.allOf(TextSearchType.class), EnumSet.allOf(CaseSensitivity.class), converter);
  }

  public TextIndexStrategy(
      final EnumSet<TextSearchType> supportedSearchTypes,
      final EnumSet<CaseSensitivity> caseSensitivity,
      final TextIndexEntryConverter<E> converter) {
    this(supportedSearchTypes, caseSensitivity, 3, converter);
  }

  public TextIndexStrategy(
      final EnumSet<TextSearchType> supportedSearchTypes,
      final EnumSet<CaseSensitivity> supportedCaseSensitivity,
      final int nCharacterGrams,
      final TextIndexEntryConverter<E> converter) {
    super();
    this.supportedSearchTypes = supportedSearchTypes;
    this.supportedCaseSensitivity = supportedCaseSensitivity;
    this.nCharacterGrams = nCharacterGrams;
    this.converter = converter;
  }

  @Override
  public byte[] toBinary() {
    final int encodedType = encodeType(supportedSearchTypes);
    final int encodedCase = encodeCaseSensitivity(supportedCaseSensitivity);

    final byte[] converterBytes = PersistenceUtils.toBinary(converter);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(encodedType)
                + VarintUtils.unsignedIntByteLength(encodedCase)
                + VarintUtils.unsignedIntByteLength(nCharacterGrams)
                + converterBytes.length);
    VarintUtils.writeUnsignedInt(encodedType, buf);
    VarintUtils.writeUnsignedInt(encodedCase, buf);
    VarintUtils.writeUnsignedInt(nCharacterGrams, buf);
    buf.put(converterBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    fromBinary(ByteBuffer.wrap(bytes));
  }

  protected void fromBinary(final ByteBuffer buf) {
    supportedSearchTypes = decodeType(VarintUtils.readUnsignedInt(buf));
    supportedCaseSensitivity = decodeCaseSensitivity(VarintUtils.readUnsignedInt(buf));
    nCharacterGrams = VarintUtils.readUnsignedInt(buf);
    final byte[] converterBytes = new byte[buf.remaining()];
    buf.get(converterBytes);
    converter = (TextIndexEntryConverter<E>) PersistenceUtils.fromBinary(converterBytes);
  }

  @Override
  public InsertionIds getInsertionIds(final E entry) {
    return TextIndexUtils.getInsertionIds(
        entryToString(entry),
        supportedSearchTypes,
        supportedCaseSensitivity,
        nCharacterGrams);
  }

  @Override
  public QueryRanges getQueryRanges(final TextSearch constraints) {
    return TextIndexUtils.getQueryRanges(
        constraints.getSearchTerm(),
        constraints.getType(),
        constraints.getCaseSensitivity(),
        supportedSearchTypes,
        nCharacterGrams);
  }

  @Override
  public PersistableBiPredicate<E, TextSearch> getFilter(final TextSearch constraints) {
    if (constraints.getType().requiresEvaluate()) {
      return new TextSearchPredicate(converter);
    }
    return CustomIndexStrategy.super.getFilter(constraints);
  }

  protected String entryToString(final E entry) {
    return converter.apply(entry);
  }

  private static int encodeType(final EnumSet<TextSearchType> set) {
    int ret = 0;

    for (final TextSearchType val : set) {
      ret |= 1 << val.ordinal();
    }

    return ret;
  }

  private static EnumSet<TextSearchType> decodeType(int code) {
    final TextSearchType[] values = TextSearchType.values();
    final EnumSet<TextSearchType> result = EnumSet.noneOf(TextSearchType.class);
    while (code != 0) {
      final int ordinal = Integer.numberOfTrailingZeros(code);
      code ^= Integer.lowestOneBit(code);
      result.add(values[ordinal]);
    }
    return result;
  }

  private static int encodeCaseSensitivity(final EnumSet<CaseSensitivity> set) {
    int ret = 0;

    for (final CaseSensitivity val : set) {
      ret |= 1 << val.ordinal();
    }

    return ret;
  }

  private static EnumSet<CaseSensitivity> decodeCaseSensitivity(int code) {
    final CaseSensitivity[] values = CaseSensitivity.values();
    final EnumSet<CaseSensitivity> result = EnumSet.noneOf(CaseSensitivity.class);
    while (code != 0) {
      final int ordinal = Integer.numberOfTrailingZeros(code);
      code ^= Integer.lowestOneBit(code);
      result.add(values[ordinal]);
    }
    return result;
  }
}
