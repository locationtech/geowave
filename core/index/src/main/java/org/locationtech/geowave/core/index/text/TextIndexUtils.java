/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.index.StringUtils;

public class TextIndexUtils {
  protected static BiPredicate<String, String> ALWAYS_TRUE = (term, value) -> true;
  private static final byte[] FORWARD_INDEX_CASE_SENSITIVE_PARTITION_KEY = new byte[] {0};
  private static final byte[] REVERSE_INDEX_CASE_SENSITIVE_PARTITION_KEY = new byte[] {1};
  private static final byte[] NGRAM_INDEX_CASE_SENSITIVE_PARTITION_KEY = new byte[] {2};
  private static final byte[] FORWARD_INDEX_CASE_INSENSITIVE_PARTITION_KEY = new byte[] {3};
  private static final byte[] REVERSE_INDEX_CASE_INSENSITIVE_PARTITION_KEY = new byte[] {4};
  private static final byte[] NGRAM_INDEX_CASE_INSENSITIVE_PARTITION_KEY = new byte[] {5};

  public static InsertionIds getInsertionIds(
      final String entry,
      final EnumSet<TextSearchType> supportedSearchTypes,
      final EnumSet<CaseSensitivity> supportedCaseSensitivities,
      final int nGramCharacters) {
    if ((entry == null) || entry.isEmpty()) {
      return new InsertionIds();
    }
    final Set<TextIndexType> indexTypes =
        supportedSearchTypes.stream().map(TextSearchType::getIndexType).collect(Collectors.toSet());
    final List<SinglePartitionInsertionIds> retVal = new ArrayList<>(indexTypes.size());
    for (final TextIndexType indexType : indexTypes) {
      for (final CaseSensitivity caseSensitivity : supportedCaseSensitivities) {
        final boolean caseSensitive = CaseSensitivity.CASE_SENSITIVE.equals(caseSensitivity);
        switch (indexType) {
          case FORWARD:
            retVal.add(getForwardInsertionIds(entry, caseSensitive));
            break;
          case REVERSE:
            retVal.add(getReverseInsertionIds(entry, caseSensitive));
            break;
          case NGRAM:
            final SinglePartitionInsertionIds i =
                getNGramInsertionIds(
                    entry,
                    nGramCharacters,
                    indexTypes.contains(TextIndexType.FORWARD),
                    caseSensitive);
            if (i != null) {
              retVal.add(i);
            }
            break;
        }
      }
    }
    return new InsertionIds(retVal);
  }

  public static QueryRanges getQueryRanges(
      final String term,
      final TextSearchType searchType,
      final CaseSensitivity caseSensitivity,
      final EnumSet<TextSearchType> supportedSearchTypes,
      final int nGramCharacters) {
    final Set<TextIndexType> indexTypes =
        supportedSearchTypes.stream().map(TextSearchType::getIndexType).collect(Collectors.toSet());

    final boolean caseSensitive = CaseSensitivity.CASE_SENSITIVE.equals(caseSensitivity);
    switch (searchType.getIndexType()) {
      case FORWARD:
        return getForwardQueryRanges(term, caseSensitive);
      case REVERSE:
        return getReverseQueryRanges(term, caseSensitive);
      case NGRAM:
      default:
        return getNGramQueryRanges(
            term,
            nGramCharacters,
            indexTypes.contains(TextIndexType.FORWARD),
            caseSensitive);
    }
  }

  private static SinglePartitionInsertionIds getForwardInsertionIds(
      final String entry,
      final boolean caseSensitive) {
    return getForwardInsertionIds(
        caseSensitive ? entry : entry.toLowerCase(),
        caseSensitive ? FORWARD_INDEX_CASE_SENSITIVE_PARTITION_KEY
            : FORWARD_INDEX_CASE_INSENSITIVE_PARTITION_KEY);
  }

  private static SinglePartitionInsertionIds getForwardInsertionIds(
      final String entry,
      final byte[] partitionKey) {
    return new SinglePartitionInsertionIds(partitionKey, StringUtils.stringToBinary(entry));
  }

  private static SinglePartitionInsertionIds getReverseInsertionIds(
      final String entry,
      final boolean caseSensitive) {
    return getReverseInsertionIds(
        caseSensitive ? entry : entry.toLowerCase(),
        caseSensitive ? REVERSE_INDEX_CASE_SENSITIVE_PARTITION_KEY
            : REVERSE_INDEX_CASE_INSENSITIVE_PARTITION_KEY);
  }

  private static SinglePartitionInsertionIds getReverseInsertionIds(
      final String entry,
      final byte[] partitionKey) {
    return new SinglePartitionInsertionIds(
        partitionKey,
        StringUtils.stringToBinary(new StringBuilder(entry).reverse().toString()));
  }

  private static SinglePartitionInsertionIds getNGramInsertionIds(
      final String entry,
      final int nGramCharacters,
      final boolean isForwardIndexed,
      final boolean caseSensitive) {
    return getNGramInsertionIds(
        caseSensitive ? entry : entry.toLowerCase(),
        nGramCharacters,
        isForwardIndexed,
        caseSensitive ? NGRAM_INDEX_CASE_SENSITIVE_PARTITION_KEY
            : NGRAM_INDEX_CASE_INSENSITIVE_PARTITION_KEY);
  }

  private static SinglePartitionInsertionIds getNGramInsertionIds(
      final String entry,
      final int nGramCharacters,
      final boolean isForwardIndexed,
      final byte[] partitionKey) {
    final int startIndex = (isForwardIndexed ? 1 : 0);
    final int endIndex = entry.length() - nGramCharacters;
    final int numNGrams = (endIndex - startIndex) + 1;
    if (numNGrams >= 0) {
      final List<byte[]> sortKeys = new ArrayList<>(numNGrams);
      for (int i = startIndex; i <= endIndex; i++) {
        sortKeys.add(StringUtils.stringToBinary(entry.substring(i, i + nGramCharacters)));
      }
      return new SinglePartitionInsertionIds(partitionKey, sortKeys);
    }
    return null;
  }

  public static QueryRanges getForwardQueryRanges(final String term, final boolean caseSensitive) {
    final byte[] forwardTermBytes =
        StringUtils.stringToBinary(caseSensitive ? term : term.toLowerCase());
    final List<SinglePartitionQueryRanges> retVal = new ArrayList<>(1);
    retVal.add(
        new SinglePartitionQueryRanges(
            caseSensitive ? FORWARD_INDEX_CASE_SENSITIVE_PARTITION_KEY
                : FORWARD_INDEX_CASE_INSENSITIVE_PARTITION_KEY,
            Collections.singletonList(new ByteArrayRange(forwardTermBytes, forwardTermBytes))));
    return new QueryRanges(retVal);
  }

  public static QueryRanges getReverseQueryRanges(final String term, final boolean caseSensitive) {
    final byte[] reverseTermBytes =
        StringUtils.stringToBinary(
            new StringBuilder(caseSensitive ? term : term.toLowerCase()).reverse().toString());
    final List<SinglePartitionQueryRanges> retVal = new ArrayList<>(1);
    retVal.add(
        new SinglePartitionQueryRanges(
            caseSensitive ? REVERSE_INDEX_CASE_SENSITIVE_PARTITION_KEY
                : REVERSE_INDEX_CASE_INSENSITIVE_PARTITION_KEY,
            Collections.singletonList(new ByteArrayRange(reverseTermBytes, reverseTermBytes))));
    return new QueryRanges(retVal);
  }

  public static QueryRanges getNGramQueryRanges(
      final String initialTerm,
      final int nGramCharacters,
      final boolean isForwardIndexed,
      final boolean caseSensitive) {
    final String term = caseSensitive ? initialTerm : initialTerm.toLowerCase();
    final boolean shouldTruncateNGram = term.length() > nGramCharacters;
    final byte[] nGramTermBytes =
        StringUtils.stringToBinary(shouldTruncateNGram ? term.substring(0, nGramCharacters) : term);
    final List<SinglePartitionQueryRanges> retVal = new ArrayList<>(1 + (isForwardIndexed ? 1 : 0));
    final SinglePartitionQueryRanges ngramRange =
        new SinglePartitionQueryRanges(
            caseSensitive ? NGRAM_INDEX_CASE_SENSITIVE_PARTITION_KEY
                : NGRAM_INDEX_CASE_INSENSITIVE_PARTITION_KEY,
            Collections.singletonList(new ByteArrayRange(nGramTermBytes, nGramTermBytes)));
    retVal.add(ngramRange);
    if (isForwardIndexed) {
      final byte[] forwardTermBytes =
          shouldTruncateNGram ? StringUtils.stringToBinary(term) : nGramTermBytes;
      retVal.add(
          new SinglePartitionQueryRanges(
              caseSensitive ? FORWARD_INDEX_CASE_SENSITIVE_PARTITION_KEY
                  : FORWARD_INDEX_CASE_INSENSITIVE_PARTITION_KEY,
              Collections.singletonList(new ByteArrayRange(forwardTermBytes, forwardTermBytes))));
    }
    return new QueryRanges(retVal);
  }
}
