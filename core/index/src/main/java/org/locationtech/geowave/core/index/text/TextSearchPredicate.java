/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.text;

import org.locationtech.geowave.core.index.CustomIndexStrategy.PersistableBiPredicate;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TextSearchPredicate<E> implements PersistableBiPredicate<E, TextSearch> {
  private TextIndexEntryConverter<E> converter;
  private String cachedSearchTerm;
  private String cachedLowerCaseTerm;

  public TextSearchPredicate() {}

  public TextSearchPredicate(final TextIndexEntryConverter<E> converter) {
    this.converter = converter;
  }

  @Override
  public boolean test(final E t, final TextSearch u) {
    final String value = converter.apply(t);
    final boolean caseSensitive = CaseSensitivity.CASE_SENSITIVE.equals(u.getCaseSensitivity());
    return u.getType().evaluate(
        ((value != null) && !caseSensitive) ? value.toLowerCase() : value,
        caseSensitive ? u.getSearchTerm() : getLowerCaseTerm(u.getSearchTerm()));
  }

  @SuppressFBWarnings(
      value = {"ES_COMPARING_PARAMETER_STRING_WITH_EQ"},
      justification = "this is actually intentional; comparing instance of a string")
  private String getLowerCaseTerm(final String term) {
    // because under normal conditions its always the same search term per instance of the
    // predicate, let's just make sure we perform toLowerCase one time instead of repeatedly for
    // each evaluation
    if ((cachedSearchTerm == null) || (cachedLowerCaseTerm == null)) {
      synchronized (this) {
        cachedSearchTerm = term;
        cachedLowerCaseTerm = term.toLowerCase();
      }
    }
    // intentionally using == because this should be the same instance of the term
    else if (term == cachedSearchTerm) {
      return cachedLowerCaseTerm;
    }
    return term.toLowerCase();
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(converter);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    converter = (TextIndexEntryConverter<E>) PersistenceUtils.fromBinary(bytes);
  }

}
