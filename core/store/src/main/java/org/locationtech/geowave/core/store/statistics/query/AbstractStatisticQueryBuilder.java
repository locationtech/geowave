/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.query;

import java.util.Arrays;
import java.util.List;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import com.clearspring.analytics.util.Lists;

/**
 * Base statistic query builder implementation.
 */
@SuppressWarnings("unchecked")
public abstract class AbstractStatisticQueryBuilder<V extends StatisticValue<R>, R, B extends StatisticQueryBuilder<V, R, B>>
    implements
    StatisticQueryBuilder<V, R, B> {

  protected final StatisticType<V> statisticType;

  protected String tag = null;

  protected BinConstraints binConstraints = null;

  protected List<String> authorizations = Lists.newArrayList();

  public AbstractStatisticQueryBuilder(final StatisticType<V> statisticType) {
    this.statisticType = statisticType;
  }

  @Override
  public B binConstraints(final BinConstraints binConstraints) {
    this.binConstraints = binConstraints;
    return (B) this;
  }

  @Override
  public B tag(final String tag) {
    this.tag = tag;
    return (B) this;
  }

  @Override
  public B internal() {
    this.tag = Statistic.INTERNAL_TAG;
    return (B) this;
  }

  @Override
  public B addAuthorization(final String authorization) {
    authorizations.add(authorization);
    return (B) this;
  }

  @Override
  public B authorizations(final String[] authorizations) {
    if (authorizations != null) {
      this.authorizations = Arrays.asList(authorizations);
    } else {
      this.authorizations.clear();
    }
    return (B) this;
  }
}
