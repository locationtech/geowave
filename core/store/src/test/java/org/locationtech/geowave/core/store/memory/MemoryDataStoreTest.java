/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.adapter.MockComponents;
import org.locationtech.geowave.core.store.adapter.MockComponents.MockAbstractDataAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic.NumericRangeValue;
import com.clearspring.analytics.util.Lists;

public class MemoryDataStoreTest {

  @Test
  public void test() throws IOException {
    final Index index =
        new IndexImpl(new MockComponents.MockIndexStrategy(), new MockComponents.TestIndexModel());
    final String namespace = "test_" + getClass().getName();
    final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
    final MemoryRequiredOptions reqOptions = new MemoryRequiredOptions();
    reqOptions.setGeoWaveNamespace(namespace);
    final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(reqOptions);
    final DataStatisticsStore statsStore =
        storeFamily.getDataStatisticsStoreFactory().createStore(reqOptions);
    final DataTypeAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

    final VisibilityHandler visHandler = new GlobalVisibilityHandler("aaa&bbb");
    final List<Statistic<?>> statistics = Lists.newArrayList();
    statistics.add(new CountStatistic(adapter.getTypeName()));
    statistics.add(
        new NumericRangeStatistic(adapter.getTypeName(), MockAbstractDataAdapter.INTEGER));
    dataStore.addType(adapter, statistics, index);
    try (final Writer<Integer> indexWriter = dataStore.createWriter(adapter.getTypeName())) {

      indexWriter.write(new Integer(25), visHandler);
      indexWriter.flush();

      indexWriter.write(new Integer(35), visHandler);
      indexWriter.flush();
    }

    // authorization check
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).addAuthorization("aaa").constraints(
                    new TestQuery(23, 26)).build())) {
      assertFalse(itemIt.hasNext());
    }

    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 26)).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(25), itemIt.next());
      assertFalse(itemIt.hasNext());
    }
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 36)).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(25), itemIt.next());
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(35), itemIt.next());
      assertFalse(itemIt.hasNext());
    }

    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statsIt =
        statsStore.getAllStatistics(null)) {
      try (CloseableIterator<? extends StatisticValue<?>> statisticValues =
          statsStore.getStatisticValues(statsIt, null, "aaa", "bbb")) {
        assertTrue(checkStats(statisticValues, 2, new NumericRange(25, 35)));
      }
    }
    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statsIt =
        statsStore.getAllStatistics(null)) {
      try (CloseableIterator<? extends StatisticValue<?>> statisticValues =
          statsStore.getStatisticValues(statsIt, null)) {
        assertTrue(checkStats(statisticValues, 0, null));
      }
    }
    dataStore.delete(
        QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
            index.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                new TestQuery(23, 26)).build());
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 36)).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(35), itemIt.next());
      assertFalse(itemIt.hasNext());
    }
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 26)).build())) {
      assertFalse(itemIt.hasNext());
    }
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new DataIdQuery(adapter.getDataId(new Integer(35)))).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(35), itemIt.next());
    }
  }

  @Test
  public void testMultipleIndices() throws IOException {
    final Index index1 =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("tm1"));
    final Index index2 =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("tm2"));
    final String namespace = "test2_" + getClass().getName();
    final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
    final MemoryRequiredOptions opts = new MemoryRequiredOptions();
    opts.setGeoWaveNamespace(namespace);
    final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(opts);
    final DataStatisticsStore statsStore =
        storeFamily.getDataStatisticsStoreFactory().createStore(opts);
    final DataTypeAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

    final VisibilityHandler visHandler = new GlobalVisibilityHandler("aaa&bbb");

    final List<Statistic<?>> statistics = Lists.newArrayList();
    statistics.add(
        new NumericRangeStatistic(adapter.getTypeName(), MockAbstractDataAdapter.INTEGER));

    dataStore.addType(adapter, statistics, index1, index2);
    try (final Writer<Integer> indexWriter = dataStore.createWriter(adapter.getTypeName())) {

      indexWriter.write(new Integer(25), visHandler);
      indexWriter.flush();

      indexWriter.write(new Integer(35), visHandler);
      indexWriter.flush();
    }

    // authorization check
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index2.getName()).addAuthorization("aaa").constraints(
                    new TestQuery(23, 26)).build())) {
      assertFalse(itemIt.hasNext());
    }

    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index1.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 26)).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(25), itemIt.next());
      assertFalse(itemIt.hasNext());
    }
    // pick an index
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).addAuthorization(
                "aaa").addAuthorization("bbb").constraints(new TestQuery(23, 36)).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(25), itemIt.next());
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(35), itemIt.next());
      assertFalse(itemIt.hasNext());
    }

    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statsIt =
        statsStore.getAllStatistics(null)) {
      try (CloseableIterator<? extends StatisticValue<?>> statisticValues =
          statsStore.getStatisticValues(statsIt, null, "aaa", "bbb")) {
        assertTrue(checkStats(statisticValues, 2, new NumericRange(25, 35)));
      }
    }
    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statsIt =
        statsStore.getAllStatistics(null)) {
      try (CloseableIterator<? extends StatisticValue<?>> statisticValues =
          statsStore.getStatisticValues(statsIt, null)) {
        assertTrue(checkStats(statisticValues, 0, null));
      }
    }

    dataStore.delete(
        QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).addAuthorization(
            "aaa").addAuthorization("bbb").constraints(new TestQuery(23, 26)).build());
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index1.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 36)).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(35), itemIt.next());
      assertFalse(itemIt.hasNext());
    }
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index2.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 36)).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(35), itemIt.next());
      assertFalse(itemIt.hasNext());
    }
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index1.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 26)).build())) {
      assertFalse(itemIt.hasNext());
    }
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index2.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new TestQuery(23, 26)).build())) {
      assertFalse(itemIt.hasNext());
    }
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index1.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new DataIdQuery(adapter.getDataId(new Integer(35)))).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(35), itemIt.next());
    }
    try (CloseableIterator<?> itemIt =
        dataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index2.getName()).addAuthorization("aaa").addAuthorization("bbb").constraints(
                    new DataIdQuery(adapter.getDataId(new Integer(35)))).build())) {
      assertTrue(itemIt.hasNext());
      assertEquals(new Integer(35), itemIt.next());
    }
  }

  private boolean checkStats(
      final Iterator<? extends StatisticValue<?>> statIt,
      final int count,
      final NumericRange range) {
    boolean countPassed = false;
    boolean rangePassed = false;
    while (statIt.hasNext()) {
      final StatisticValue<?> stat = statIt.next();
      if ((stat instanceof CountValue)) {
        countPassed = (((CountValue) stat).getValue() == count);
      } else if ((stat instanceof NumericRangeValue)) {
        rangePassed =
            range == null ? !((NumericRangeValue) stat).isSet()
                : ((((NumericRangeValue) stat).getMin() == range.getMin())
                    && (((NumericRangeValue) stat).getMax() == range.getMax()));
      }
    }
    return countPassed && rangePassed;
  }

  private class TestQueryFilter implements QueryFilter {
    final double min, max;

    public TestQueryFilter(final double min, final double max) {
      super();
      this.min = min;
      this.max = max;
    }

    @Override
    public boolean accept(
        final CommonIndexModel indexModel,
        final IndexedPersistenceEncoding<?> persistenceEncoding) {
      final double min =
          ((CommonIndexedPersistenceEncoding) persistenceEncoding).getNumericData(
              indexModel.getDimensions()).getDataPerDimension()[0].getMin();
      final double max =
          ((CommonIndexedPersistenceEncoding) persistenceEncoding).getNumericData(
              indexModel.getDimensions()).getDataPerDimension()[0].getMax();
      return !((this.max <= min) || (this.min > max));
    }

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}
  }

  private class TestQuery implements QueryConstraints {

    final double min, max;

    public TestQuery(final double min, final double max) {
      super();
      this.min = min;
      this.max = max;
    }

    @Override
    public List<QueryFilter> createFilters(final Index index) {
      return Arrays.asList((QueryFilter) new TestQueryFilter(min, max));
    }

    @Override
    public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
      return Collections.<MultiDimensionalNumericData>singletonList(
          new BasicNumericDataset(new NumericData[] {new NumericRange(min, max)}));
    }

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}
  }
}
