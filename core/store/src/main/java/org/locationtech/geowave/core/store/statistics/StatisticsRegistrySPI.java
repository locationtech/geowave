/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.function.Supplier;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;

/**
 * Base SPI for registered statistics. This class also serves as the persistable registry for those
 * statistics.
 */
public interface StatisticsRegistrySPI {

  /**
   * Return a set of registered statistics.
   * 
   * @return the registered statistics
   */
  RegisteredStatistic[] getRegisteredStatistics();

  /**
   * Return a set of registered binning strategies.
   * 
   * @return the registered binning strategies
   */
  RegisteredBinningStrategy[] getRegisteredBinningStrategies();

  /**
   * This class contains everything needed to register a statistic with GeoWave.
   */
  public static class RegisteredStatistic {
    private final StatisticType<StatisticValue<Object>> statType;
    private final Supplier<? extends Statistic<? extends StatisticValue<?>>> statisticConstructor;
    private final Supplier<? extends StatisticValue<?>> valueConstructor;
    private final short statisticPersistableId;
    private final short valuePersistableId;

    private Statistic<?> prototype = null;

    /**
     * @param statType the statistics type
     * @param statisticConstructor the statistic constructor
     * @param valueConstructor the statistic value constructor
     * @param statisticPersistableId the persistable id to use for the statistic
     * @param valuePersistableId the persistable id to use for the statistic value
     */
    @SuppressWarnings("unchecked")
    public RegisteredStatistic(
        final StatisticType<? extends StatisticValue<?>> statType,
        final Supplier<? extends Statistic<? extends StatisticValue<?>>> statisticConstructor,
        final Supplier<? extends StatisticValue<?>> valueConstructor,
        final short statisticPersistableId,
        final short valuePersistableId) {
      this.statType = (StatisticType<StatisticValue<Object>>) statType;
      this.statisticConstructor = statisticConstructor;
      this.valueConstructor = valueConstructor;
      this.statisticPersistableId = statisticPersistableId;
      this.valuePersistableId = valuePersistableId;
    }

    /**
     * @return the statistics type
     */
    public StatisticType<StatisticValue<Object>> getStatisticsType() {
      return statType;
    }

    /**
     * @return the options constructor
     */
    @SuppressWarnings("unchecked")
    public Supplier<Statistic<StatisticValue<Object>>> getStatisticConstructor() {
      return (Supplier<Statistic<StatisticValue<Object>>>) statisticConstructor;
    }

    /**
     * @return the options constructor
     */
    @SuppressWarnings("unchecked")
    public Supplier<StatisticValue<Object>> getValueConstructor() {
      return (Supplier<StatisticValue<Object>>) valueConstructor;
    }

    public boolean isDataTypeStatistic() {
      return statType instanceof DataTypeStatisticType;
    }

    public boolean isIndexStatistic() {
      return statType instanceof IndexStatisticType;
    }

    public boolean isFieldStatistic() {
      return statType instanceof FieldStatisticType;
    }

    public boolean isCompatibleWith(final Class<?> clazz) {
      if (prototype == null) {
        prototype = statisticConstructor.get();
      }
      return prototype.isCompatibleWith(clazz);
    }

    public short getStatisticPersistableId() {
      return statisticPersistableId;
    }

    public short getValuePersistableId() {
      return valuePersistableId;
    }
  }

  /**
   * This class contains everything needed to register a statistic binning strategy with GeoWave.
   */
  public static class RegisteredBinningStrategy {
    private final String strategyName;
    private final Supplier<? extends StatisticBinningStrategy> constructor;
    private final short persistableId;

    /**
     * @param strategyName the name of the binning strategy
     * @param constructor the constructor for the binning strategy
     * @param persistableId the persistable id of the binning strategy
     */
    public RegisteredBinningStrategy(
        final String strategyName,
        final Supplier<? extends StatisticBinningStrategy> constructor,
        final short persistableId) {
      this.strategyName = strategyName;
      this.constructor = constructor;
      this.persistableId = persistableId;
    }

    /**
     * @return the strategy name
     */
    public String getStrategyName() {
      return strategyName;
    }

    /**
     * @return the binning strategy constructor
     */
    @SuppressWarnings("unchecked")
    public Supplier<StatisticBinningStrategy> getConstructor() {
      return (Supplier<StatisticBinningStrategy>) constructor;
    }

    public short getPersistableId() {
      return persistableId;
    }
  }

}
