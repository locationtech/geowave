package org.locationtech.geowave.core.store.statistics;

import org.locationtech.geowave.core.index.persist.PersistableRegistrySpi;

public class StatisticsPersistableRegistry implements PersistableRegistrySpi {

  @Override
  public PersistableIdAndConstructor[] getSupportedPersistables() {
    return StatisticsRegistry.instance().getPersistables();
  }

}
