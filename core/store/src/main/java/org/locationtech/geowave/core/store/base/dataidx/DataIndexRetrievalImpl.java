package org.locationtech.geowave.core.store.base.dataidx;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;

public class DataIndexRetrievalImpl implements DataIndexRetrieval {

  private final DataStoreOperations operations;
  private final PersistentAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;
  private final Pair<String[], InternalDataAdapter<?>> fieldSubsets;
  private final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
  private final String[] additionalAuthorizations;


  public DataIndexRetrievalImpl(
      final DataStoreOperations operations,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final String[] additionalAuthorizations) {
    this.operations = operations;
    this.adapterStore = adapterStore;
    this.internalAdapterStore = internalAdapterStore;
    this.fieldSubsets = fieldSubsets;
    this.aggregation = aggregation;
    this.additionalAuthorizations = additionalAuthorizations;
  }

  @Override
  public GeoWaveValue[] getData(final short adapterId, final byte[] dataId) {
    return DataIndexUtils.getFieldValuesFromDataIdIndex(
        operations,
        adapterStore,
        internalAdapterStore,
        fieldSubsets,
        aggregation,
        additionalAuthorizations,
        adapterId,
        dataId);
  }
}
