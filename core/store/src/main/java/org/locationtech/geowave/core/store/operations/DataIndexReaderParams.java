package org.locationtech.geowave.core.store.operations;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class DataIndexReaderParams extends BaseReaderParams<GeoWaveRow> {
  private final byte[][] dataIds;
  private final short adapterId;

  public DataIndexReaderParams(
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final short adapterId,
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
      final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
      final byte[][] dataIds,
      final boolean isAuthorizationsLimiting,
      final String[] additionalAuthorizations) {
    super(
        adapterStore,
        internalAdapterStore,
        aggregation,
        fieldSubsets,
        isAuthorizationsLimiting,
        additionalAuthorizations);
    this.dataIds = dataIds;
    this.adapterId = adapterId;
  }

  public byte[][] getDataIds() {
    return dataIds;
  }

  public short getAdapterId() {
    return adapterId;
  }

}
