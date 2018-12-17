package org.locationtech.geowave.core.store.operations;

import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;

public class DataIndexReaderParamsBuilder<T>
    extends BaseReaderParamsBuilder<T, DataIndexReaderParamsBuilder<T>> {

  protected byte[][] dataIds = null;
  protected short adapterId;

  public DataIndexReaderParamsBuilder(
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore) {
    super(adapterStore, internalAdapterStore);
  }

  @Override
  protected DataIndexReaderParamsBuilder<T> builder() {
    return this;
  }

  public DataIndexReaderParamsBuilder<T> dataIds(final byte[]... dataIds) {
    this.dataIds = dataIds;
    return builder();
  }

  public DataIndexReaderParamsBuilder<T> adapterId(final short adapterId) {
    this.adapterId = adapterId;
    return builder();
  }

  public DataIndexReaderParams build() {
    if (dataIds == null) {
      dataIds = new byte[0][];
    }
    return new DataIndexReaderParams(
        adapterStore,
        internalAdapterStore,
        adapterId,
        aggregation,
        fieldSubsets,
        dataIds,
        isAuthorizationsLimiting,
        additionalAuthorizations);
  }
}
