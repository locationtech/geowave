package org.locationtech.geowave.python;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.*;
import java.net.URL;

/**
 * Most of this is not necessary for the time-being. Currently, this exists to bridge unachievable
 * method sigs in python, primarily that addType takes var-args for Indices TODO: This is only a
 * partial implementation
 */

public class DataStoreInterfacer {

  public <T> void ingest(DataStore ds, URL url, Index... index)
      throws MismatchedIndexToAdapterMapping {
    ds.ingest(url, index);
  }

  public <T> void ingest(DataStore ds, URL url, IngestOptions<T> options, Index... index)
      throws MismatchedIndexToAdapterMapping {
    ds.ingest(url, options, index);
  }

  public <T> CloseableIterator<T> query(DataStore ds, final Query<T> query) {
    return ds.query(query);
  }

  public <P extends Persistable, R, T> R aggregate(
      DataStore ds,
      final AggregationQuery<P, R, T> query) {
    return ds.aggregate(query);
  }

  public Index[] getIndices(DataStore ds) {
    Index[] indices = ds.getIndices();
    System.out.println(indices.length);
    return indices;
  }

  public <T> void addType(DataStore ds, DataTypeAdapter<T> dataTypeAdapter, Index initialIndex) {
    ds.addType(dataTypeAdapter, initialIndex);
  }

}
