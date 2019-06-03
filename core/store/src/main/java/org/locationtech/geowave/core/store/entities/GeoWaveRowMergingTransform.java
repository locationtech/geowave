package org.locationtech.geowave.core.store.entities;

import java.io.IOException;
import java.util.Iterator;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;

public class GeoWaveRowMergingTransform implements GeoWaveRowIteratorTransformer<GeoWaveRow> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveRowMergingTransform.class);
  private final RowTransform<?> rowTransform;

  public GeoWaveRowMergingTransform(
      final RowMergingDataAdapter<?, ?> adapter,
      final short internalAdapterId) {
    super();
    rowTransform = adapter.getTransform();
    try {
      rowTransform.initOptions(adapter.getOptions(internalAdapterId, null));
    } catch (final IOException e) {
      LOGGER.warn("Unable to initialize row merging adapter for type: " + adapter.getTypeName(), e);
    }
  }

  @Override
  public Iterator<GeoWaveRow> apply(final Iterator<GeoWaveRow> input) {
    if (input != null) {
      return Iterators.transform(input, row -> {
        return DataStoreUtils.mergeSingleRowValues(row, rowTransform);
      });
    }
    return null;
  }
}
