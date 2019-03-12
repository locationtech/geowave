package org.locationtech.geowave.datastore.kudu.operations;

import java.util.Collection;
import org.apache.kudu.client.KuduException;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.kudu.operations.KuduOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Sets;

public class KuduReader<T> implements RowReader<T> {
  private final ReaderParams<T> readerParams;
  private final RecordReaderParams recordReaderParams;
  private final KuduOperations operations;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private CloseableIterator<T> iterator;
  private final boolean visibilityEnabled;
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduReader.class);

  public KuduReader(
      final ReaderParams<T> readerParams,
      final KuduOperations operations,
      final boolean visibilityEnabled) {
    this.readerParams = readerParams;
    recordReaderParams = null;
    this.operations = operations;
    this.rowTransformer = readerParams.getRowTransformer();
    this.visibilityEnabled = visibilityEnabled;

    initScanner();
  }

  @SuppressWarnings("unchecked")
  protected void initScanner() {
    final Collection<SinglePartitionQueryRanges> ranges =
        readerParams.getQueryRanges().getPartitionQueryRanges();
    try {
      iterator =
          operations.getKuduRangeRead(
              readerParams.getIndex().getName(),
              readerParams.getAdapterIds(),
              ranges,
              DataStoreUtils.isMergingIteratorRequired(readerParams, visibilityEnabled),
              rowTransformer,
              new ClientVisibilityFilter(
                  Sets.newHashSet(readerParams.getAdditionalAuthorizations())),
              visibilityEnabled).results();
    } catch (final KuduException e) {
      LOGGER.error("Error in initializing reader", e);
    }
  }

  @Override
  public void close() {
    iterator.close();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return iterator.next();
  }

}
