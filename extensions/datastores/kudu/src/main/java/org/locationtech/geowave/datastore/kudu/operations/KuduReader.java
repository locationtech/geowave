package org.locationtech.geowave.datastore.kudu.operations;

import com.google.common.collect.Sets;
import org.apache.kudu.client.KuduException;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class KuduReader<T> implements RowReader<T> {
  private final ReaderParams<T> readerParams;
  private final RecordReaderParams recordReaderParams;
  private final DataIndexReaderParams dataIndexReaderParams;
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
    this.recordReaderParams = null;
    this.dataIndexReaderParams = null;
    this.operations = operations;
    this.rowTransformer = readerParams.getRowTransformer();
    this.visibilityEnabled = visibilityEnabled;

    initScanner();
  }

  public KuduReader(
      final DataIndexReaderParams dataIndexReaderParams,
      final KuduOperations operations,
      final boolean visibilityEnabled) {
    this.dataIndexReaderParams = dataIndexReaderParams;
    this.readerParams = null;
    this.recordReaderParams = null;
    this.operations = operations;
    this.rowTransformer =
        (GeoWaveRowIteratorTransformer<T>) GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER;
    this.visibilityEnabled = visibilityEnabled;

    initDataIndexScanner();
  }

  public KuduReader(
      RecordReaderParams recordReaderParams,
      KuduOperations operations,
      boolean visibilityEnabled) {
    this.readerParams = null;
    this.recordReaderParams = recordReaderParams;
    this.dataIndexReaderParams = null;
    this.operations = operations;
    this.rowTransformer =
        (GeoWaveRowIteratorTransformer<T>) GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER;
    this.visibilityEnabled = visibilityEnabled;
    initRecordScanner();
  }

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

  protected void initDataIndexScanner() {
    final byte[][] dataIds;
    if (dataIndexReaderParams.getDataIds() == null) {
      if (dataIndexReaderParams.getStartInclusiveDataId() != null
          || dataIndexReaderParams.getEndInclusiveDataId() != null) {
        final List<byte[]> intermediaries = new ArrayList<>();
        ByteArrayUtils.addAllIntermediaryByteArrays(
            intermediaries,
            new ByteArrayRange(
                dataIndexReaderParams.getStartInclusiveDataId(),
                dataIndexReaderParams.getEndInclusiveDataId()));
        dataIds = intermediaries.toArray(new byte[0][]);
      } else {
        dataIds = null;
      }
    } else {
      dataIds = dataIndexReaderParams.getDataIds();
    }
    try {
      iterator =
          operations.<T>getKuduDataIndexRead(
              DataIndexUtils.DATA_ID_INDEX.getName(),
              dataIndexReaderParams.getAdapterId(),
              dataIds,
              new ClientVisibilityFilter(
                  Sets.newHashSet(dataIndexReaderParams.getAdditionalAuthorizations())),
              visibilityEnabled).results();
    } catch (final KuduException e) {
      LOGGER.error("Error in initializing reader", e);
    }
  }

  protected void initRecordScanner() {
    final short[] adapterIds =
        recordReaderParams.getAdapterIds() != null ? recordReaderParams.getAdapterIds()
            : new short[0];

    final GeoWaveRowRange range = recordReaderParams.getRowRange();
    final byte[] startKey = range.isInfiniteStartSortKey() ? null : range.getStartSortKey();
    final byte[] stopKey = range.isInfiniteStopSortKey() ? null : range.getEndSortKey();
    final SinglePartitionQueryRanges partitionRange =
        new SinglePartitionQueryRanges(
            range.getPartitionKey(),
            Collections.singleton(new ByteArrayRange(startKey, stopKey)));
    try {
      this.iterator =
          operations.getKuduRangeRead(
              recordReaderParams.getIndex().getName(),
              adapterIds,
              Collections.singleton(partitionRange),
              DataStoreUtils.isMergingIteratorRequired(recordReaderParams, visibilityEnabled),
              rowTransformer,
              new ClientVisibilityFilter(
                  Sets.newHashSet(recordReaderParams.getAdditionalAuthorizations())),
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
