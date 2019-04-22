package org.locationtech.geowave.datastore.kudu.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.datastore.kudu.KuduRow;
import org.locationtech.geowave.datastore.kudu.KuduRow.KuduField;
import org.locationtech.geowave.datastore.kudu.util.KuduUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;

public class KuduRangeRead<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduRangeRead.class);
  private final Collection<SinglePartitionQueryRanges> ranges;
  private final Schema schema;
  private final short[] adapterIds;
  final byte[][] dataIds;
  private final KuduTable table;
  private final KuduOperations operations;
  private final boolean isDataIndex;
  private final boolean visibilityEnabled;
  private final Predicate<GeoWaveRow> filter;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private final boolean rowMerging;
  private List<RowResultIterator> results;

  protected KuduRangeRead(
      final Collection<SinglePartitionQueryRanges> ranges,
      final short[] adapterIds,
      final byte[][] dataIds,
      final KuduTable table,
      final KuduOperations operations,
      final boolean isDataIndex,
      final boolean visibilityEnabled,
      final Predicate<GeoWaveRow> filter,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final boolean rowMerging) {
    this.ranges = ranges;
    this.adapterIds = adapterIds;
    this.dataIds = dataIds;
    this.table = table;
    this.schema = table.getSchema();
    this.operations = operations;
    this.isDataIndex = isDataIndex;
    this.visibilityEnabled = visibilityEnabled;
    this.filter = filter;
    this.rowTransformer = rowTransformer;
    this.rowMerging = rowMerging;
    this.results = new ArrayList<>();
  }

  public CloseableIterator<T> results() {
    results = new ArrayList<>();
    Iterator<GeoWaveRow> tmpIterator;
    for (final short adapterId : adapterIds) {
      KuduPredicate adapterIdPred =
          KuduPredicate.newComparisonPredicate(
              schema.getColumn(KuduField.GW_ADAPTER_ID_KEY.getFieldName()),
              ComparisonOp.EQUAL,
              adapterId);
      if ((ranges != null) && !ranges.isEmpty()) {
        for (final SinglePartitionQueryRanges r : ranges) {
          final byte[] partitionKey =
              ((r.getPartitionKey() == null) || (r.getPartitionKey().length == 0))
                  ? KuduUtils.EMPTY_KEY
                  : r.getPartitionKey();
          for (final ByteArrayRange range : r.getSortKeyRanges()) {
            final byte[] start = range.getStart() != null ? range.getStart() : new byte[0];
            final byte[] end =
                range.getEnd() != null ? range.getEndAsNextPrefix()
                    : new byte[] {
                        (byte) 0xFF,
                        (byte) 0xFF,
                        (byte) 0xFF,
                        (byte) 0xFF,
                        (byte) 0xFF,
                        (byte) 0xFF,
                        (byte) 0xFF};
            KuduPredicate lowerPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduField.GW_SORT_KEY.getFieldName()),
                    ComparisonOp.GREATER_EQUAL,
                    start);
            KuduPredicate upperPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduField.GW_SORT_KEY.getFieldName()),
                    ComparisonOp.LESS,
                    end);
            KuduPredicate partitionPred =
                KuduPredicate.newComparisonPredicate(
                    schema.getColumn(KuduField.GW_PARTITION_ID_KEY.getFieldName()),
                    ComparisonOp.EQUAL,
                    partitionKey);

            KuduScannerBuilder scannerBuilder = operations.getScannerBuilder(table);
            KuduScanner scanner =
                scannerBuilder.addPredicate(lowerPred).addPredicate(upperPred).addPredicate(
                    partitionPred).addPredicate(adapterIdPred).build();
            KuduUtils.executeQuery(scanner, results);
          }
        }
      } else if (dataIds != null) {
        for (final byte[] dataId : dataIds) {
          KuduPredicate partitionPred =
              KuduPredicate.newComparisonPredicate(
                  schema.getColumn(KuduField.GW_PARTITION_ID_KEY.getFieldName()),
                  ComparisonOp.EQUAL,
                  dataId);
          KuduScannerBuilder scannerBuilder = operations.getScannerBuilder(table);
          KuduScanner scanner =
              scannerBuilder.addPredicate(partitionPred).addPredicate(adapterIdPred).build();
          KuduUtils.executeQuery(scanner, results);
        }
      } else {
        KuduScannerBuilder scannerBuilder = operations.getScannerBuilder(table);
        KuduScanner scanner = scannerBuilder.addPredicate(adapterIdPred).build();
        KuduUtils.executeQuery(scanner, results);
      }
    }

    if (dataIds == null) {
      if (visibilityEnabled) {
        if (isDataIndex) {
          tmpIterator =
              Streams.stream(Iterators.concat(results.iterator())).map(
                  r -> KuduRow.deserializeDataIndexRow(r, visibilityEnabled)).filter(
                      filter).iterator();
        } else {
          tmpIterator =
              Streams.stream(Iterators.concat(results.iterator())).map(
                  r -> (GeoWaveRow) new KuduRow(r)).filter(filter).iterator();
        }
      } else {
        if (isDataIndex) {
          tmpIterator =
              Iterators.transform(
                  Iterators.concat(results.iterator()),
                  r -> KuduRow.deserializeDataIndexRow(r, visibilityEnabled));
        } else {
          tmpIterator =
              Iterators.transform(Iterators.concat(results.iterator()), r -> new KuduRow(r));
        }
      }
      return new CloseableIteratorWrapper<>(() -> {
      },
          rowTransformer.apply(
              rowMerging ? new GeoWaveRowMergingIterator(tmpIterator) : tmpIterator));
    } else {
      Iterator<RowResult> rowResultIterator = Iterators.concat(results.iterator());
      // Order the rows for data index query
      final Map<ByteArray, GeoWaveRow> resultsMap = new HashMap<>();
      while (rowResultIterator.hasNext()) {
        RowResult r = rowResultIterator.next();
        final byte[] d = r.getBinaryCopy(KuduField.GW_PARTITION_ID_KEY.getFieldName());
        resultsMap.put(
            new ByteArray(d),
            DataIndexUtils.deserializeDataIndexRow(
                d,
                adapterIds[0],
                r.getBinaryCopy(KuduField.GW_VALUE_KEY.getFieldName()),
                visibilityEnabled));
      }
      tmpIterator =
          Arrays.stream(dataIds).map(d -> resultsMap.get(new ByteArray(d))).filter(
              r -> r != null).iterator();
      return new CloseableIteratorWrapper<>(() -> {
      }, (Iterator<T>) tmpIterator);
    }
  }

}
