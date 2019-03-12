package org.locationtech.geowave.datastore.kudu.operations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
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
  private final KuduTable table;
  private final KuduOperations operations;
  private final boolean visibilityEnabled;
  private final Predicate<GeoWaveRow> filter;
  private final GeoWaveRowIteratorTransformer<T> rowTransformer;
  private final boolean rowMerging;
  private List<RowResultIterator> results;

  protected KuduRangeRead(
      final Collection<SinglePartitionQueryRanges> ranges,
      final short[] adapterIds,
      final KuduTable table,
      final KuduOperations operations,
      final boolean visibilityEnabled,
      final Predicate<GeoWaveRow> filter,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final boolean rowMerging) {
    this.ranges = ranges;
    this.adapterIds = adapterIds;
    this.table = table;
    this.schema = table.getSchema();
    this.operations = operations;
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
                  ? KuduUtils.EMPTY_PARTITION_KEY
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
            executeQuery(scanner, results);
          }
        }
      } else {
        KuduScannerBuilder scannerBuilder = operations.getScannerBuilder(table);
        KuduScanner scanner = scannerBuilder.addPredicate(adapterIdPred).build();
        executeQuery(scanner, results);
      }
    }

    if (visibilityEnabled) {
      tmpIterator =
          Streams.stream(Iterators.concat(results.iterator())).map(
              r -> (GeoWaveRow) new KuduRow(r)).filter(filter).iterator();
    } else {
      tmpIterator =
          Iterators.transform(
              Iterators.concat(results.iterator()),
              r -> (GeoWaveRow) new KuduRow(r));
    }
    rowTransformer.apply(rowMerging ? new GeoWaveRowMergingIterator(tmpIterator) : tmpIterator);
    return new CloseableIteratorWrapper<>(() -> {
    }, (Iterator<T>) tmpIterator);
  }

  private void executeQuery(KuduScanner scanner, List<RowResultIterator> results) {
    while (scanner.hasMoreRows()) {
      try {
        results.add(scanner.nextRows());
      } catch (KuduException e) {
        LOGGER.error("Error when reading rows", e);
      }
    }
    try {
      scanner.close();
    } catch (KuduException e) {
      LOGGER.error("Error when closing scanner", e);
    }
  }
}
