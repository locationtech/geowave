package org.locationtech.geowave.datastore.kudu.operations;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.StatisticsRowIterator;
import org.locationtech.geowave.datastore.kudu.KuduMetadataRow.KuduMetadataField;
import org.locationtech.geowave.datastore.kudu.util.KuduUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KuduMetadataReader implements MetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduMetadataReader.class);
  private final KuduOperations operations;
  private final MetadataType metadataType;

  public KuduMetadataReader(KuduOperations operations, MetadataType metadataType) {
    this.operations = operations;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(MetadataQuery query) {
    List<RowResultIterator> queryResult = new ArrayList<>();
    final String tableName = operations.getMetadataTableName(metadataType);
    try {
      KuduTable table = operations.getTable(tableName);
      Schema schema = table.getSchema();
      KuduScanner.KuduScannerBuilder scannerBuilder = operations.getScannerBuilder(table);
      if (query.hasPrimaryId()) {
        KuduPredicate primaryPred =
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                query.getPrimaryId());
        scannerBuilder = scannerBuilder.addPredicate(primaryPred);
      }
      if (query.hasSecondaryId()) {
        KuduPredicate secondaryPred =
            KuduPredicate.newComparisonPredicate(
                schema.getColumn(KuduMetadataField.GW_SECONDARY_ID_KEY.getFieldName()),
                KuduPredicate.ComparisonOp.EQUAL,
                query.getSecondaryId());
        scannerBuilder = scannerBuilder.addPredicate(secondaryPred);
      }
      KuduScanner scanner = scannerBuilder.build();
      KuduUtils.executeQuery(scanner, queryResult);
    } catch (KuduException e) {
      LOGGER.error("Encountered error while reading metadata row", e);
    }
    Iterator<GeoWaveMetadata> temp =
        Streams.stream(Iterators.concat(queryResult.iterator())).map(
            result -> new GeoWaveMetadata(
                query.hasPrimaryId() ? query.getPrimaryId()
                    : result.getBinaryCopy(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                query.hasSecondaryId() ? query.getSecondaryId()
                    : result.getBinaryCopy(KuduMetadataField.GW_SECONDARY_ID_KEY.getFieldName()),
                getVisibility(result),
                result.getBinaryCopy(KuduMetadataField.GW_VALUE_KEY.getFieldName()))).iterator();
    CloseableIterator<GeoWaveMetadata> retVal = new CloseableIterator.Wrapper<>(temp);
    return MetadataType.STATS.equals(metadataType)
        ? new StatisticsRowIterator(retVal, query.getAuthorizations())
        : retVal;
  }

  private byte[] getVisibility(RowResult result) {
    if (MetadataType.STATS.equals(metadataType)) {
      return result.getBinaryCopy(KuduMetadataField.GW_VISIBILITY_KEY.getFieldName());
    }
    return null;
  }

}
