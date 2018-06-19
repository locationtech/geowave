package mil.nga.giat.geowave.datastore.hbase.operations;

import java.util.Iterator;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils.ScannerClosableWrapper;
import mil.nga.giat.geowave.mapreduce.URLClassloaderUtils;

public class HBaseMetadataReader implements
		MetadataReader
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseMetadataReader.class);
	private final HBaseOperations operations;
	private final DataStoreOptions options;
	private final MetadataType metadataType;

	public HBaseMetadataReader(
			final HBaseOperations operations,
			final DataStoreOptions options,
			final MetadataType metadataType ) {
		this.operations = operations;
		this.options = options;
		this.metadataType = metadataType;
	}

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			final MetadataQuery query ) {
		final Scan scanner = new Scan();

		try {
			final byte[] columnFamily = StringUtils.stringToBinary(metadataType.name());
			final byte[] columnQualifier = query.getSecondaryId();

			if (columnQualifier != null) {
				scanner.addColumn(
						columnFamily,
						columnQualifier);
			}
			else {
				scanner.addFamily(columnFamily);
			}

			if (query.hasPrimaryId()) {
				scanner.setStartRow(query.getPrimaryId());
				scanner.setStopRow(query.getPrimaryId());
			}
			final boolean clientsideStatsMerge = (metadataType == MetadataType.STATS)
					&& !options.isServerSideLibraryEnabled();
			if (clientsideStatsMerge) {
				scanner.setMaxVersions(); // Get all versions
			}

			String[] additionalAuthorizations = query.getAuthorizations();
			if ((additionalAuthorizations != null) && (additionalAuthorizations.length > 0)) {
				scanner.setAuthorizations(new Authorizations(
						additionalAuthorizations));
			}
			final Iterable<Result> rS = operations.getScannedResults(
					scanner,
					AbstractGeoWavePersistence.METADATA_TABLE);
			final Iterator<Result> it = rS.iterator();
			final Iterator<GeoWaveMetadata> transformedIt = Iterators.transform(
					it,
					new com.google.common.base.Function<Result, GeoWaveMetadata>() {
						@Override
						public GeoWaveMetadata apply(
								final Result result ) {
							byte[] resultantCQ;
							if (columnQualifier == null) {
								final NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
								if ((familyMap != null) && !familyMap.isEmpty()) {
									resultantCQ = familyMap.firstKey();
								}
								else {
									resultantCQ = new byte[0];
								}
							}
							else {
								resultantCQ = columnQualifier;
							}
							return new GeoWaveMetadata(
									result.getRow(),
									resultantCQ,
									null,
									getMergedStats(
											result,
											clientsideStatsMerge));
						}
					});
			if (rS instanceof ResultScanner) {
				return new CloseableIteratorWrapper<>(
						new ScannerClosableWrapper(
								(ResultScanner) rS),
						transformedIt);
			}
			else {
				return new CloseableIterator.Wrapper<>(
						transformedIt);
			}

		}
		catch (final Exception e) {
			LOGGER.warn(
					"GeoWave metadata table not found",
					e);
		}
		return new CloseableIterator.Wrapper<>(
				Iterators.emptyIterator());
	}

	private byte[] getMergedStats(
			final Result result,
			final boolean clientsideStatsMerge ) {
		if (!clientsideStatsMerge || (result.size() == 1)) {
			return result.value();
		}

		return URLClassloaderUtils.toBinary(HBaseUtils.getMergedStats(result.listCells()));
	}
}
