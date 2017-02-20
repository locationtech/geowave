package mil.nga.giat.geowave.datastore.accumulo.operations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.datastore.accumulo.MergingVisibilityCombiner;
import mil.nga.giat.geowave.datastore.accumulo.util.ScannerClosableWrapper;

public class AccumuloMetadataReader implements
		MetadataReader
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloMetadataReader.class);
	private static final int STATS_MULTI_VISIBILITY_COMBINER_PRIORITY = 15;
	private final AccumuloOperations operations;
	private final DataStoreOptions options;
	private final MetadataType metadataType;

	public AccumuloMetadataReader(
			AccumuloOperations operations,
			DataStoreOptions options,
			MetadataType metadataType ) {
		this.operations = operations;
		this.options = options;
		this.metadataType = metadataType;
	}

	@Override
	public CloseableIterator<GeoWaveMetadata> query(
			final MetadataQuery query ) {
		BatchScanner scanner;
		try {
			scanner = operations.createBatchScanner(
					AbstractGeoWavePersistence.METADATA_TABLE,
					query.getAuthorizations());
			final IteratorSetting[] settings = getScanSettings();
			if ((settings != null) && (settings.length > 0)) {
				for (final IteratorSetting setting : settings) {
					scanner.addScanIterator(setting);
				}
			}
			final String columnFamily = metadataType.name();
			final byte[] columnQualifier = query.getSecondaryId();
			if (columnFamily != null) {
				if (columnQualifier != null) {
					scanner.fetchColumn(
							new Text(
									columnFamily),
							new Text(
									columnQualifier));
				}
				else {
					scanner.fetchColumnFamily(new Text(
							columnFamily));
				}
			}
			final Collection<Range> ranges = new ArrayList<Range>();
			if (query.getPrimaryId() != null) {
				ranges.add(new Range(
						new Text(
								query.getPrimaryId())));
			}
			else {
				ranges.add(new Range());
			}
			scanner.setRanges(ranges);
			return new CloseableIteratorWrapper<>(
					new ScannerClosableWrapper(
							scanner),
					Iterators.transform(
							scanner.iterator(),
							new com.google.common.base.Function<Entry<Key, Value>, GeoWaveMetadata>() {

								@Override
								public GeoWaveMetadata apply(
										final Entry<Key, Value> row ) {
									return new GeoWaveMetadata(
											row.getKey().getRow().getBytes(),
											row.getKey().getColumnQualifier().getBytes(),
											row.getKey().getColumnVisibility().getBytes(),
											row.getValue().get());
								}

							}));
		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"GeoWave metadata table not found",
					e);
		}
		return new CloseableIterator.Wrapper<>(
				Iterators.emptyIterator());
	}

	private IteratorSetting[] getScanSettings() {
		if (MetadataType.STATS.equals(metadataType) && options.isServerSideLibraryEnabled()) {
			return getStatsScanSettings();
		}
		return null;
	}

	private static IteratorSetting[] getStatsScanSettings() {
		final IteratorSetting statsMultiVisibilityCombiner = new IteratorSetting(
				STATS_MULTI_VISIBILITY_COMBINER_PRIORITY,
				MergingVisibilityCombiner.class);
		return new IteratorSetting[] {
			statsMultiVisibilityCombiner
		};
	}
}
