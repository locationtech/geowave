package mil.nga.giat.geowave.format.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.AvroFeatureUtils;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.avro.AttributeValues;
import mil.nga.giat.geowave.adapter.vector.avro.AvroSimpleFeatureCollection;
import mil.nga.giat.geowave.adapter.vector.avro.FeatureDefinition;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This plugin is used for ingesting any GPX formatted data from a local file
 * system into GeoWave as GeoTools' SimpleFeatures. It supports the default
 * configuration of spatial and spatial-temporal indices and it will support
 * wither directly ingesting GPX data from a local file system to GeoWave or to
 * stage the data in an intermediate format in HDFS and then to ingest it into
 * GeoWave using a map-reduce job. It supports OSM metadata.xml files if the
 * file is directly in the root base directory that is passed in command-line to
 * the ingest framework.
 */
public class AvroIngestPlugin extends
		AbstractSimpleFeatureIngestPlugin<AvroSimpleFeatureCollection>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AvroIngestPlugin.class);

	public AvroIngestPlugin() {}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"avro",
			"dat",
			"bin",
			"json" // TODO: does the Avro DataFileReader actually support JSON
					// formatted avro files, or should we limit the extensions
					// to expected binary extensions?
		};
	}

	@Override
	public void init(
			final File baseDirectory ) {}

	@Override
	public boolean supportsFile(
			final File file ) {
		try {
			DataFileReader.openReader(
					file,
					new SpecificDatumReader<AvroSimpleFeatureCollection>()).close();
			return true;
		}
		catch (final IOException e) {
			// just log as info as this may not have been intended to be read as
			// avro vector data
			LOGGER.info(
					"Unable to read file as Avro vector data '" + file.getName() + "'",
					e);
		}

		return false;
	}

	@Override
	protected SimpleFeatureType[] getTypes() {
		return new SimpleFeatureType[] {};
	}

	@Override
	public Schema getAvroSchema() {
		return AvroSimpleFeatureCollection.getClassSchema();
	}

	@Override
	public AvroSimpleFeatureCollection[] toAvroObjects(
			final File input ) {
		final List<AvroSimpleFeatureCollection> retVal = new ArrayList<AvroSimpleFeatureCollection>();
		try (DataFileReader<AvroSimpleFeatureCollection> reader = new DataFileReader<AvroSimpleFeatureCollection>(
				input,
				new SpecificDatumReader<AvroSimpleFeatureCollection>())) {
			while (reader.hasNext()) {
				final AvroSimpleFeatureCollection simpleFeatureCollection = reader.next();
				retVal.add(simpleFeatureCollection);
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read file '" + input.getAbsolutePath() + "' as AVRO SimpleFeatureCollection",
					e);
		}
		return retVal.toArray(new AvroSimpleFeatureCollection[] {});
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<AvroSimpleFeatureCollection, SimpleFeature> ingestWithMapper() {
		return new IngestAvroFeaturesFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<AvroSimpleFeatureCollection, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"Avro simple feature collections cannot be ingested with a reducer");
	}

	@Override
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final AvroSimpleFeatureCollection featureCollection,
			final Collection<ByteArrayId> primaryIndexIds,
			final String globalVisibility ) {
		final FeatureDefinition featureDefinition = featureCollection.getFeatureType();
		final List<GeoWaveData<SimpleFeature>> retVal = new ArrayList<GeoWaveData<SimpleFeature>>();
		SimpleFeatureType featureType;
		try {
			featureType = AvroFeatureUtils.avroFeatureDefinitionToGTSimpleFeatureType(featureDefinition);

			final FeatureDataAdapter adapter = new FeatureDataAdapter(
					featureType);
			final List<String> attributeTypes = featureDefinition.getAttributeTypes();
			for (final AttributeValues attributeValues : featureCollection.getSimpleFeatureCollection()) {
				try {
					final SimpleFeature simpleFeature = AvroFeatureUtils.avroSimpleFeatureToGTSimpleFeature(
							featureType,
							attributeTypes,
							attributeValues);
					retVal.add(new GeoWaveData<SimpleFeature>(
							adapter,
							primaryIndexIds,
							simpleFeature));
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to read simple feature from Avro",
							e);
				}
			}
		}
		catch (final ClassNotFoundException e) {
			LOGGER.warn(
					"Unable to read simple feature type from Avro",
					e);
		}
		return new Wrapper<GeoWaveData<SimpleFeature>>(
				retVal.iterator());
	}

	@Override
	public PrimaryIndex[] getRequiredIndices() {
		return new PrimaryIndex[] {};
	}

	public static class IngestAvroFeaturesFromHdfs extends
			AbstractIngestSimpleFeatureWithMapper<AvroSimpleFeatureCollection>
	{
		public IngestAvroFeaturesFromHdfs() {
			this(
					new AvroIngestPlugin());
			// this constructor will be used when deserialized
		}

		public IngestAvroFeaturesFromHdfs(
				final AvroIngestPlugin parentPlugin ) {
			super(
					parentPlugin);
		}
	}

	@Override
	public IngestPluginBase<AvroSimpleFeatureCollection, SimpleFeature> getIngestWithAvroPlugin() {
		return new IngestAvroFeaturesFromHdfs(
				this);
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

}
