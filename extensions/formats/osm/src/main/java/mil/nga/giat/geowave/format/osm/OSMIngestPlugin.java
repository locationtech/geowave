package mil.nga.giat.geowave.format.osm;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.jaitools.jts.CoordinateSequence2D;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.openstreetmap.osmosis.osmbinary.file.BlockInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.ingest.AbstractSimpleFeatureIngestPlugin;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.geotime.store.dimension.Time;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.FloatCompareUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestPluginBase;
import mil.nga.giat.geowave.core.ingest.avro.AbstractStageWholeFileToAvro;
import mil.nga.giat.geowave.core.ingest.avro.WholeFile;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.ByteBufferBackedInputStream;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.core.ingest.hdfs.mapreduce.KeyValueData;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.NullIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.format.osm.convert.SimpleFeatureGenerator;
import mil.nga.giat.geowave.format.osm.feature.FeatureConfigParser;
import mil.nga.giat.geowave.format.osm.feature.types.FeatureDefinitionSet;
import mil.nga.giat.geowave.format.osm.parser.OsmPbfParser;
import mil.nga.giat.geowave.format.osm.parser.OsmPbfParserOptions;
import mil.nga.giat.geowave.format.osm.parser.OsmPbfParser.OsmAvroBinaryParser;
import mil.nga.giat.geowave.format.osm.types.generated.Node;
import mil.nga.giat.geowave.format.osm.types.generated.Relation;
import mil.nga.giat.geowave.format.osm.types.generated.Way;

public class OSMIngestPlugin extends
	AbstractSimpleFeatureIngestPlugin<WholeFile>
{
	private static Logger LOGGER = LoggerFactory.getLogger(OSMIngestPlugin.class);
	public final static PrimaryIndex NODE_INDEX = new NullIndex(
			"NODES");
	public final static PrimaryIndex RELATION_INDEX = new NullIndex(
			"RELATIONS");
	public final static PrimaryIndex WAY_INDEX = new NullIndex(
			"WAYS");

	private static final List<ByteArrayId> NODE_AS_ID_LIST = Arrays.asList(NODE_INDEX.getId());
	private static final List<ByteArrayId> RELATION_AS_ID_LIST = Arrays.asList(RELATION_INDEX.getId());
	private static final List<ByteArrayId> WAY_AS_ID_LIST = Arrays.asList(WAY_INDEX.getId());
	

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"pbf"
		};
	}

	public OSMIngestPlugin() {
		super();
	}

	@Override
	public void init(
			final URL baseDirectory ) {

		//Parse mapping contents to define feature types. This comes from mapping file
		FeatureDefinitionSet.initialize(OSMIngestUtils.mappingContents);
	}

	@Override
	public boolean supportsFile(
			final URL file ) {
		// TODO: consider checking for schema compliance
		try {
			return file.openConnection().getContentLength() > 0;
		}
		catch (IOException e) {
			LOGGER.info(
					"Unable to read URL for '" + file.getPath() + "'",
					e);
		}
		return false;
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<WholeFile, SimpleFeature> ingestWithMapper() {
		return new IngestWithMapperImpl();
	}

	
	public static class IngestWithMapperImpl extends
	AbstractIngestSimpleFeatureWithMapper<WholeFile>
	{
		//TODO: Separate Types or SimpleFeatureGenerator or both?
		//private final SimpleFeatureBuilder nodeBuilder;
		//private final SimpleFeatureBuilder relationBuilder;
		//private final SimpleFeatureBuilder wayBuilder;
		//private final SimpleFeatureType nodeType;
		//private final SimpleFeatureType relationType;
		//private final SimpleFeatureType wayType;
		

		public IngestWithMapperImpl() {
			super(new OSMIngestPlugin());
		}
	}


	@Override
	public IngestPluginBase<WholeFile, SimpleFeature> getIngestWithAvroPlugin() {
		return ingestWithMapper();
	}

	@Override
	public Class<? extends CommonIndexValue>[] getSupportedIndexableTypes() {
		return new Class[] {
			GeometryWrapper.class,
			Time.class
		};
	}

	@Override
	public IngestWithReducer<WholeFile, ?, ?, SimpleFeature> ingestWithReducer() {
		throw new UnsupportedOperationException(
				"Ingest with reducer currently unsupported at this time");
	}

	@Override
	public Schema getAvroSchema() {
		return WholeFile.getClassSchema();
	}

	@Override
	public WholeFile[] toAvroObjects(
			URL file ) {
		final WholeFile avroFile = new WholeFile();
		avroFile.setOriginalFilePath(file.getPath());
		try {
			avroFile.setOriginalFile(ByteBuffer.wrap(IOUtils.toByteArray(file)));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read GBDX file: " + file.getPath(),
					e);
			return new WholeFile[] {};
		}

		return new WholeFile[] {
			avroFile
		};
	}

	@Override
	protected SimpleFeatureType[] getTypes() {
		//Should we add Node/Way/Relation?
		//int typeCount = FeatureDefinitionSet.featureTypes.values().size();
		//SimpleFeatureType[] types = new SimpleFeatureType[typeCount + 3];
		//FeatureDefinitionSet.featureTypes.values().toArray(types);
		//TODO:Add Node/Way/Relation types
		
		
		return FeatureDefinitionSet.featureTypes.values().toArray(null);
	}

	@Override
	protected CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			WholeFile hdfsObject,
			Collection<ByteArrayId> primaryIndexIds,
			String globalVisibility ) {
		//pbf file should be loaded to hdfs. Now need to unpack to nodes, ways, relations
		ByteBufferBackedInputStream is = new ByteBufferBackedInputStream(hdfsObject.getOriginalFile());

		DataFileWriter nodeWriter = new DataFileWriter(
				new GenericDatumWriter());
		DataFileWriter wayWriter = new DataFileWriter(
				new GenericDatumWriter());
		DataFileWriter relationWriter = new DataFileWriter(
				new GenericDatumWriter());
		nodeWriter.setCodec(CodecFactory.snappyCodec());
		wayWriter.setCodec(CodecFactory.snappyCodec());
		relationWriter.setCodec(CodecFactory.snappyCodec());
		FSDataOutputStream nodeOut = null;
		FSDataOutputStream wayOut = null;
		FSDataOutputStream relationOut = null;

		mil.nga.giat.geowave.format.osm.parser.OsmAvroBinaryParser parser = new mil.nga.giat.geowave.format.osm.parser.OsmAvroBinaryParser();
		
		
		//TODO: This feels extremely unnecessary? Maybe just read into ByteBufferBacked output stream, and use from there?
		final Configuration conf = new Configuration();
		conf.set(
				"fs.defaultFS",
				hdfsHostPort);
		conf.set(
				"fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		final Path hdfsBaseDirectory = new Path(
				basePath);

		try (final FileSystem fs = FileSystem.get(
				conf)) {
			if (!fs.exists(
					hdfsBaseDirectory)) {
				fs.mkdirs(
						hdfsBaseDirectory);
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to create remote HDFS directory",
					e);
		}
		
		try {
			//TODO: How do I get hdfs ingest directory?
			nodeOut = fs.create(nodesPath);
			wayOut = fs.create(waysPath);
			relationOut = fs.create(relationsPath);

			nodeWriter.create(
					Node.getClassSchema(),
					nodeOut);
			wayWriter.create(
					Way.getClassSchema(),
					wayOut);
			relationWriter.create(
					Relation.getClassSchema(),
					relationOut);

			parser.setupWriter(
					nodeWriter,
					wayWriter,
					relationWriter);
			
			new BlockInputStream(is, parser).process();
		}
		catch (IOException e) {
			LOGGER.error("Unable to parse and write pbf file to hdfs", e);
			e.printStackTrace();
		}
		
		//After this point pbf file should be expanded to nodes/ways/relations on hdfs
		
		//TODO: Loop through and process all nodes/ways/relations in directories
		
				return null;
	}

}