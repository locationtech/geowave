package mil.nga.giat.geowave.ingest.mapreduce.gpx;

import java.util.Date;

import mil.nga.giat.geowave.accumulo.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexStore;
import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.gt.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.mapreduce.GeoWaveOutputFormat;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;

public class GPXJobRunner extends Configured implements Tool {

	static String JOB_NAME = "GPX Ingest from %s to namespace %s";
	static String GPX_POINT_FEATURE = "gpxpoint";
	static String GPX_TRACK_FEATURE = "gpxtrack";

	public static void main( final String[] args )
			throws Exception {
		final Configuration conf = new Configuration();
		final int res = ToolRunner.run(conf, new GPXJobRunner(), args);
		System.exit(res);
	}

	@Override
	public int run( final String[] args )
			throws Exception {
		final Configuration conf = getConf();
		final String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
		if (otherArgs.length != 6) {
			System.err.println("Parameters should be: <input hdfs sequence file>  <zookeepers> <accumulo instance> <accumulo user> <accumulo pass> <geowave namespace>");
			return 2;
		}

		conf.set("inputDirectory", otherArgs[0]);
		conf.set("zookeepers", otherArgs[3]);
		conf.set("accumuloInstance", otherArgs[4]);
		conf.set("accumuloUser", otherArgs[5]);
		conf.set("accumuloPass", otherArgs[6]);
		conf.set("geowaveNamespace", otherArgs[7]);

		final Job job = new Job(conf, String.format(JOB_NAME, conf.get("inputDirectory"), conf.get("geowaveNamespace")));

		job.setJarByClass(GPXJobRunner.class);
		job.setMapperClass(GPXMapper.class);
		// job.setReducerClass(IngestReducer.class);

		FileInputFormat.setInputPaths(job, conf.get("inputDirectory"));
		job.setInputFormatClass(SequenceFileInputFormat.class);

		// set mappper output info
		job.setMapOutputKeyClass(ByteArrayId.class);
		job.setMapOutputValueClass(Object.class);

		// set geowave output format
		job.setOutputFormatClass(GeoWaveOutputFormat.class);
		
		job.setNumReduceTasks(0);

		// set accumulo operations
		GeoWaveOutputFormat.setAccumuloOperationsInfo(job, 
				otherArgs[3], // zookeepers
				otherArgs[4], // accumuloInstance
				otherArgs[5], // accumuloUser
				otherArgs[6], // accumuloPass
				otherArgs[7]); // geowaveNamespace

		final AccumuloOperations operations = new BasicAccumuloOperations(
				otherArgs[3], // zookeepers
				otherArgs[4], // accumuloInstance
				otherArgs[5], // accumuloUser
				otherArgs[6], // accumuloPass
				otherArgs[7]); // geowaveNamespace

		final AdapterStore adapterStore = new AccumuloAdapterStore(operations);
		final IndexStore indexStore = new AccumuloIndexStore(operations);

		final Index index = IndexType.SPATIAL.createDefaultIndex();
		final DataAdapter<SimpleFeature> pointAdapter = new FeatureDataAdapter(createGPXPointDataType());
		final DataAdapter<SimpleFeature> trackAdapter = new FeatureDataAdapter(createGPXTrackDataType());

		adapterStore.addAdapter(pointAdapter);
		adapterStore.addAdapter(trackAdapter);

		indexStore.addIndex(index);

		// add data adapters
		GeoWaveOutputFormat.addDataAdapter(job, pointAdapter);
		GeoWaveOutputFormat.addDataAdapter(job, trackAdapter);

		// set index
		GeoWaveOutputFormat.setIndex(job, index);

		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	


	static SimpleFeatureType createGPXPointDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(GPX_POINT_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Geometry.class).buildDescriptor("geometry"));

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Date.class).buildDescriptor("StartTimeStamp"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Date.class).buildDescriptor("EndTimeStamp"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Long.class).buildDescriptor("Duration"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Long.class).buildDescriptor("NumberPoints"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("TrackId"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Long.class).buildDescriptor("Points"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("Name"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Long.class).buildDescriptor("Uid"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("User"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("Description"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("Tags"));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	static SimpleFeatureType createGPXTrackDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(GPX_TRACK_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Geometry.class).buildDescriptor("geometry"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("Mission"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("TrackNumber"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("TrackUUID"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("TrackItemUUID"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("MotionEvent"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Date.class).buildDescriptor("StartTime"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Date.class).buildDescriptor("EndTime"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("Classification"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Double.class).buildDescriptor("Latitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Double.class).buildDescriptor("Longitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Double.class).buildDescriptor("Elevation"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Integer.class).buildDescriptor("FrameNumber"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Integer.class).buildDescriptor("PixelRow"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(Integer.class).buildDescriptor("PixelColumn"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(String.class).buildDescriptor("ImageChip"));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	
}
