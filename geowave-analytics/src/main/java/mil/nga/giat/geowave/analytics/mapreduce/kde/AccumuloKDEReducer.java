package mil.nga.giat.geowave.analytics.mapreduce.kde;

import java.io.IOException;

import javax.vecmath.Point2d;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloOptions;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexStore;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.index.MemoryIndexStore;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class AccumuloKDEReducer extends
		Reducer<DoubleWritable, LongWritable, Text, Mutation>
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloKDEReducer.class);
	public static final String STATS_NAME_KEY = "STATS_NAME";
	private long totalKeys = 0;
	private double max = -Double.MAX_VALUE;
	private double inc = 0;

	private SimpleFeatureType type;
	private SimpleFeatureBuilder builder;
	private Index index;
	private DataStore dataStore;
	private int minLevels;
	private int maxLevels;
	private int numLevels;
	private int level;
	private int numXPosts;
	private int numYPosts;
	private FeatureDataAdapter adapter;
	private String statsName;

	@Override
	protected void reduce(
			final DoubleWritable key,
			final Iterable<LongWritable> values,
			final Context context )
			throws IOException,
			InterruptedException {
		if (key.get() < 0) {
			final double prevMax = -key.get();
			if (prevMax > max) {
				max = prevMax;
			}
		}
		else {
			final double normalized = key.get() / max;
			// calculate weights for this key
			for (final LongWritable v : values) {
				final long cellIndex = v.get() / numLevels;
				final Point2d[] bbox = fromIndexToLL_UR(cellIndex);
				builder.add(GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
						bbox[0].x,
						bbox[1].x,
						bbox[0].y,
						bbox[1].y)));
				builder.add(key.get());
				builder.add(normalized);
				inc += (1.0 / totalKeys);
				builder.add(inc);
				final SimpleFeature feature = builder.buildFeature(new Long(
						cellIndex).toString());
				dataStore.ingest(
						adapter,
						index,
						feature);
			}
		}
	}

	private Point2d[] fromIndexToLL_UR(
			final long index ) {
		final double llLon = ((Math.floor(index / numYPosts) * 360.0) / numXPosts) - 180.0;
		final double llLat = (((index % numYPosts) * 180.0) / numYPosts) - 90.0;
		final double urLon = llLon + (360.0 / numXPosts);
		final double urLat = llLat + (180.0 / numYPosts);
		return new Point2d[] {
			new Point2d(
					llLon,
					llLat),
			new Point2d(
					urLon,
					urLat)
		};
	}

	@Override
	protected void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		minLevels = context.getConfiguration().getInt(
				KDEJobRunner.MIN_LEVEL_KEY,
				1);
		maxLevels = context.getConfiguration().getInt(
				KDEJobRunner.MAX_LEVEL_KEY,
				25);
		statsName = context.getConfiguration().get(
				STATS_NAME_KEY,
				"");
		numLevels = (maxLevels - minLevels) + 1;
		level = context.getConfiguration().getInt(
				"mapred.task.partition",
				0) + minLevels;
		numXPosts = (int) Math.pow(
				2,
				level + 1);
		numYPosts = (int) Math.pow(
				2,
				level);
		type = createFeatureType(getTypeName(
				level,
				statsName));
		builder = new SimpleFeatureBuilder(
				type);
		index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final IndexStore indexStore = new MemoryIndexStore(
				new Index[] {
					index
				});
		adapter = new FeatureDataAdapter(
				type);
		final AdapterStore adapterStore = new MemoryAdapterStore(
				new DataAdapter[] {
					new FeatureDataAdapter(
							type)
				});
		final AccumuloOptions options = new AccumuloOptions();
		options.setPersistDataStatistics(false);
		// TODO consider an in memory statistics store that will write the
		// statistics when the job is completed
		dataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				null,
				new ReducerContextWriterOperations(
						context,
						context.getConfiguration().get(
								KDEJobRunner.TABLE_NAME)),
				options);

		totalKeys = context.getConfiguration().getLong(
				"Entries per level.level" + level,
				10);
	}

	public static String getTypeName(
			final int level,
			final String statsName ) {
		return "l" + level + "_stats" + statsName;
	}

	public static SimpleFeatureType createFeatureType(
			final String typeName ) {
		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(typeName);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Geometry.class).buildDescriptor(
				"geometry"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).buildDescriptor(
				"Value"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).buildDescriptor(
				"Normalized"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).buildDescriptor(
				"Percentile"));
		return simpleFeatureTypeBuilder.buildFeatureType();
	}

}
