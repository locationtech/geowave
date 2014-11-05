package mil.nga.giat.geowave.analytics.mapreduce.kde;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.analytics.CellCounter;
import mil.nga.giat.geowave.analytics.GaussianFilter;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class GaussianCellMapper extends
		Mapper<Key, Value, LongWritable, DoubleWritable>
{
	private final static Logger LOGGER = Logger.getLogger(GaussianCellMapper.class);
	public static final String DATA_ADAPTER_KEY = "DATA_ADAPTER";
	protected static final String CQL_FILTER_KEY = "CQL_FILTER";
	protected DataAdapter<?> adapter;
	protected Index index;
	protected int minLevel;
	protected int maxLevel;
	protected Filter filter;
	protected Map<Integer, LevelStore> levelStoreMap;

	@Override
	protected void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final String adapterStr = context.getConfiguration().get(
				DATA_ADAPTER_KEY);
		final byte[] adapterBytes = ByteArrayUtils.byteArrayFromString(adapterStr);
		adapter = PersistenceUtils.fromBinary(
				adapterBytes,
				DataAdapter.class);
		minLevel = context.getConfiguration().getInt(
				KDEJobRunner.MIN_LEVEL_KEY,
				1);
		maxLevel = context.getConfiguration().getInt(
				KDEJobRunner.MAX_LEVEL_KEY,
				25);
		final String cql = context.getConfiguration().get(
				CQL_FILTER_KEY);
		if ((cql != null) && !cql.isEmpty()) {
			try {
				filter = CQL.toFilter(cql);
			}
			catch (final CQLException e) {
				LOGGER.warn(
						"Unable to parse CQL filter",
						e);
			}
		}
		levelStoreMap = new HashMap<Integer, LevelStore>();
		for (int level = maxLevel; level >= minLevel; level--) {
			final int numXPosts = (int) Math.pow(
					2,
					level + 1);
			final int numYPosts = (int) Math.pow(
					2,
					level);
			populateLevelStore(
					context,
					numXPosts,
					numYPosts,
					level);
		}

	}

	protected void populateLevelStore(
			final Context context,
			final int numXPosts,
			final int numYPosts,
			final int level ) {
		levelStoreMap.put(
				level,
				new LevelStore(
						numXPosts,
						numYPosts,
						new MapContextCellCounter(
								context,
								level,
								minLevel,
								maxLevel)));
	}

	@Override
	protected void map(
			final Key key,
			final Value value,
			final Context context )
			throws IOException,
			InterruptedException {
		final AccumuloRowId rowElements = new AccumuloRowId(
				key);
		if (!new ByteArrayId(
				rowElements.getAdapterId()).equals(adapter.getAdapterId())) {
			return;
		}
		final Object feature = AccumuloUtils.decodeRow(
				key,
				value,
				adapter,
				index);
		Point pt = null;
		SimpleFeature simpleFeature = null;
		if ((feature != null) && (feature instanceof SimpleFeature)) {
			simpleFeature = ((SimpleFeature) feature);
			if ((filter != null) && !filter.evaluate(simpleFeature)) {
				return;
			}
			final Object geomObj = simpleFeature.getDefaultGeometry();
			if ((geomObj != null) && (geomObj instanceof Geometry)) {
				pt = ((Geometry) geomObj).getCentroid();
			}
		}
		if (pt == null || pt.isEmpty()) {
			return;
		}
		for (int level = maxLevel; level >= minLevel; level--) {
			incrementLevelStore(
					level,
					pt,
					simpleFeature);
		}
	}

	protected void incrementLevelStore(
			final int level,
			final Point pt,
			final SimpleFeature feature ) {
		final LevelStore levelStore = levelStoreMap.get(level);
		GaussianFilter.incrementPt(
				pt.getY(),
				pt.getX(),
				levelStore.counter,
				levelStore.numXPosts,
				levelStore.numYPosts);
	}

	public static class LevelStore
	{
		public final int numXPosts;
		public final int numYPosts;
		public final CellCounter counter;

		public LevelStore(
				final int numXPosts,
				final int numYPosts,
				final CellCounter counter ) {
			this.numXPosts = numXPosts;
			this.numYPosts = numYPosts;
			this.counter = counter;
		}
	}

}
