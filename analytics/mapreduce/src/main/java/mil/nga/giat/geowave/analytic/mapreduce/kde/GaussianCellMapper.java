package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class GaussianCellMapper extends
		Mapper<GeoWaveInputKey, SimpleFeature, LongWritable, DoubleWritable>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GaussianCellMapper.class);
	protected static final String CQL_FILTER_KEY = "CQL_FILTER";
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
					level + 1) * KDEJobRunner.TILE_SIZE;
			final int numYPosts = (int) Math.pow(
					2,
					level) * KDEJobRunner.TILE_SIZE;
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
			final GeoWaveInputKey key,
			final SimpleFeature value,
			final Context context )
			throws IOException,
			InterruptedException {
		Point pt = null;
		if (value != null) {
			if ((filter != null) && !filter.evaluate(value)) {
				return;
			}
			final Object geomObj = value.getDefaultGeometry();
			if ((geomObj != null) && (geomObj instanceof Geometry)) {
				pt = ((Geometry) geomObj).getCentroid();
			}
		}
		if ((pt == null) || pt.isEmpty()) {
			return;
		}
		for (int level = maxLevel; level >= minLevel; level--) {
			incrementLevelStore(
					level,
					pt,
					value);
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
