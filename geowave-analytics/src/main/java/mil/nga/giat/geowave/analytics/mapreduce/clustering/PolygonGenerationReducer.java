package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBWriter;

/*
 * Each reducer handles once cluster worth of points, generates polygons
 */
public class PolygonGenerationReducer extends
		Reducer<IntWritable, Text, Text, Mutation>
{
	final Logger log = LoggerFactory.getLogger(PolygonGenerationReducer.class);

	/*
	 * each reducer processes a single cluster worth of points in batches
	 * (streaming) to prevent data overrun
	 */
	@Override
	public void reduce(
			final IntWritable key,
			final Iterable<Text> values,
			final Context context )
			throws IOException,
			InterruptedException {

		/*
		 * Logic inspired by SpatialHadoop convexHullStream method
		 */
		// absolute point cloud limit
		final int pointCloudThreshold = 50000000;

		// limit on new points per convex hull run (batch)
		final int batchThreshold = 10000;

		int batchSizeCounter = 0;
		boolean hasNewPoints = false;

		final String outputRowId = context.getConfiguration().get(
				"outputRowId");

		final Integer centroidId = key.get();

		Geometry hullGeometry = null;
		Coordinate[] batchCoords = new Coordinate[pointCloudThreshold];

		for (final Text value : values) {
			if (batchSizeCounter < batchThreshold) {
				String ptStr = value.toString();
				ptStr = ptStr.substring(
						1,
						ptStr.length() - 1);
				final String[] splits = ptStr.split(",");
				final Coordinate coord = new Coordinate(
						Double.parseDouble(splits[0]),
						Double.parseDouble(splits[1]));

				batchCoords[batchSizeCounter] = coord;
				batchSizeCounter++;

				hasNewPoints = true;

				// contains point count greater than max threshold
				if (batchSizeCounter >= pointCloudThreshold) {
					log.error("Polygon generation reducer: point count exceeds threshold of " + pointCloudThreshold);
					break;
				}
			}
			else {
				final Coordinate[] actualCoords = new Coordinate[batchSizeCounter];
				System.arraycopy(
						batchCoords,
						0,
						actualCoords,
						0,
						batchSizeCounter);

				// generate convex hull for current batch of points
				final ConvexHull convexHull = new ConvexHull(
						actualCoords,
						new GeometryFactory());
				hullGeometry = convexHull.getConvexHull();
				final Coordinate[] hullCoords = hullGeometry.getCoordinates();

				batchCoords = new Coordinate[pointCloudThreshold];
				batchSizeCounter = 0;
				hasNewPoints = false;

				// use current convex hull result to prime next batch
				for (final Coordinate hullCoord : hullCoords) {
					batchCoords[batchSizeCounter] = hullCoord;
					batchSizeCounter++;
				}
			}
		}

		// handle the stragglers
		if (hasNewPoints) {
			final Coordinate[] actualCoords = new Coordinate[batchSizeCounter];
			System.arraycopy(
					batchCoords,
					0,
					actualCoords,
					0,
					batchSizeCounter);

			// generate convex hull for current batch of points
			final ConvexHull convexHull = new ConvexHull(
					actualCoords,
					new GeometryFactory());
			hullGeometry = convexHull.getConvexHull();
		}

		if ((hullGeometry != null) && (hullGeometry instanceof Polygon)) {
			final Polygon polygon = (Polygon) hullGeometry;

			final WKBWriter wkbWriter = new WKBWriter();

			// write back to accumulo, in Well Known Binary format
			// key: outputRowId | "SUMMARY" | centroidId
			// value: WKB Polygon string
			final Mutation m = new Mutation(
					outputRowId);
			m.put(
					new Text(
							"SUMMARY"),
					new Text(
							centroidId.toString()),
					new Value(
							wkbWriter.write(polygon)));
			context.write(
					null,
					m);
		}
	}
}
