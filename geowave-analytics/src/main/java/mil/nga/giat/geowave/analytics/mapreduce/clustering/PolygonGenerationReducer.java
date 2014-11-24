package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.triangulate.DelaunayTriangulationBuilder;

/*
 * Each reducer handles once cluster worth of points, generates polygons
 */
public class PolygonGenerationReducer extends
		Reducer<IntWritable, Text, Text, Mutation>
{
	final Logger log = LoggerFactory.getLogger(PolygonGenerationReducer.class);

	private List<Edge> connectTheEdges(
			final List<Edge> boundaryEdges ) {
		final double tol = 10e-12;
		final List<Edge> sortedEdges = new ArrayList<Edge>();
		final int numEdges = boundaryEdges.size();
		sortedEdges.add(boundaryEdges.get(0));
		boundaryEdges.remove(0);
		while (sortedEdges.size() < numEdges) {
			final Edge currentEdge = sortedEdges.get(sortedEdges.size() - 1);
			final com.vividsolutions.jts.geom.Coordinate pt2 = currentEdge.pt2;
			for (int ii = 0; ii < boundaryEdges.size(); ii++) {
				final Edge be = boundaryEdges.get(ii);
				// end to start
				if ((Math.abs(pt2.x - be.pt1.x) + Math.abs(pt2.y - be.pt1.y)) < tol) {
					sortedEdges.add(be);
					boundaryEdges.remove(ii);
					break;
				}
				// end to end, need to flip
				else if ((Math.abs(pt2.x - be.pt2.x) + Math.abs(pt2.y - be.pt2.y)) < tol) {
					final com.vividsolutions.jts.geom.Coordinate temp = be.pt1;
					be.pt1 = be.pt2;
					be.pt2 = temp;

					sortedEdges.add(be);
					boundaryEdges.remove(ii);
					break;
				}
			}
		}

		return sortedEdges;
	}

	/*
	 * each reducer processes a single cluster worth of points
	 */
	@Override
	public void reduce(
			final IntWritable key,
			final Iterable<Text> values,
			final Context context )
			throws IOException,
			InterruptedException {
		final String outputRowId = context.getConfiguration().get(
				"outputRowId");

		try {
			final WKTReader wktReader = new WKTReader();

			final Integer centroidId = key.get();

			final List<Coordinate> coords = new ArrayList<Coordinate>();
			for (final Text value : values) {
				String ptStr = value.toString();
				ptStr = ptStr.substring(
						1,
						ptStr.length() - 1);
				final String[] splits = ptStr.split(",");
				final Coordinate coord = new Coordinate(
						Double.parseDouble(splits[0]),
						Double.parseDouble(splits[1]));
				coords.add(coord);
			}

			// use JTS to perform Delaunay Triangulation to get the
			// characteristic shapes of the clusters
			final DelaunayTriangulationBuilder dtb = new DelaunayTriangulationBuilder();
			dtb.setSites(coords);
			final MultiLineString edges = (MultiLineString) dtb.getEdges(new GeometryFactory());

			// extract boundary edges by finding the edges that only belong to a
			// single triangle
			final List<Edge> myEdges = new ArrayList<Edge>();
			MultiLineString wktEdges;
			wktEdges = (MultiLineString) wktReader.read(edges.toText());
			final int numEdges = wktEdges.getNumGeometries();
			for (int idx = 0; idx < numEdges; idx++) {
				final Geometry wktEdge = wktEdges.getGeometryN(idx);
				final com.vividsolutions.jts.geom.Coordinate[] coordinates = wktEdge.getCoordinates();

				// add to the master edge list
				myEdges.add(new Edge(
						coordinates[0],
						coordinates[1]));
			}

			final GeometryCollection triangles = (GeometryCollection) dtb.getTriangles(new GeometryFactory());
			for (int tt = 0; tt < triangles.getNumGeometries(); tt++) {
				final Geometry triangle = triangles.getGeometryN(tt);
				final com.vividsolutions.jts.geom.Coordinate[] vertices = triangle.getCoordinates();

				// go through the edges of this polygon
				for (int idx = 1; idx < vertices.length; idx++) {
					for (final Edge myEdge : myEdges) {
						myEdge.tallyEdge(
								vertices[idx],
								vertices[idx - 1]);
					}
				}
			}

			// edges of count 1 means that they belong to only one triangle, all
			// internal edges have a count of 2
			final List<Edge> boundaryEdges = new ArrayList<Edge>();
			for (final Edge myEdge : myEdges) {
				if (myEdge.count == 1) {
					boundaryEdges.add(myEdge);
				}
			}

			// sort the edges so they are connected in a continuous line and add
			// them to the list
			com.vividsolutions.jts.geom.Polygon sortedPolygon = null;
			if (boundaryEdges.size() > 0) {
				final List<Edge> sortedEdges = connectTheEdges(boundaryEdges);

				final Coordinate[] coordinateArray = new Coordinate[sortedEdges.size() + 1];
				int coordIdx = 0;
				for (final Edge sortedEdge : sortedEdges) {
					coordinateArray[coordIdx] = new Coordinate(
							sortedEdge.pt1.x,
							sortedEdge.pt1.y);
					coordIdx++;
				}
				final Edge lastSortedEdge = sortedEdges.get(sortedEdges.size() - 1);
				coordinateArray[sortedEdges.size()] = new Coordinate(
						lastSortedEdge.pt2.x,
						lastSortedEdge.pt2.y);
				final CoordinateArraySequence cas = new CoordinateArraySequence(
						coordinateArray);
				final com.vividsolutions.jts.geom.LinearRing linearRing = new com.vividsolutions.jts.geom.LinearRing(
						cas,
						new GeometryFactory());
				sortedPolygon = new com.vividsolutions.jts.geom.Polygon(
						linearRing,
						null,
						new GeometryFactory());
			}

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
							wkbWriter.write(sortedPolygon)));
			context.write(
					null,
					m);
		}
		catch (final ParseException e) {
			log.error(
					"Polygon generation reducer: " + e.getMessage(),
					e);
		}
	}
}
