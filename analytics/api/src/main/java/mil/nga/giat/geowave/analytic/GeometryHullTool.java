/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import mil.nga.giat.geowave.analytic.clustering.NeighborData;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.core.index.FloatCompareUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math.util.MathUtils;
import org.apache.commons.math3.geometry.Vector;
import org.apache.commons.math3.geometry.euclidean.twod.Euclidean2D;
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.operation.union.UnaryUnionOp;

/**
 * 
 * Set of algorithms to mere hulls and increase the gradient of convexity over
 * hulls.
 * 
 */
public class GeometryHullTool
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(GeometryHullTool.class);

	DistanceFn<Coordinate> distanceFnForCoordinate;
	double concaveThreshold = 1.8;

	public void connect(
			final List<Geometry> geometries ) {

	}

	public DistanceFn<Coordinate> getDistanceFnForCoordinate() {
		return distanceFnForCoordinate;
	}

	public void setDistanceFnForCoordinate(
			final DistanceFn<Coordinate> distanceFnForCoordinate ) {
		this.distanceFnForCoordinate = distanceFnForCoordinate;
	}

	protected double getConcaveThreshold() {
		return concaveThreshold;
	}

	/*
	 * Set the threshold for the concave algorithm
	 */
	protected void setConcaveThreshold(
			final double concaveThreshold ) {
		this.concaveThreshold = concaveThreshold;
	}

	protected static class Edge implements
			Comparable<Edge>
	{
		Coordinate start;
		Coordinate end;
		double distance;
		Edge next, last;
		private TreeSet<NeighborData<Coordinate>> points = null;

		public Edge(
				final Coordinate start,
				final Coordinate end,
				final double distance ) {
			super();
			this.start = start;
			this.end = end;
			this.distance = distance;
		}

		public TreeSet<NeighborData<Coordinate>> getPoints() {
			if (points == null) {
				points = new TreeSet<NeighborData<Coordinate>>();
			}
			return points;
		}

		@Override
		public int compareTo(
				final Edge edge ) {
			return (distance - edge.distance) > 0 ? 1 : -1;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((end == null) ? 0 : end.hashCode());
			result = (prime * result) + ((start == null) ? 0 : start.hashCode());
			return result;
		}

		public void connectLast(
				final Edge last ) {
			this.last = last;
			last.next = this;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final Edge other = (Edge) obj;
			if (end == null) {
				if (other.end != null) {
					return false;
				}
			}
			else if (!end.equals(other.end)) {
				return false;
			}
			if (start == null) {
				if (other.start != null) {
					return false;
				}
			}
			else if (!start.equals(other.start)) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "Edge [start=" + start + ", end=" + end + ", distance=" + distance + "]";
		}

	}

	private Edge createEdgeWithSideEffects(
			final Coordinate start,
			final Coordinate end,
			final Set<Coordinate> innerPoints,
			final TreeSet<Edge> edges ) {
		final Edge newEdge = new Edge(
				start,
				end,
				distanceFnForCoordinate.measure(
						start,
						end));
		innerPoints.remove(newEdge.start);
		innerPoints.remove(newEdge.end);
		edges.add(newEdge);
		return newEdge;
	}

	/*
	 * Generate a concave hull, if possible, given a geometry and a set of
	 * additional points.
	 * 
	 * @param fast expedite processing allowing for some outliers.
	 */

	public Geometry createHullFromGeometry(
			Geometry clusterGeometry,
			Collection<Coordinate> additionalPoints,
			boolean fast ) {

		if (additionalPoints.isEmpty()) return clusterGeometry;
		final Set<Coordinate> batchCoords = new HashSet<Coordinate>();

		if (clusterGeometry != null) {
			for (final Coordinate coordinate : clusterGeometry.getCoordinates()) {
				batchCoords.add(coordinate);
			}
		}
		for (final Coordinate coordinate : additionalPoints) {
			batchCoords.add(coordinate);
		}

		GeometryFactory factory = clusterGeometry == null ? new GeometryFactory() : clusterGeometry.getFactory();
		final Coordinate[] actualCoords = batchCoords.toArray(new Coordinate[batchCoords.size()]);

		if (batchCoords.size() == 2) {
			return factory.createLineString(actualCoords);
		}

		final ConvexHull convexHull = new ConvexHull(
				actualCoords,
				factory);

		final Geometry convexHullGeo = convexHull.getConvexHull();

		try {
			// does this shape benefit from concave hulling?
			// it cannot be a line string
			if (batchCoords.size() > 5 && convexHullGeo.getArea() > 0.0) {
				final Geometry concaveHull = fast ? concaveHull(
						convexHullGeo,
						batchCoords) : this.concaveHullParkOhMethod(
						convexHullGeo,
						batchCoords);
				if (fast && !concaveHull.isSimple()) {

					LOGGER.warn(
							"Produced non simple hull",
							concaveHull.toText());
					return this.concaveHullParkOhMethod(
							convexHullGeo,
							batchCoords);

				}
				return concaveHull;
			}
			else {
				return convexHullGeo;
			}
		}
		catch (final Exception ex) {

			/*
			 * Geometry[] points = new Geometry[actualCoords.length + 1]; for
			 * (int i = 0; i < actualCoords.length; i++) points[i] =
			 * hull.getFactory().createPoint( actualCoords[i]);
			 * points[points.length - 1] = hull; try { ShapefileTool.writeShape(
			 * "test_perf_xh", new File( "./targettest_perf_xh"), points); }
			 * catch (IOException e) { e.printStackTrace(); }
			 */
			LOGGER.error(
					"Failed to compute hull",
					ex);

			return convexHullGeo;
		}
	}

	/**
	 * 
	 * Gift unwrapping (e.g. dig) concept, taking a convex hull and a set of
	 * inner points, add inner points to the hull without violating hull
	 * invariants--all points must reside on the hull or inside the hull. Based
	 * on: Jin-Seo Park and Se-Jong Oh.
	 * "A New Concave Algorithm and Concaveness Measure for n-dimensional Datasets"
	 * . Department of Nanobiomedical Science. Dankook University". 2010.
	 * 
	 * Per the paper, N = concaveThreshold
	 * 
	 * @param geometry
	 * @param providedInnerPoints
	 * @return
	 */
	public Geometry concaveHullParkOhMethod(
			final Geometry geometry,
			final Collection<Coordinate> providedInnerPoints ) {

		final Set<Coordinate> innerPoints = new HashSet<Coordinate>(
				providedInnerPoints);
		final TreeSet<Edge> edges = new TreeSet<Edge>();
		final Coordinate[] geoCoordinateList = geometry.getCoordinates();
		final int s = geoCoordinateList.length - 1;
		final Edge firstEdge = createEdgeWithSideEffects(
				geoCoordinateList[0],
				geoCoordinateList[1],
				innerPoints,
				edges);
		Edge lastEdge = firstEdge;
		for (int i = 1; i < s; i++) {
			final Edge newEdge = createEdgeWithSideEffects(
					geoCoordinateList[i],
					geoCoordinateList[i + 1],
					innerPoints,
					edges);
			newEdge.connectLast(lastEdge);
			lastEdge = newEdge;
		}
		firstEdge.connectLast(lastEdge);
		while (!edges.isEmpty() && !innerPoints.isEmpty()) {
			final Edge edge = edges.pollLast();
			lastEdge = edge;
			double score = Double.MAX_VALUE;
			Coordinate selectedCandidate = null;
			for (final Coordinate candidate : innerPoints) {
				final double dist = calcDistance(
						edge.start,
						edge.end,
						candidate);
				// on the hull
				if (MathUtils.equals(
						dist,
						0.0,
						0.000000001)) {
					score = 0.0;
					selectedCandidate = candidate;
					break;
				}
				if ((dist > 0) && (dist < score)) {
					score = dist;
					selectedCandidate = candidate;
				}
			}
			if (selectedCandidate == null) {
				continue;
			}
			// if one a line segment of the hull, then remove candidate
			if (FloatCompareUtils.checkDoublesEqual(
					score,
					0.0)) {
				innerPoints.remove(selectedCandidate);
				edges.add(edge);
				continue;
			}
			// Park and Oh look only at the neighbor edges
			// but this fails in some cases.
			if (isCandidateCloserToAnotherEdge(
					score,
					edge,
					edges,
					selectedCandidate)) {
				continue;
			}

			innerPoints.remove(selectedCandidate);
			final double eh = edge.distance;
			final double startToCandidate = distanceFnForCoordinate.measure(
					edge.start,
					selectedCandidate);
			final double endToCandidate = distanceFnForCoordinate.measure(
					edge.end,
					selectedCandidate);
			final double min = Math.min(
					startToCandidate,
					endToCandidate);
			// protected against duplicates
			if ((eh / min) > concaveThreshold) {
				final Edge newEdge1 = new Edge(
						edge.start,
						selectedCandidate,
						startToCandidate);
				final Edge newEdge2 = new Edge(
						selectedCandidate,
						edge.end,
						endToCandidate);
				// need to replace this with something more intelligent. This
				// occurs in cases of sharp angles. An angular approach may also
				// work
				// look for an angle to flip in the reverse direction.
				if (!intersectAnotherEdge(
						newEdge1,
						edge) && !intersectAnotherEdge(
						newEdge2,
						edge) && !intersectAnotherEdge(
						newEdge1,
						edge.last) && !intersectAnotherEdge(
						newEdge2,
						edge.next)) {
					edges.add(newEdge2);
					edges.add(newEdge1);
					newEdge1.connectLast(edge.last);
					newEdge2.connectLast(newEdge1);
					edge.next.connectLast(newEdge2);
					lastEdge = newEdge1;
				}
			}
		}
		return geometry.getFactory().createPolygon(
				reassemble(lastEdge));
	}

	/**
	 * 
	 * Gift unwrapping (e.g. dig) concept, taking a convex hull and a set of
	 * inner points, add inner points to the hull without violating hull
	 * invariants--all points must reside on the hull or inside the hull. Based
	 * on: Jin-Seo Park and Se-Jong Oh.
	 * "A New Concave Algorithm and Concaveness Measure for n-dimensional Datasets"
	 * . Department of Nanobiomedical Science. Dankook University". 2010.
	 * 
	 * Per the paper, N = concaveThreshold.
	 * 
	 * This algorithm evaluates remarkably faster than Park and Oh, but the
	 * quality of the result is marginally less. If it is acceptable to have
	 * some small number of points fall outside of the hull and speed is
	 * critical, use this method. The measure of error is difficult to calculate
	 * since it is not directly calculated based on the number of inner points.
	 * Rather, the measure is based on some number of points in proximity the
	 * optimal concave hull.
	 * 
	 * 
	 * @param geometry
	 * @param providedInnerPoints
	 * @return
	 */
	public Geometry concaveHull(
			final Geometry geometry,
			final Collection<Coordinate> providedInnerPoints ) {
		final Set<Coordinate> innerPoints = (providedInnerPoints instanceof Set) ? (Set<Coordinate>) providedInnerPoints
				: new HashSet<Coordinate>(
						providedInnerPoints);
		final TreeSet<Edge> edges = new TreeSet<Edge>();
		final Coordinate[] geoCoordinateList = geometry.getCoordinates();
		final int s = geoCoordinateList.length - 1;
		final Edge firstEdge = createEdgeWithSideEffects(
				geoCoordinateList[0],
				geoCoordinateList[1],
				innerPoints,
				edges);
		Edge lastEdge = firstEdge;
		for (int i = 1; i < s; i++) {
			final Edge newEdge = createEdgeWithSideEffects(
					geoCoordinateList[i],
					geoCoordinateList[i + 1],
					innerPoints,
					edges);
			newEdge.connectLast(lastEdge);
			lastEdge = newEdge;
		}
		firstEdge.connectLast(lastEdge);
		for (final Coordinate candidate : innerPoints) {
			double min = Double.MAX_VALUE;
			Edge bestEdge = null;
			for (final Edge edge : edges) {
				final double dist = calcDistance(
						edge.start,
						edge.end,
						candidate);
				if ((dist > 0) && (dist < min)) {
					min = dist;
					bestEdge = edge;
				}
			}
			if (bestEdge != null) {
				bestEdge.getPoints().add(
						new NeighborData<Coordinate>(
								candidate,
								null,
								min));
			}
		}
		while (!edges.isEmpty()) {
			final Edge edge = edges.pollLast();
			lastEdge = edge;
			NeighborData<Coordinate> candidate = edge.getPoints().pollFirst();
			while (candidate != null) {
				if (!MathUtils.equals(
						candidate.getDistance(),
						0.0,
						0.000000001)) {
					final Coordinate selectedCandidate = candidate.getElement();
					final double eh = edge.distance;
					final double startToCandidate = distanceFnForCoordinate.measure(
							edge.start,
							selectedCandidate);
					final double endToCandidate = distanceFnForCoordinate.measure(
							edge.end,
							selectedCandidate);
					final double min = Math.min(
							startToCandidate,
							endToCandidate);
					// protected against duplicates
					if ((eh / min) > concaveThreshold) {
						final Edge newEdge1 = new Edge(
								edge.start,
								selectedCandidate,
								startToCandidate);
						final Edge newEdge2 = new Edge(
								selectedCandidate,
								edge.end,
								endToCandidate);
						edges.add(newEdge2);
						edges.add(newEdge1);
						newEdge1.connectLast(edge.last);
						newEdge2.connectLast(newEdge1);
						edge.next.connectLast(newEdge2);
						lastEdge = newEdge1;
						for (final NeighborData<Coordinate> otherPoint : edge.getPoints()) {
							final double[] distProfile1 = calcDistanceSegment(
									newEdge1.start,
									newEdge1.end,
									otherPoint.getElement());
							final double[] distProfile2 = calcDistanceSegment(
									newEdge2.start,
									newEdge2.end,
									otherPoint.getElement());
							if (distProfile1[0] >= 0.0 && distProfile1[0] <= 1.0) {
								if (distProfile1[0] < 0.0 || distProfile1[0] > 1.0 || distProfile2[1] > distProfile1[1]) {
									otherPoint.setDistance(distProfile1[1]);
									newEdge1.getPoints().add(
											otherPoint);
								}
								else {
									otherPoint.setDistance(distProfile2[1]);
									newEdge2.getPoints().add(
											otherPoint);
								}
							}
							else if (distProfile2[0] >= 0.0 && distProfile2[0] <= 1.0) {

								otherPoint.setDistance(distProfile2[1]);
								newEdge2.getPoints().add(
										otherPoint);
							}
						}
						edge.getPoints().clear(); // forces this loop to end
					}
				}
				candidate = edge.getPoints().pollFirst();
			}

		}
		return geometry.getFactory().createPolygon(
				reassemble(lastEdge));
	}

	public static boolean intersectAnotherEdge(
			final Edge newEdge,
			final Edge edgeToReplace ) {
		Edge nextEdge = edgeToReplace.next.next;
		final Edge stopEdge = edgeToReplace.last;
		while (nextEdge != stopEdge) {
			if (edgesIntersect(
					newEdge,
					nextEdge)) {
				return true;
			}
			nextEdge = nextEdge.next;
		}
		return false;
	}

	public static boolean edgesIntersect(
			final Edge e1,
			final Edge e2 ) {
		return CGAlgorithms.distanceLineLine(
				e1.start,
				e1.end,
				e2.start,
				e2.end) <= 0.0;
	}

	private static boolean isCandidateCloserToAnotherEdge(
			final double distanceToBeat,
			final Edge selectedEdgeToBeat,
			final Collection<Edge> edges,
			final Coordinate selectedCandidate ) {
		for (final Edge edge : edges) {
			if (selectedEdgeToBeat.equals(edge)) {
				continue;
			}
			final double dist = calcDistance(
					edge.start,
					edge.end,
					selectedCandidate);
			if ((dist >= 0.0) && (dist < distanceToBeat)) {
				return true;
			}
		}
		return false;
	}

	private static Coordinate[] reassemble(
			final Edge lastEdge ) {
		final List<Coordinate> coordinates = new ArrayList<Coordinate>();
		coordinates.add(lastEdge.start);
		Edge nextEdge = lastEdge.next;
		while (nextEdge != lastEdge) {
			coordinates.add(nextEdge.start);
			nextEdge = nextEdge.next;
		}
		coordinates.add(lastEdge.start);
		return coordinates.toArray(new Coordinate[coordinates.size()]);
	}

	protected boolean isInside(
			final Coordinate coor,
			final Coordinate[] hullCoordinates ) {
		double maxAngle = 0;
		for (int i = 1; i < hullCoordinates.length; i++) {
			final Coordinate hullCoordinate = hullCoordinates[i];
			maxAngle = Math.max(
					calcAngle(
							hullCoordinates[0],
							coor,
							hullCoordinate),
					maxAngle);
		}
		// return 360 == Math.abs(maxAngle);
		return (Math.abs(maxAngle) >= 359.999 && Math.abs(maxAngle) <= 360.0001);
	}

	/**
	 * Forms create edges between two shapes maintaining convexity.
	 * 
	 * Does not currently work if the shapes intersect
	 * 
	 * @param shape1
	 * @param shape2
	 * @return
	 */
	public Geometry connect(
			final Geometry shape1,
			final Geometry shape2 ) {

		try {
			if (shape1 instanceof Polygon && shape2 instanceof Polygon && !shape1.intersects(shape2)) return connect(
					shape1,
					shape2,
					getClosestPoints(
							shape1,
							shape2,
							distanceFnForCoordinate));
			return UnaryUnionOp.union(Arrays.asList(
					shape1,
					shape2));
		}
		catch (Exception ex) {
			LOGGER.warn(
					"Exception caught in connect method",
					ex);
		}
		return createHullFromGeometry(
				shape1,
				Arrays.asList(shape2.getCoordinates()),
				false);
	}

	protected Geometry connect(
			final Geometry shape1,
			final Geometry shape2,
			final Pair<Integer, Integer> closestCoordinates ) {
		Coordinate[] leftCoords = shape1.getCoordinates(), rightCoords = shape2.getCoordinates();
		int startLeft, startRight;
		if ((leftCoords[closestCoordinates.getLeft()].x < rightCoords[closestCoordinates.getRight()].x)) {
			startLeft = closestCoordinates.getLeft();
			startRight = closestCoordinates.getRight();
		}
		else {
			leftCoords = shape2.getCoordinates();
			rightCoords = shape1.getCoordinates();
			startLeft = closestCoordinates.getRight();
			startRight = closestCoordinates.getLeft();
		}
		final HashSet<Coordinate> visitedSet = new HashSet<Coordinate>();

		visitedSet.add(leftCoords[startLeft]);
		visitedSet.add(rightCoords[startRight]);

		final boolean leftClockwise = clockwise(leftCoords);
		final boolean rightClockwise = clockwise(rightCoords);

		final Pair<Integer, Integer> upperCoords = walk(
				visitedSet,
				leftCoords,
				rightCoords,
				startLeft,
				startRight,
				new DirectionFactory() {

					@Override
					public Direction createLeftFootDirection(
							final int start,
							final int max ) {
						return leftClockwise ? new IncreaseDirection(
								start,
								max,
								true) : new DecreaseDirection(
								start,
								max,
								true);
					}

					@Override
					public Direction createRightFootDirection(
							final int start,
							final int max ) {
						return rightClockwise ? new DecreaseDirection(
								start,
								max,
								false) : new IncreaseDirection(
								start,
								max,
								false);
					}

				});

		final Pair<Integer, Integer> lowerCoords = walk(
				visitedSet,
				leftCoords,
				rightCoords,
				startLeft,
				startRight,
				new DirectionFactory() {

					@Override
					public Direction createLeftFootDirection(
							final int start,
							final int max ) {
						return leftClockwise ? new DecreaseDirection(
								start,
								max,
								false) : new IncreaseDirection(
								start,
								max,
								false);
					}

					@Override
					public Direction createRightFootDirection(
							final int start,
							final int max ) {
						return rightClockwise ? new IncreaseDirection(
								start,
								max,
								true) : new DecreaseDirection(
								start,
								max,
								true);
					}

				});

		final List<Coordinate> newCoordinateSet = new ArrayList<Coordinate>();
		final Direction leftSet = leftClockwise ? new IncreaseDirection(
				upperCoords.getLeft(),
				lowerCoords.getLeft() + 1,
				leftCoords.length) : new DecreaseDirection(
				upperCoords.getLeft(),
				lowerCoords.getLeft() - 1,
				leftCoords.length);
		newCoordinateSet.add(leftCoords[upperCoords.getLeft()]);
		while (leftSet.hasNext()) {
			newCoordinateSet.add(leftCoords[leftSet.next()]);
		}
		final Direction rightSet = rightClockwise ? new IncreaseDirection(
				lowerCoords.getRight(),
				upperCoords.getRight() + 1,
				rightCoords.length) : new DecreaseDirection(
				lowerCoords.getRight(),
				upperCoords.getRight() - 1,
				rightCoords.length);
		newCoordinateSet.add(rightCoords[lowerCoords.getRight()]);
		while (rightSet.hasNext()) {
			newCoordinateSet.add(rightCoords[rightSet.next()]);
		}
		newCoordinateSet.add(leftCoords[upperCoords.getLeft()]);
		return shape1.getFactory().createPolygon(
				newCoordinateSet.toArray(new Coordinate[newCoordinateSet.size()]));
	}

	private Pair<Integer, Integer> walk(
			final Set<Coordinate> visited,
			final Coordinate[] shape1Coords,
			final Coordinate[] shape2Coords,
			final int start1,
			final int start2,
			final DirectionFactory factory ) {

		final int upPos = takeBiggestStep(
				visited,
				shape2Coords[start2],
				shape1Coords,
				factory.createLeftFootDirection(
						start1,
						shape1Coords.length));

		// even if the left foot was stationary, try to move the right foot
		final int downPos = takeBiggestStep(
				visited,
				shape1Coords[upPos],
				shape2Coords,
				factory.createRightFootDirection(
						start2,
						shape2Coords.length));

		// if the right step moved, then see if another l/r step can be taken
		if (downPos != start2) {
			return walk(
					visited,
					shape1Coords,
					shape2Coords,
					upPos,
					downPos,
					factory);
		}
		return Pair.of(
				upPos,
				start2);
	}

	/**
	 * Determine if the polygon is defined clockwise
	 * 
	 * @param set
	 * @return
	 */
	public static boolean clockwise(
			final Coordinate[] set ) {
		double sum = 0.0;
		for (int i = 1; i < set.length; i++) {
			sum += (set[i].x - set[i - 1].x) / (set[i].y + set[i - 1].y);
		}
		return sum > 0.0;
	}

	public static double calcSmallestAngle(
			final Coordinate one,
			final Coordinate vertex,
			final Coordinate two ) {
		final double angle = Math.abs(calcAngle(
				one,
				vertex,
				two));
		return (angle > 180.0) ? angle - 180.0 : angle;
	}

	/**
	 * Calculate the angle between two points and a given vertex
	 * 
	 * @param one
	 * @param vertex
	 * @param two
	 * @return
	 */
	public static double calcAngle(
			final Coordinate one,
			final Coordinate vertex,
			final Coordinate two ) {

		final double p1x = one.x - vertex.x;
		final double p1y = one.y - vertex.y;
		final double p2x = two.x - vertex.x;
		final double p2y = two.y - vertex.y;

		final double angle1 = Math.toDegrees(Math.atan2(
				p1y,
				p1x));
		final double angle2 = Math.toDegrees(Math.atan2(
				p2y,
				p2x));
		return angle2 - angle1;
	}

	/**
	 * Calculate the distance between two points and a given vertex
	 * 
	 * @param one
	 * @param vertex
	 * @param two
	 * @return array if doubles double[0] = length of the projection from start
	 *         on the line containing the segment(start to end) double[1] =
	 *         distance to the segment double[2] = distance to the line
	 *         containing the segment(start to end)
	 */
	public static double[] calcDistanceSegment(
			final Coordinate start,
			final Coordinate end,
			final Coordinate point ) {

		final Vector<Euclidean2D> vOne = new Vector2D(
				start.x,
				start.y);

		final Vector<Euclidean2D> vTwo = new Vector2D(
				end.x,
				end.y);

		final Vector<Euclidean2D> vVertex = new Vector2D(
				point.x,
				point.y);

		final Vector<Euclidean2D> E1 = vTwo.subtract(vOne);

		final Vector<Euclidean2D> E2 = vVertex.subtract(vOne);

		final double distOneTwo = E2.dotProduct(E1);
		final double lengthVOneSq = E1.getNormSq();
		final double projectionLength = distOneTwo / lengthVOneSq;
		final Vector<Euclidean2D> projection = E1.scalarMultiply(
				projectionLength).add(
				vOne);
		final double o = ((projectionLength < 0.0) ? vOne.distance(vVertex) : ((projectionLength > 1.0) ? vTwo
				.distance(vVertex) : vVertex.distance(projection)));

		return new double[] {
			projectionLength,
			o,
			vVertex.distance(projection)
		};
	}

	public static double calcDistance(
			final Coordinate start,
			final Coordinate end,
			final Coordinate point ) {
		double[] p = calcDistanceSegment(
				start,
				end,
				point);
		return (p[0] < 0.0 || p[0] > 1.0) ? -1 : p[1];
	}

	public static Pair<Integer, Integer> getClosestPoints(
			final Geometry shape1,
			final Geometry shape2,
			final DistanceFn<Coordinate> distanceFnForCoordinate ) {
		int bestShape1Position = 0;
		int bestShape2Position = 0;
		double minDist = Double.MAX_VALUE;
		int pos1 = 0, pos2 = 0;
		for (final Coordinate coord1 : shape1.getCoordinates()) {
			pos2 = 0;
			for (final Coordinate coord2 : shape2.getCoordinates()) {
				final double dist = (distanceFnForCoordinate.measure(
						coord1,
						coord2));
				if (dist < minDist) {
					bestShape1Position = pos1;
					bestShape2Position = pos2;
					minDist = dist;
				}
				pos2++;
			}
			pos1++;
		}
		return Pair.of(
				bestShape1Position,
				bestShape2Position);

	}

	private int takeBiggestStep(
			final Set<Coordinate> visited,
			final Coordinate station,
			final Coordinate[] shapeCoords,
			final Direction legIncrement ) {
		double angle = 0.0;
		final Coordinate startPoint = shapeCoords[legIncrement.getStart()];
		int last = legIncrement.getStart();
		Coordinate lastCoordinate = shapeCoords[last];
		while (legIncrement.hasNext()) {
			final int pos = legIncrement.next();
			// skip over duplicate (a ring or polygon has one duplicate)
			if (shapeCoords[pos].equals(lastCoordinate)) {
				continue;
			}
			lastCoordinate = shapeCoords[pos];
			if (visited.contains(lastCoordinate)) {
				break;
			}
			double currentAngle = legIncrement.angleChange(calcAngle(
					startPoint,
					station,
					lastCoordinate));
			currentAngle = currentAngle < -180 ? currentAngle + 360 : currentAngle;
			if ((currentAngle >= angle) && (currentAngle < 180.0)) {
				angle = currentAngle;
				last = pos;
				visited.add(shapeCoords[pos]);
			}
			else {
				return last;
			}
		}
		return last;
	}

	private interface DirectionFactory
	{
		Direction createLeftFootDirection(
				int start,
				int max );

		Direction createRightFootDirection(
				int start,
				int max );
	}

	private interface Direction extends
			Iterator<Integer>
	{
		public int getStart();

		public double angleChange(
				double angle );
	}

	private class IncreaseDirection implements
			Direction
	{

		final int max;
		final int start;
		final int stop;
		int current = 0;
		final boolean angleIsNegative;

		@Override
		public int getStart() {
			return start;
		}

		public IncreaseDirection(
				final int start,
				final int max,
				final boolean angleIsNegative ) {
			super();
			this.max = max;
			current = getNext(start);
			stop = start;
			this.start = start;
			this.angleIsNegative = angleIsNegative;
		}

		public IncreaseDirection(
				final int start,
				final int stop,
				final int max ) {
			super();
			this.max = max;
			current = getNext(start);
			this.stop = stop;
			this.start = start;
			angleIsNegative = true;
		}

		@Override
		public Integer next() {
			final int n = current;
			current = getNext(current);
			return n;
		}

		@Override
		public boolean hasNext() {
			return current != stop;
		}

		protected int getNext(
				final int n ) {
			return (n + 1) % max;
		}

		@Override
		public void remove() {}

		@Override
		public double angleChange(
				final double angle ) {
			return angleIsNegative ? -angle : angle;
		}
	}

	private class DecreaseDirection extends
			IncreaseDirection implements
			Direction
	{

		public DecreaseDirection(
				final int start,
				final int max,
				final boolean angleIsNegative ) {
			super(
					start,
					max,
					angleIsNegative);
		}

		public DecreaseDirection(
				final int start,
				final int stop,
				final int max ) {
			super(
					start,
					stop,
					max);
		}

		@Override
		protected int getNext(
				final int n ) {
			return (n == 0) ? max - 1 : n - 1;
		}

	}
}
