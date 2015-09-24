package mil.nga.giat.geowave.format.stanag4676.parser.model;

import java.util.ArrayList;
import java.util.List;

public class Area
{

	/**
	 * points is an ordered list defining an area 3 or more points define a
	 * polygon 2 points define a circle, the first being the center of the
	 * circle and the second being a point along the circumference. The radius
	 * of the circle would be the distance between the two points.
	 * 
	 */
	public List<GeodeticPosition> points = new ArrayList<GeodeticPosition>();

	public List<GeodeticPosition> getPoints() {
		return this.points;
	}

	public void setPoints(
			List<GeodeticPosition> points ) {
		this.points = points;
	}
}
