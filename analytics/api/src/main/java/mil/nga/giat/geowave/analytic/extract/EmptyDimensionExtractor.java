package mil.nga.giat.geowave.analytic.extract;

import com.vividsolutions.jts.geom.Geometry;

public abstract class EmptyDimensionExtractor<T> implements
		DimensionExtractor<T>
{

	private static final double[] EMPTY_VAL = new double[0];
	private static final String[] EMPTY_NAME = new String[0];

	@Override
	public double[] getDimensions(
			T anObject ) {
		return EMPTY_VAL;
	}

	@Override
	public String[] getDimensionNames() {
		return EMPTY_NAME;
	}

	@Override
	public abstract Geometry getGeometry(
			T anObject );

	@Override
	public abstract String getGroupID(
			T anObject );

}
