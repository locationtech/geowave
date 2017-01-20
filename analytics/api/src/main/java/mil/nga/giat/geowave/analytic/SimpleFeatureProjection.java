package mil.nga.giat.geowave.analytic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Assumes two-dimensional simple feature without time dimensions.
 * 
 */
public class SimpleFeatureProjection implements
		Projection<SimpleFeature>
{

	@Override
	public Geometry getProjection(
			final SimpleFeature anItem ) {
		return (Geometry) anItem.getDefaultGeometry();
	}

	@Override
	public void initialize(
			final JobContext context,
			final Class<?> scope )
			throws IOException {}

	public void setup(
			final PropertyManagement runTimeProperties,
			final Class<?> scope,
			final Configuration configuration ) {}
}
