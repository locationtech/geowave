package mil.nga.giat.geowave.analytic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Project a n-dimensional item into a two-dimensional polygon for convex hull
 * construction.
 * 
 * @param <T>
 */
public interface Projection<T>
{
	public Geometry getProjection(
			T anItem );

	public void initialize(
			JobContext context,
			Class<?> scope )
			throws IOException;

	public void setup(
			PropertyManagement runTimeProperties,
			Class<?> scope,
			Configuration configuration );
}
