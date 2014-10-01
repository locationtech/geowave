package mil.nga.giat.geowave.ingest.hdfs.mapreduce;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloUtils;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.mapreduce.Job;

import com.vividsolutions.jts.geom.Polygon;

public class GeoWaveInputFormat extends AccumuloInputFormat
{
	/*
	 * Method takes in a polygon and generates the corresponding
	 * ranges in a GeoWave spatial index
	 */
	protected static List<ByteArrayRange> getGeoWaveRangesForQuery(
			final Polygon polygon ) {

		final Index index = IndexType.SPATIAL.createDefaultIndex();
		final List<ByteArrayRange> ranges = index.getIndexStrategy().getQueryRanges(
				new SpatialQuery(
						polygon).getIndexConstraints(index.getIndexStrategy()));

		return ranges;
	}
	
	/*
	 * Method generates and sets the ranges corresponding to the polygon in the input format
	 */
	public static void setRangesForPolygon(Job job, Polygon polygon)
	{
		List<ByteArrayRange> byteRanges = getGeoWaveRangesForQuery(polygon);
		
		/*
		 * TODO group ranges to fit maximum allowable number of mappers	
		 *  - needs ability to know number of entries in each bytearrayrange
		 *  - needs ability to break up range into sub ranges in order to segment the ranges for mappers
		 *  - perhaps put this logic in an overridden getSplits() method?	
		 */
		final List<Range> ranges = new ArrayList<Range>();
		for (final ByteArrayRange byteRange : byteRanges) {
			ranges.add(AccumuloUtils.byteArrayRangeToAccumuloRange(byteRange));
		}
		
		setRanges(
				job,
				ranges);
		
		// one range per mapper
		setAutoAdjustRanges(
				job,
				false);
	}
	
}
