package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ComparisonIdentityMapper extends
		Mapper<ComparisonCellData, LongWritable, ComparisonCellData, LongWritable>
{

	@Override
	protected void map(
			final ComparisonCellData key,
			final LongWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		context.write(
				key,
				value);
	}

}
