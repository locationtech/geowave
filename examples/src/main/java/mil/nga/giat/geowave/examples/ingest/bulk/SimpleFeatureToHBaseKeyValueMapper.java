package mil.nga.giat.geowave.examples.ingest.bulk;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Coordinate;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseCellGenerator;

public class SimpleFeatureToHBaseKeyValueMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, ImmutableBytesWritable>
{

	private final WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(
			GeonamesSimpleFeatureType.getInstance());
	private final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex();
	private final VisibilityWriter<SimpleFeature> visibilityWriter = new UniformVisibilityWriter<SimpleFeature>(
			new UnconstrainedVisibilityHandler<SimpleFeature, Object>());
	private final HBaseCellGenerator<SimpleFeature> generator = new HBaseCellGenerator<SimpleFeature>(
			adapter,
			index,
			visibilityWriter);
	private SimpleFeature simpleFeature;
	private List<Cell> keyValuePairs;
	private final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
			GeonamesSimpleFeatureType.getInstance());
	private String[] geonamesEntryTokens;
	private String geonameId;
	private double longitude;
	private double latitude;
	private String location;

	@Override
	protected void map(
			final LongWritable key,
			final Text value,
			final Context context )
					throws IOException,
					InterruptedException {

		simpleFeature = parseGeonamesValue(
				value);

		// build Geowave-formatted Accumulo [Key,Value] pairs
		keyValuePairs = generator.constructKeyValuePairs(
				adapter.getAdapterId().getBytes(),
				simpleFeature);

		// output each [Key,Value] pair to shuffle-and-sort phase where we rely
		// on MapReduce to sort by Key
		for (final Cell keyValue : keyValuePairs) {
			context.write(
					new ImmutableBytesWritable(
							CellUtil.cloneRow(
									keyValue)),
					new ImmutableBytesWritable(
							CellUtil.cloneValue(
									keyValue)));
		}
	}

	private SimpleFeature parseGeonamesValue(
			final Text value ) {

		geonamesEntryTokens = value.toString().split(
				"\\t"); // Exported Geonames entries are tab-delimited

		geonameId = geonamesEntryTokens[0];
		location = geonamesEntryTokens[1];
		latitude = Double.parseDouble(
				geonamesEntryTokens[4]);
		longitude = Double.parseDouble(
				geonamesEntryTokens[5]);

		return buildSimpleFeature(
				geonameId,
				longitude,
				latitude,
				location);
	}

	private SimpleFeature buildSimpleFeature(
			final String featureId,
			final double longitude,
			final double latitude,
			final String location ) {

		builder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(
						new Coordinate(
								longitude,
								latitude)));
		builder.set(
				"Latitude",
				latitude);
		builder.set(
				"Longitude",
				longitude);
		builder.set(
				"Location",
				location);

		return builder.buildFeature(
				featureId);
	}

}
