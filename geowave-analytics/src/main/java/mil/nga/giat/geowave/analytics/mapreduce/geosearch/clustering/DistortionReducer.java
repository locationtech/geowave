package mil.nga.giat.geowave.analytics.mapreduce.geosearch.clustering;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Single reducer gets all point expectations, calculate distortion, and writes to accumulo
 */
public class DistortionReducer extends Reducer<IntWritable, DoubleWritable, Text, Mutation>
{		
	public void  reduce(IntWritable assignedCentroidId, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
	{			
		int numDimensions = Integer.parseInt(context.getConfiguration().get("numDimensions"));
//		String runId = context.getConfiguration().get("run.id");
		String clusterCount = context.getConfiguration().get("cluster.count");
		String outputRowId = context.getConfiguration().get("jumpRowId");
		
		double expectation = 0.0;
		int ptCount = 0;
		for(DoubleWritable value : values)
		{
			expectation += value.get();
			ptCount++;
		}
		
		if(ptCount > 0)
		{
			expectation /= ptCount;

			Double distortion = Math.pow(expectation / numDimensions, -(numDimensions / 2));

			// key: jump row id | "DISTORTION" | kk 
			// value: distortion value
			Mutation m = new Mutation(outputRowId);
			m.put(new Text("DISTORTION"), new Text(clusterCount), new Value(distortion.toString().getBytes()));

			// write distortion to accumulo, defaults to table given to AccumuloOutputFormat, in driver
			context.write(null, m);
		}

//		// separate distortion values by creating unique type
//		SimpleFeatureType type = ClusteringUtils.createSimpleFeatureType(runId + "_" + iter + "_distortion"); 		
//		WritableDataAdapter<SimpleFeature> adapter = new FeatureDataAdapter(type);		
//		Index index = IndexType.SPATIAL.createDefaultIndex();
//		
//		double expectation = 0.0;
//		int ptCount = 0;
//		for(DoubleWritable value : values)
//		{
//			expectation += value.get();
//			ptCount++;
//		}
//		
//		if(ptCount > 0)
//		{
//			expectation /= ptCount;
//
//			Double distortion = Math.pow(expectation / numDimensions, -(numDimensions / 2));
//			
//			// output key
//			GeoWaveIngestKey outKey = new GeoWaveIngestKey(index.getId(), adapter.getAdapterId());
//
//			// send it to GeoWave
//			context.write(outKey, new DoubleWritable(distortion));			
//		}
	}
}
