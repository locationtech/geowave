package mil.nga.giat.geowave.analytics.mapreduce.clustering;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.vividsolutions.jts.geom.Coordinate;

public class KMeansReducer {

	public static class Reduce extends Reducer<IntWritable, Text, Text, Mutation>
	{		
		/*
		 * Reducer:
		 *  - each reducer represents one centroid
		 *  - iterate through all input points, calculate new centroid location
		 *   - write new centroids to GeoWave with new adapter (current iteration encoded inside)
		 */

		public void  reduce(IntWritable assignedCentroidId, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			System.out.println("K-Means, Reducing...");
			
			Integer centroidId = assignedCentroidId.get();
			
			String runId = context.getConfiguration().get("run.id");
			String iter = context.getConfiguration().get("iteration.number");
			
			double totalX = 0.0, totalY = 0.0, ptCount = 0.0;
			
			for(Text value : values)
			{
				String ptStr = value.toString();
				ptStr = ptStr.substring(1, ptStr.length() -1);
				String[] splits = ptStr.split(",");
				
				double x = Double.parseDouble(splits[0]);
				double y = Double.parseDouble(splits[1]);
				
				totalX += x;
				totalY += y;
				
				ptCount += 1.0;
			}
			
			double avgX = totalX / ptCount;
			double avgY = totalY / ptCount;
			
			Coordinate coord = new Coordinate(avgX, avgY);
			
			System.out.println("runid: " + runId + ", iter: " + iter + ", centroidId: " + centroidId + ", centroid: " + coord.toString());
			
			/*
			 *  writes to the centroid keeper row
			 *  
			 *  run_id | iter# | centroid_id | pt_Str (x,y)
			 */
			Mutation m = new Mutation(context.getConfiguration().get("run.id"));
			m.put(new Text(iter), new Text(centroidId.toString()), new Value(coord.toString().getBytes()));
			context.write(new Text(context.getConfiguration().get("kmeans.table")), m);				
		}
	}
	
}
