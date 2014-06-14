package mil.nga.giat.geowave.ingest.mapreduce.gpx;

import java.io.IOException;

import mil.nga.giat.geowave.index.ByteArrayId;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GPXMapper extends Mapper<Text, BytesWritable, ByteArrayId, Object> {

	public static void main(String[] args) {
	}
	
	
	/* should read in original filename (parse out missions?) + filedata
	 * should output a tracid as the key, and a pointobject as the value
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override 
	protected void map(Text keyin, BytesWritable valin, Context context) throws IOException{
		
	}
	

}
