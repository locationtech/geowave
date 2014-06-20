package mil.nga.giat.geowave.ingest.hdfs;

import org.apache.avro.Schema;

public interface HdfsPluginBase
{
	public Schema getAvroSchemaForHdfsType();

}
