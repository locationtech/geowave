package mil.nga.giat.geowave.test;

import mil.nga.giat.geowave.test.mapreduce.MapReduceTestEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

public class GeoWaveDFSTestEnvironment extends
		MapReduceTestEnvironment
{
	protected static final String HDFS_BASE = new File(
			"./target/hdfs_temp/").getAbsoluteFile().toString();
	protected static final Configuration CONF = getConfiguration();
	protected static MiniDFSCluster HDFS_CLUSTER = null;
	protected static String NAME_NODE = null;

	@BeforeClass
	public static void setupDFS()
			throws IOException {
		FileUtil.fullyDelete(new File(
				HDFS_BASE));
		CONF.set(
				MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
				HDFS_BASE);
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(
				CONF);
		HDFS_CLUSTER = builder.build();
		NAME_NODE = "127.0.0.1" + ":" + HDFS_CLUSTER.getNameNodePort();
	}

	@AfterClass
	public static void shutdownDFS() {
		HDFS_CLUSTER.shutdown();
	}

}
