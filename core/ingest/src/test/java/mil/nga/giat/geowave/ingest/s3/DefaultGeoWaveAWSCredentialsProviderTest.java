package mil.nga.giat.geowave.ingest.s3;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.Test;

import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.ingest.IngestUtils.URLTYPE;
import mil.nga.giat.geowave.core.ingest.spark.SparkIngestDriver;

public class DefaultGeoWaveAWSCredentialsProviderTest
{
	protected static final String GDELT_INPUT_FILES = "s3://geowave-test/data/gdelt";

	@Test
	public void testAnonymousAccess()
			throws NoSuchFieldException,
			SecurityException,
			IllegalArgumentException,
			IllegalAccessException,
			URISyntaxException,
			IOException {
		IngestUtils.setURLStreamHandlerFactory(URLTYPE.S3);
		SparkIngestDriver sparkDriver = new SparkIngestDriver();
		sparkDriver.initializeS3FS("s3://s3.amazonaws.com");
		Stream<Path> s = Files.list(IngestUtils.setupS3FileSystem(
				GDELT_INPUT_FILES,
				"s3://s3.amazonaws.com"));
		System.err.println(s.count());
	}
}
