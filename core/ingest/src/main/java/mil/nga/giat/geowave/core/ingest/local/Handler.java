package mil.nga.giat.geowave.core.ingest.local;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import mil.nga.giat.geowave.core.ingest.s3.S3URLConnection;

public class Handler extends
		URLStreamHandler
{

	@Override
	protected URLConnection openConnection(
			URL url )
			throws IOException {
		return new S3URLConnection(
				url);
	}
}
