package mil.nga.giat.geowave.core.ingest.local;

import java.io.IOException;
import java.net.URL;

import org.apache.commons.lang.StringUtils;

/**
 * @author jenshadlich@googlemail.com
 */

public class S3ParamsExtractor
{

	public static S3Params extract(
			URL url )
			throws IOException,
			IllegalArgumentException {

		if (!"s3".equals(url.getProtocol())) {
			throw new IllegalArgumentException(
					"Unsupported protocol '" + url.getProtocol() + "'");
		}

		// bucket
		int index = StringUtils.ordinalIndexOf(
				url.getPath(),
				"/",
				2);
		String bucket = url.getPath().substring(
				1,
				index);

		// key
		String key = url.getPath().substring(
				index + 1);

		return new S3Params(
				bucket,
				key);
	}

}