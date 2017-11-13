package mil.nga.giat.geowave.core.ingest.local;

/**
 * @author jenshadlich@googlemail.com
 */

public class S3Params
{

	private final String bucket;
	private final String key;

	S3Params(
			String bucket,
			String key ) {
		this.bucket = bucket;
		this.key = key;
	}

	public String getBucket() {
		return bucket;
	}

	public String getKey() {
		return key;
	}

}
