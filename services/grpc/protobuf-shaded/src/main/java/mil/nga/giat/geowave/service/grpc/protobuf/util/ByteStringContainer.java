package mil.nga.giat.geowave.service.grpc.protobuf.util;

import com.google.protobuf.ByteString;

//exported class for copying ByteString
public class ByteStringContainer
{

	public static ByteString copyFrom(
			byte[] bytes ) {
		return ByteString.copyFrom(bytes);
	}

	protected ByteString bs;

	public ByteStringContainer(
			byte[] bytes ) {
		bs = ByteString.copyFrom(bytes);
	}

	public ByteString get() {
		return bs;
	}
}
