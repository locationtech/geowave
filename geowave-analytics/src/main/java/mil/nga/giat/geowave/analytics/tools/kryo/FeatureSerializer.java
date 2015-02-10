package mil.nga.giat.geowave.analytics.tools.kryo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import mil.nga.giat.geowave.vector.adapter.FeatureWritable;

import org.opengis.feature.simple.SimpleFeature;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FeatureSerializer extends
		Serializer<SimpleFeature>
{

	@Override
	public SimpleFeature read(
			final Kryo arg0,
			final Input arg1,
			final Class<SimpleFeature> arg2 ) {
		final FeatureWritable fw = new FeatureWritable();
		final byte[] data = new byte[arg1.readInt()];
		arg1.read(data);
		try (DataInputStream is = new DataInputStream(
				new ByteArrayInputStream(
						data))) {
			fw.readFields(is);
		}
		catch (final IOException e) {
			e.printStackTrace();
			return null;
		}
		return fw.getFeature();
	}

	@Override
	public void write(
			final Kryo arg0,
			final Output arg1,
			final SimpleFeature arg2 ) {
		final FeatureWritable fw = new FeatureWritable(arg2.getFeatureType());
		fw.setFeature(arg2);
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (DataOutputStream os = new DataOutputStream(
				bos)) {
			fw.write(os);
			os.flush();
			final byte[] data = bos.toByteArray();
			arg1.writeInt(data.length);
			arg1.write(data);
		}
		catch (final IOException e) {
			e.printStackTrace();
		}
	}

}