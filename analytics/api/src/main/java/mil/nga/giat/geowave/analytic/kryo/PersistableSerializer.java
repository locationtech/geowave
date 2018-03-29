package mil.nga.giat.geowave.analytic.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;

public class PersistableSerializer extends
		Serializer<Persistable>
{

	@Override
	public Persistable read(
			Kryo kryo,
			Input input,
			Class<Persistable> classTag ) {

		// Read object byte count and allocate buffer to read object data
		int byteCount = input.readInt();
		byte[] bytes = new byte[byteCount];
		int bytesRead = input.read(bytes);
		// TODO: This was only added for findbugs warning, not really necessary
		// check
		if (bytesRead < 0) {
			return null;
		}

		return PersistenceUtils.fromBinary(bytes);
	}

	@Override
	public void write(
			Kryo kryo,
			Output output,
			Persistable object ) {

		// Persistence utils includes classId as short in front of persistable
		// object.
		byte[] serializedObj = PersistenceUtils.toBinary(object);
		int objLength = serializedObj.length;
		output.writeInt(objLength);
		output.write(serializedObj);
	}

}