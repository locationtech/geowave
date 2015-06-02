package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author viggy Functionality similar to
 *         <code> HadoopWritableSerializationTool </code>
 */
public class HBaseHadoopWritableSerializationTool
{
	private final AdapterStore adapterStore;
	private final Map<ByteArrayId, HBaseHadoopWritableSerializer<Object, Writable>> serializers = new HashMap<ByteArrayId, HBaseHadoopWritableSerializer<Object, Writable>>();
	private final ObjectWritable objectWritable = new ObjectWritable();

	public HBaseHadoopWritableSerializationTool(
			final AdapterStore adapterStore ) {
		super();
		this.adapterStore = adapterStore;
	}

	public AdapterStore getAdapterStore() {
		return adapterStore;
	}

	public DataAdapter<?> getAdapter(
			ByteArrayId adapterId ) {
		return this.adapterStore.getAdapter(adapterId);
	}

	public HBaseHadoopWritableSerializer<Object, Writable> getHadoopWritableSerializerForAdapter(
			final ByteArrayId adapterID ) {

		HBaseHadoopWritableSerializer<Object, Writable> serializer = serializers.get(adapterID);
		if (serializer == null) {
			DataAdapter<?> adapter;
			if ((adapterStore != null) && ((adapter = adapterStore.getAdapter(adapterID)) != null) && (adapter instanceof HBaseHadoopDataAdapter)) {
				serializer = ((HBaseHadoopDataAdapter<Object, Writable>) adapter).createWritableSerializer();
				serializers.put(
						adapterID,
						serializer);
			}
			else {
				serializer = new HBaseHadoopWritableSerializer<Object, Writable>() {
					final ObjectWritable writable = new ObjectWritable();

					@Override
					public ObjectWritable toWritable(
							final Object entry ) {
						writable.set(entry);
						return writable;
					}

					@Override
					public Object fromWritable(
							final Writable writable ) {
						return ((ObjectWritable) writable).get();
					}
				};
			}
		}
		return serializer;
	}

	public ObjectWritable toWritable(
			final ByteArrayId adapterID,
			final Object entry ) {
		if (entry instanceof Writable) {
			objectWritable.set(entry);
		}
		else {
			objectWritable.set(getHadoopWritableSerializerForAdapter(
					adapterID).toWritable(
					entry));
		}
		return objectWritable;
	}

	public Object fromWritable(
			final ByteArrayId adapterID,
			final ObjectWritable writable ) {
		final Object innerObj = writable.get();
		return (innerObj instanceof Writable) ? getHadoopWritableSerializerForAdapter(
				adapterID).fromWritable(
				(Writable) innerObj) : innerObj;
	}

}
