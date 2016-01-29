package mil.nga.giat.geowave.adapter.vector;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.adapter.vector.field.SimpleFeatureSerializationProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class WholeFeatureDataAdapter extends
		KryoFeatureDataAdapter
{
	protected WholeFeatureDataAdapter() {
		super();
		// TODO Auto-generated constructor stub
	}

	public WholeFeatureDataAdapter(
			SimpleFeatureType featureType ) {
		super(
				featureType);
		// TODO Auto-generated constructor stub
	}

	SimpleFeatureBuilder b;

	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		return (FieldReader) new SimpleFeatureSerializationProvider.WholeFeatureReader(
				featureType);
	}

	@Override
	public FieldWriter<SimpleFeature, Object> getWriter(
			final ByteArrayId fieldId ) {
		return (FieldWriter) new SimpleFeatureSerializationProvider.WholeFeatureWriter();
	}

	@Override
	public SimpleFeature decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		final PersistentValue<Object> obj = data.getAdapterExtendedData().getValues().get(
				0);
		final byte[][] bytes = (byte[][]) obj.getValue();
		int i = 0;
		final SimpleFeatureBuilder bldr = getBuilder();
		for (final byte[] f : bytes) {
			if (f != null) {
				final FieldReader reader = FieldUtils.getDefaultReaderForClass(featureType.getType(
						i).getBinding());

				bldr.set(
						i,
						reader.readField(f));
			}
			i++;
		}
		return bldr.buildFeature(data.getDataId().getString());
	}

	private synchronized SimpleFeatureBuilder getBuilder() {
		if (b == null) {
			b = new SimpleFeatureBuilder(
					featureType);
		}
		return b;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final SimpleFeature entry,
			final CommonIndexModel indexModel ) {
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		extendedData.addValue(new PersistentValue<Object>(
				new ByteArrayId(
						""),
				entry.getAttributes().toArray(
						new Object[] {})));
		AdapterPersistenceEncoding encoding = super.encode(
				entry,
				indexModel);
		return new WholeFeatureAdapterEncoding(
				getAdapterId(),
				getDataId(entry),
				encoding.getCommonData(),
				extendedData);
	}

}
