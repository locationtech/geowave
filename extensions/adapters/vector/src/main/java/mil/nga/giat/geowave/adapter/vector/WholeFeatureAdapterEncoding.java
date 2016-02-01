package mil.nga.giat.geowave.adapter.vector;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class WholeFeatureAdapterEncoding extends
		AdapterPersistenceEncoding
{

	public WholeFeatureAdapterEncoding(
			ByteArrayId adapterId,
			ByteArrayId dataId,
			PersistentDataset<CommonIndexValue> commonData,
			PersistentDataset<Object> adapterExtendedData ) {
		super(
				adapterId,
				dataId,
				commonData,
				adapterExtendedData);
	}

	@Override
	public PersistentDataset<CommonIndexValue> getCommonData() {
		return new PersistentDataset<CommonIndexValue>();
	}

	@Override
	public MultiDimensionalNumericData getNumericData(
			NumericDimensionField[] dimensions ) {
		final NumericData[] dataPerDimension = new NumericData[dimensions.length];
		for (int d = 0; d < dimensions.length; d++) {
			dataPerDimension[d] = dimensions[d].getNumericData(commonData.getValue(dimensions[d].getFieldId()));
		}
		return new BasicNumericDataset(
				dataPerDimension);
	}

}
