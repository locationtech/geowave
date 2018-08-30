package mil.nga.giat.geowave.core.store.entities;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

public abstract class MergeableGeoWaveRow implements
		GeoWaveRow
{

	final List<GeoWaveValue> attributeValues;

	public MergeableGeoWaveRow(
			GeoWaveValue[] attributeValues ) {
		this.attributeValues = Lists.newArrayList(attributeValues);
	}

	@Override
	public final GeoWaveValue[] getFieldValues() {
		return attributeValues.toArray(new GeoWaveValue[attributeValues.size()]);
	}

	public void mergeRow(
			MergeableGeoWaveRow row ) {
		Collections.addAll(
				attributeValues,
				row.getFieldValues());
		mergeRowInternal(row);
	}

	// In case any extending classes want to do something when rows are merged
	protected void mergeRowInternal(
			MergeableGeoWaveRow row ) {};

	public boolean shouldMerge(
			GeoWaveRow row ) {
		return (this.getInternalAdapterId() == row.getInternalAdapterId()) && Arrays.equals(
				this.getDataId(),
				row.getDataId()) && Arrays.equals(
				this.getPartitionKey(),
				row.getPartitionKey()) && Arrays.equals(
				this.getSortKey(),
				row.getSortKey()) && (this.getNumberOfDuplicates() == row.getNumberOfDuplicates());
	}

}
