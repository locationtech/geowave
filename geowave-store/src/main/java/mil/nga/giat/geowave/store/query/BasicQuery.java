package mil.nga.giat.geowave.store.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.filter.BasicQueryFilter;
import mil.nga.giat.geowave.store.filter.QueryFilter;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.store.index.Index;

/**
 * The Basic Query class represent a hyper-cube query across all dimensions that
 * match the Constraints passed into the constructor
 */
public class BasicQuery implements
		Query
{
	public static class Constraints
	{
		// these basic queries are tied to NumericDimensionDefinition types, not
		// ideal, but third-parties can and will nned to implement their own
		// queries if they implement their own dimension definitions
		protected final Map<Class<? extends NumericDimensionDefinition>, NumericData> constraintsPerTypeOfDimensionDefinition;

		public Constraints(
				final Map<Class<? extends NumericDimensionDefinition>, NumericData> constraintsPerTypeOfDimensionDefinition ) {
			this.constraintsPerTypeOfDimensionDefinition = constraintsPerTypeOfDimensionDefinition;
		}

		public MultiDimensionalNumericData getIndexConstraints(
				final NumericIndexStrategy indexStrategy ) {
			final NumericDimensionDefinition[] dimensionDefinitions = indexStrategy.getOrderedDimensionDefinitions();
			final NumericData[] dataPerDimension = new NumericData[dimensionDefinitions.length];
			for (int d = 0; d < dimensionDefinitions.length; d++) {
				dataPerDimension[d] = constraintsPerTypeOfDimensionDefinition.get(dimensionDefinitions[d].getClass());
			}
			return new BasicNumericDataset(
					dataPerDimension);
		}
	}

	private final Constraints constraints;

	public BasicQuery(
			final Constraints constraints ) {
		this.constraints = constraints;
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		final DimensionField<?>[] dimensionFields = indexModel.getDimensions();
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		final NumericData[] orderedConstraintsPerDimension = new NumericData[dimensionFields.length];
		for (int d = 0; d < dimensionFields.length; d++) {
			orderedConstraintsPerDimension[d] = constraints.constraintsPerTypeOfDimensionDefinition.get(dimensionFields[d].getBaseDefinition().getClass());
		}
		filters.add(createQueryFilter(

				new BasicNumericDataset(
						orderedConstraintsPerDimension),
				dimensionFields));
		return filters;
	}

	protected QueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final DimensionField<?>[] dimensionFields ) {
		return new BasicQueryFilter(
				constraints,
				dimensionFields);
	}

	@Override
	public boolean isSupported(
			final Index index ) {
		final DimensionField<? extends CommonIndexValue>[] fields = index.getIndexModel().getDimensions();
		final Set<Class<? extends NumericDimensionDefinition>> fieldTypeSet = new HashSet<Class<? extends NumericDimensionDefinition>>();
		// first create a set of the field's base definition types that are
		// within the index model
		for (final DimensionField<? extends CommonIndexValue> field : fields) {
			fieldTypeSet.add(field.getBaseDefinition().getClass());
		}
		// then ensure each of the definition types that is required by these
		// constraints are in the index model
		for (final Class<? extends NumericDimensionDefinition> fieldType : constraints.constraintsPerTypeOfDimensionDefinition.keySet()) {
			if (!fieldTypeSet.contains(fieldType)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public MultiDimensionalNumericData getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		return constraints.getIndexConstraints(indexStrategy);
	}
}
