package mil.nga.giat.geowave.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.data.PersistenceEncoding;
import mil.nga.giat.geowave.store.dimension.DimensionField;
import mil.nga.giat.geowave.store.dimension.GeometryWrapper;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This filter can perform fine-grained acceptance testing (intersection test
 * with a query geometry) with JTS geometry
 * 
 */
public class SpatialQueryFilter extends
		BasicQueryFilter
{
	private Geometry queryGeometry;

	private Set<ByteArrayId> geometryFieldIds;

	protected SpatialQueryFilter() {
		super();
	}

	public SpatialQueryFilter(
			final MultiDimensionalNumericData query,
			final DimensionField<?>[] dimensionDefinitions,
			final Geometry queryGeometry ) {
		this(
				stripGeometry(
						query,
						dimensionDefinitions),
				queryGeometry);

	}

	private SpatialQueryFilter(
			final StrippedGeometry strippedGeometry,
			final Geometry queryGeometry ) {
		super(
				strippedGeometry.strippedQuery,
				strippedGeometry.strippedDimensionDefinitions);
		this.queryGeometry = queryGeometry;
		geometryFieldIds = strippedGeometry.geometryFieldIds;
	}

	private static class StrippedGeometry
	{
		private final MultiDimensionalNumericData strippedQuery;
		private final DimensionField<?>[] strippedDimensionDefinitions;
		private final Set<ByteArrayId> geometryFieldIds;

		public StrippedGeometry(
				final MultiDimensionalNumericData strippedQuery,
				final DimensionField<?>[] strippedDimensionDefinitions,
				final Set<ByteArrayId> geometryFieldIds ) {
			this.strippedQuery = strippedQuery;
			this.strippedDimensionDefinitions = strippedDimensionDefinitions;
			this.geometryFieldIds = geometryFieldIds;
		}
	}

	private static StrippedGeometry stripGeometry(
			final MultiDimensionalNumericData query,
			final DimensionField<?>[] dimensionDefinitions ) {
		final Set<ByteArrayId> geometryFieldIds = new HashSet<ByteArrayId>();
		final List<NumericData> numericDataPerDimension = new ArrayList<NumericData>();
		final List<DimensionField<?>> fields = new ArrayList<DimensionField<?>>();
		final NumericData[] data = query.getDataPerDimension();
		for (int d = 0; d < dimensionDefinitions.length; d++) {
			// if the type on the generic is assignable to geometry then save
			// the field ID for later filtering
			if (isSpatial(dimensionDefinitions[d])) {
				geometryFieldIds.add(dimensionDefinitions[d].getFieldId());
			}
			else {
				numericDataPerDimension.add(data[d]);
				fields.add(dimensionDefinitions[d]);
			}
		}
		return new StrippedGeometry(
				new BasicNumericDataset(
						numericDataPerDimension.toArray(new NumericData[] {})),
				fields.toArray(new DimensionField<?>[] {}),
				geometryFieldIds);
	}

	public static boolean isSpatial(
			final DimensionField<?> d ) {
		final Class<?> commonIndexType = GenericTypeResolver.resolveTypeArgument(
				d.getClass(),
				DimensionField.class);
		return GeometryWrapper.class.isAssignableFrom(commonIndexType);
	}

	@Override
	public boolean accept(
			final PersistenceEncoding persistenceEncoding ) {
		if (queryGeometry == null) {
			return true;
		}
		// we can actually get the geometry for the data and test the
		// intersection of the query geometry with that
		boolean geometryPasses = false;
		for (final ByteArrayId fieldId : geometryFieldIds) {
			final Object geomObj = persistenceEncoding.getCommonData().getValue(
					fieldId);
			if ((geomObj != null) && (geomObj instanceof GeometryWrapper)) {
				final GeometryWrapper geom = (GeometryWrapper) geomObj;
				if (geometryPasses(geom.getGeometry())) {
					geometryPasses = true;
					break;
				}
			}
		}
		if (!geometryPasses) {
			return false;
		}
		if (isSpatialOnly()) {// if this is only a spatial index, return
			// true
			return true;
		}
		// otherwise, if the geometry passes, and there are other dimensions,
		// check the other dimensions
		return super.accept(persistenceEncoding);
	}

	private boolean geometryPasses(
			final Geometry dataGeometry ) {
		if (dataGeometry == null) {
			return false;
		}
		return dataGeometry.intersects(queryGeometry);
	}

	protected boolean isSpatialOnly() {
		return (dimensionFields == null) || (dimensionFields.length == 0);
	}

	@Override
	public byte[] toBinary() {
		final byte[] geometryBinary = GeometryUtils.geometryToBinary(queryGeometry);
		int geometryFieldIdByteSize = 4;
		for (final ByteArrayId id : geometryFieldIds) {
			geometryFieldIdByteSize += (4 + id.getBytes().length);
		}
		final ByteBuffer geometryFieldIdBuffer = ByteBuffer.allocate(geometryFieldIdByteSize);
		geometryFieldIdBuffer.putInt(geometryFieldIds.size());
		for (final ByteArrayId id : geometryFieldIds) {
			geometryFieldIdBuffer.putInt(id.getBytes().length);
			geometryFieldIdBuffer.put(id.getBytes());
		}
		final byte[] theRest = super.toBinary();
		final ByteBuffer buf = ByteBuffer.allocate(8 + geometryBinary.length + geometryFieldIdByteSize + theRest.length);
		buf.putInt(geometryBinary.length);
		buf.putInt(geometryFieldIdByteSize);
		buf.put(geometryBinary);
		buf.put(geometryFieldIdBuffer.array());
		buf.put(theRest);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] geometryBinary = new byte[buf.getInt()];
		final byte[] theRest = new byte[bytes.length - geometryBinary.length - buf.getInt() - 8];
		buf.get(geometryBinary);
		final int fieldIdSize = buf.getInt();
		geometryFieldIds = new HashSet<ByteArrayId>(
				fieldIdSize);
		for (int i = 0; i < fieldIdSize; i++) {
			final byte[] fieldId = new byte[buf.getInt()];
			buf.get(fieldId);
			geometryFieldIds.add(new ByteArrayId(
					fieldId));
		}
		buf.get(theRest);
		queryGeometry = GeometryUtils.geometryFromBinary(geometryBinary);

		super.fromBinary(theRest);
	}
}
