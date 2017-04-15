package mil.nga.giat.geowave.core.geotime.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.BasicQueryFilter;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

/**
 * This filter can perform fine-grained acceptance testing (intersection test
 * with a query geometry) with JTS geometry
 * 
 */
public class SpatialQueryFilter extends
		BasicQueryFilter
{

	private static final Interner<GeometryImage> geometryImageInterner = Interners.newWeakInterner();
	public static final PreparedGeometryFactory FACTORY = new PreparedGeometryFactory();
	private GeometryImage preparedGeometryImage;

	protected interface SpatialQueryCompareOp
	{
		public boolean compare(
				final Geometry dataGeometry,
				final PreparedGeometry constraintGeometry );

		public BasicQueryCompareOperation getBaseCompareOp();
	}

	public enum CompareOperation
			implements
			SpatialQueryCompareOp {
		CONTAINS {
			@Override
			public boolean compare(
					final Geometry dataGeometry,
					final PreparedGeometry constraintGeometry ) {
				return constraintGeometry.contains(dataGeometry);
			}

			@Override
			public BasicQueryCompareOperation getBaseCompareOp() {
				return BasicQueryCompareOperation.CONTAINS;
			}
		},
		OVERLAPS {

			@Override
			public boolean compare(
					final Geometry dataGeometry,
					final PreparedGeometry constraintGeometry ) {
				return constraintGeometry.overlaps(dataGeometry);
			}

			@Override
			public BasicQueryCompareOperation getBaseCompareOp() {
				return BasicQueryCompareOperation.OVERLAPS;
			}
		},
		INTERSECTS {
			@Override
			public boolean compare(
					final Geometry dataGeometry,
					final PreparedGeometry constraintGeometry ) {
				return constraintGeometry.intersects(dataGeometry);
			}

			@Override
			public BasicQueryCompareOperation getBaseCompareOp() {
				return BasicQueryCompareOperation.INTERSECTS;
			}
		},
		TOUCHES {
			@Override
			public boolean compare(
					final Geometry dataGeometry,
					final PreparedGeometry constraintGeometry ) {
				return constraintGeometry.touches(dataGeometry);
			}

			@Override
			public BasicQueryCompareOperation getBaseCompareOp() {
				return BasicQueryCompareOperation.TOUCHES;
			}
		},
		WITHIN {
			@Override
			public boolean compare(
					final Geometry dataGeometry,
					final PreparedGeometry constraintGeometry ) {
				return constraintGeometry.within(dataGeometry);
			}

			@Override
			public BasicQueryCompareOperation getBaseCompareOp() {
				return BasicQueryCompareOperation.WITHIN;
			}
		},
		DISJOINT {
			@Override
			public boolean compare(
					final Geometry dataGeometry,
					final PreparedGeometry constraintGeometry ) {
				return constraintGeometry.disjoint(dataGeometry);
			}

			@Override
			public BasicQueryCompareOperation getBaseCompareOp() {
				return BasicQueryCompareOperation.DISJOINT;
			}
		},
		CROSSES {
			@Override
			public boolean compare(
					final Geometry dataGeometry,
					final PreparedGeometry constraintGeometry ) {
				return constraintGeometry.crosses(dataGeometry);
			}

			@Override
			public BasicQueryCompareOperation getBaseCompareOp() {
				return BasicQueryCompareOperation.CROSSES;
			}
		},
		EQUALS {
			@Override
			public boolean compare(
					final Geometry dataGeometry,
					final PreparedGeometry constraintGeometry ) {
				// This method is same as Geometry.equalsTopo which is
				// computationally expensive.
				// See equalsExact for quick structural equality
				return constraintGeometry.getGeometry().equals(
						dataGeometry);
			}

			@Override
			public BasicQueryCompareOperation getBaseCompareOp() {
				return BasicQueryCompareOperation.EQUALS;
			}
		}
	};

	private CompareOperation compareOperation = CompareOperation.INTERSECTS;

	private Set<ByteArrayId> geometryFieldIds;

	protected SpatialQueryFilter() {
		super();
	}

	public SpatialQueryFilter(
			final MultiDimensionalNumericData query,
			final NumericDimensionField<?>[] orderedConstrainedDimensionDefinitions,
			final NumericDimensionField<?>[] unconstrainedDimensionDefinitions,
			final Geometry queryGeometry,
			final CompareOperation compareOp,
			final BasicQueryCompareOperation nonSpatialCompareOp ) {
		this(
				stripGeometry(
						query,
						orderedConstrainedDimensionDefinitions,
						unconstrainedDimensionDefinitions),
				queryGeometry,
				compareOp,
				nonSpatialCompareOp);
	}

	private SpatialQueryFilter(
			final StrippedGeometry strippedGeometry,
			final Geometry queryGeometry,
			final CompareOperation compareOp,
			final BasicQueryCompareOperation nonSpatialCompareOp ) {
		super(
				strippedGeometry.strippedQuery,
				strippedGeometry.strippedDimensionDefinitions,
				nonSpatialCompareOp);
		preparedGeometryImage = new GeometryImage(
				FACTORY.create(queryGeometry));
		geometryFieldIds = strippedGeometry.geometryFieldIds;
		this.compareOperation = compareOp;
	}

	private static class StrippedGeometry
	{
		private final MultiDimensionalNumericData strippedQuery;
		private final NumericDimensionField<?>[] strippedDimensionDefinitions;
		private final Set<ByteArrayId> geometryFieldIds;

		public StrippedGeometry(
				final MultiDimensionalNumericData strippedQuery,
				final NumericDimensionField<?>[] strippedDimensionDefinitions,
				final Set<ByteArrayId> geometryFieldIds ) {
			this.strippedQuery = strippedQuery;
			this.strippedDimensionDefinitions = strippedDimensionDefinitions;
			this.geometryFieldIds = geometryFieldIds;
		}
	}

	private static StrippedGeometry stripGeometry(
			final MultiDimensionalNumericData query,
			final NumericDimensionField<?>[] orderedConstrainedDimensionDefinitions,
			final NumericDimensionField<?>[] unconstrainedDimensionDefinitions ) {
		final Set<ByteArrayId> geometryFieldIds = new HashSet<ByteArrayId>();
		final List<NumericData> numericDataPerDimension = new ArrayList<NumericData>();
		final List<NumericDimensionField<?>> fields = new ArrayList<NumericDimensionField<?>>();
		final NumericData[] data = query.getDataPerDimension();
		for (int d = 0; d < orderedConstrainedDimensionDefinitions.length; d++) {
			// if the type on the generic is assignable to geometry then save
			// the field ID for later filtering
			if (isSpatial(orderedConstrainedDimensionDefinitions[d])) {
				geometryFieldIds.add(orderedConstrainedDimensionDefinitions[d].getFieldId());
			}
			else {
				numericDataPerDimension.add(data[d]);
				fields.add(orderedConstrainedDimensionDefinitions[d]);
			}
		}
		// we need to also add all geometry field IDs even if it is
		// unconstrained to be able to apply a geometry intersection (understand
		// that the bbox for a geometry can imply a full range based on its
		// envelope but the polygon may still need to be intersected with
		// results)
		for (int d = 0; d < unconstrainedDimensionDefinitions.length; d++) {
			if (isSpatial(unconstrainedDimensionDefinitions[d])) {
				geometryFieldIds.add(unconstrainedDimensionDefinitions[d].getFieldId());
			}
		}
		return new StrippedGeometry(
				new BasicNumericDataset(
						numericDataPerDimension.toArray(new NumericData[numericDataPerDimension.size()])),
				fields.toArray(new NumericDimensionField<?>[fields.size()]),
				geometryFieldIds);
	}

	public static boolean isSpatial(
			final NumericDimensionField<?> d ) {
		final Class<?> commonIndexType = GenericTypeResolver.resolveTypeArgument(
				d.getClass(),
				NumericDimensionField.class);
		return GeometryWrapper.class.isAssignableFrom(commonIndexType);
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		if (preparedGeometryImage == null) {
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
		return super.accept(
				indexModel,
				persistenceEncoding);
	}

	private boolean geometryPasses(
			final Geometry dataGeometry ) {
		if (dataGeometry == null) {
			return false;
		}
		if (preparedGeometryImage != null) {
			return compareOperation.compare(
					dataGeometry,
					preparedGeometryImage.preparedGeometry);
		}
		return false;
	}

	protected boolean isSpatialOnly() {
		return (dimensionFields == null) || (dimensionFields.length == 0);
	}

	@Override
	public byte[] toBinary() {
		final byte[] geometryBinary = preparedGeometryImage.geometryBinary;
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
		final ByteBuffer buf = ByteBuffer.allocate(12 + geometryBinary.length + geometryFieldIdByteSize
				+ theRest.length);
		buf.putInt(compareOperation.ordinal());
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
		compareOperation = CompareOperation.values()[buf.getInt()];
		final byte[] geometryBinary = new byte[buf.getInt()];
		final byte[] theRest = new byte[bytes.length - geometryBinary.length - buf.getInt() - 12];
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
		preparedGeometryImage = geometryImageInterner.intern(new GeometryImage(
				geometryBinary));
		// build the the PreparedGeometry and underling Geometry if not
		// reconstituted yet; most likely occurs if this thread constructed the
		// image.
		preparedGeometryImage.init();

		super.fromBinary(theRest);
	}

	/**
	 * This class is used for interning a PreparedGeometry. Prepared geometries
	 * cannot be interned since they do not extend Object.hashCode().
	 * 
	 * Interning a geometry assumes a geometry is already constructed on the
	 * heap at the time interning begins. The byte image of geometry provides a
	 * more efficient component to hash and associate with a single image of the
	 * geometry.
	 * 
	 * The approach of interning the Geometry prior to construction of a
	 * PreparedGeometry lead to excessive memory use. Thus, this class is
	 * constructed to hold the prepared geometry and prevent reconstruction of
	 * the underlying geometry from a byte array if the Geometry has been
	 * interned.
	 * 
	 * Using this approach increased performance of a large query unit test by
	 * 40% and reduced heap memory consumption by roughly 50%.
	 * 
	 */
	public static class GeometryImage
	{

		byte[] geometryBinary;
		PreparedGeometry preparedGeometry = null;

		public GeometryImage(
				final PreparedGeometry preparedGeometry ) {
			super();
			this.preparedGeometry = preparedGeometry;
			geometryBinary = GeometryUtils.geometryToBinary(preparedGeometry.getGeometry());
		}

		public GeometryImage(
				final byte[] geometryBinary ) {
			super();
			this.geometryBinary = geometryBinary;
		}

		public synchronized void init() {
			if (preparedGeometry == null) {
				preparedGeometry = FACTORY.create(GeometryUtils.geometryFromBinary(geometryBinary));
			}
		}

		public PreparedGeometry getGeometry() {
			return preparedGeometry;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + Arrays.hashCode(geometryBinary);
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final GeometryImage other = (GeometryImage) obj;
			if (!Arrays.equals(
					geometryBinary,
					other.geometryBinary)) {
				return false;
			}
			return true;
		}

	}
}
