package mil.nga.giat.geowave.core.geotime.store.filter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.dimension.DimensionField;
import mil.nga.giat.geowave.core.store.filter.BasicQueryFilter;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;

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
		preparedGeometryImage = new GeometryImage(
				FACTORY.create(queryGeometry));
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
						numericDataPerDimension.toArray(new NumericData[numericDataPerDimension.size()])),
				fields.toArray(new DimensionField<?>[fields.size()]),
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
			final IndexedPersistenceEncoding persistenceEncoding ) {
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
		return super.accept(persistenceEncoding);
	}

	private boolean geometryPasses(
			final Geometry dataGeometry ) {
		if (dataGeometry == null) {
			return false;
		}
		if (preparedGeometryImage != null) {
			return preparedGeometryImage.preparedGeometry.intersects(dataGeometry);
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
				PreparedGeometry preparedGeometry ) {
			super();
			this.preparedGeometry = preparedGeometry;
			geometryBinary = GeometryUtils.geometryToBinary(preparedGeometry.getGeometry());
		}

		public GeometryImage(
				byte[] geometryBinary ) {
			super();
			this.geometryBinary = geometryBinary;
		}

		public synchronized void init() {
			if (preparedGeometry == null) preparedGeometry = FACTORY.create(GeometryUtils.geometryFromBinary(geometryBinary));
		}

		public PreparedGeometry getGeometry() {
			return preparedGeometry;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(geometryBinary);
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			GeometryImage other = (GeometryImage) obj;
			if (!Arrays.equals(
					geometryBinary,
					other.geometryBinary)) return false;
			return true;
		}

	}
}
