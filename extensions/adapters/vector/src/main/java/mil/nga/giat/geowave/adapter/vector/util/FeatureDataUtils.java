package mil.nga.giat.geowave.adapter.vector.util;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;

import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Geometry;

public class FeatureDataUtils
{
	private final static Logger LOGGER = Logger.getLogger(FeatureDataUtils.class);

	public static SimpleFeature defaultCRSTransform(
			final SimpleFeature entry,
			final SimpleFeatureType persistedType,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {

		// if the feature is in a different coordinate reference system than
		// EPSG:4326, transform the geometry
		final CoordinateReferenceSystem crs = entry.getFeatureType().getCoordinateReferenceSystem();
		SimpleFeature defaultCRSEntry = entry;

		if (!GeoWaveGTDataStore.DEFAULT_CRS.equals(crs)) {
			MathTransform featureTransform = null;
			if ((persistedType.getCoordinateReferenceSystem() != null) && persistedType.getCoordinateReferenceSystem().equals(
					crs) && (transform != null)) {
				// we can use the transform we have already calculated for this
				// feature
				featureTransform = transform;
			}
			else if (crs != null) {
				// this feature differs from the persisted type in CRS,
				// calculate the transform
				try {
					featureTransform = CRS.findMathTransform(
							crs,
							GeoWaveGTDataStore.DEFAULT_CRS,
							true);
				}
				catch (final FactoryException e) {
					LOGGER.warn(
							"Unable to find transform to EPSG:4326, the feature geometry will remain in its original CRS",
							e);
				}
			}
			if (featureTransform != null) {
				try {
					// what should we do besides log a message when an entry
					// can't be transformed to EPSG:4326 for some reason?
					// this will clone the feature and retype it to EPSG:4326
					defaultCRSEntry = SimpleFeatureBuilder.retype(
							entry,
							reprojectedType);
					// this will transform the geometry
					defaultCRSEntry.setDefaultGeometry(JTS.transform(
							(Geometry) entry.getDefaultGeometry(),
							featureTransform));
				}
				catch (MismatchedDimensionException | TransformException e) {
					LOGGER.warn(
							"Unable to perform transform to EPSG:4326, the feature geometry will remain in its original CRS",
							e);
				}
			}
		}
		return defaultCRSEntry;
	}

	public static String getAxis(
			final CoordinateReferenceSystem crs ) {
		final CoordinateSystem cs = crs == null ? null : crs.getCoordinateSystem();
		if (cs != null && cs.getDimension() > 0) return cs.getAxis(
				0).getDirection().name().toString();
		return "";
	}

	public static SimpleFeatureType decodeType(
			final String nameSpace,
			final String typeName,
			final String typeDescriptor,
			final String axis )
			throws SchemaException {

		SimpleFeatureType featureType = DataUtilities.createType(
				nameSpace,
				typeName,
				typeDescriptor);

		final String lCaseAxis = axis.toLowerCase();
		final CoordinateReferenceSystem crs = featureType.getCoordinateReferenceSystem();
		final String typeAxis = getAxis(crs);
		// Default for EPSG:4326 is lat/long, If the provided type was
		// long/lat, then re-establish the order
		if (crs != null && crs.getIdentifiers().toString().contains(
				"EPSG:4326") && !lCaseAxis.equalsIgnoreCase(
				typeAxis)) {
			SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
			builder.init(featureType);

			try {
				featureType = SimpleFeatureTypeBuilder.retype(
						featureType,
						CRS.decode(
								"urn:ogc:def:crs:EPSG:6.6:4326",
								lCaseAxis.equals("east")));
			}
			catch (FactoryException e) {
				throw new SchemaException(
						"Cannot decode EPSG:4326",
						e);
			}
		}
		return featureType;

	}
}
