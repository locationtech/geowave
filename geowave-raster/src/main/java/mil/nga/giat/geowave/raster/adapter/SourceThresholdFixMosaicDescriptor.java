package mil.nga.giat.geowave.raster.adapter;

import javax.media.jai.JAI;
import javax.media.jai.OperationRegistry;
import javax.media.jai.ParameterListDescriptor;
import javax.media.jai.ParameterListDescriptorImpl;
import javax.media.jai.PropertyGenerator;
import javax.media.jai.operator.MosaicDescriptor;

import com.sun.media.jai.opimage.MosaicRIF;

/**
 *
 * this is a workaround because GeoTools resampling will force the source
 * threshold to be 1.0 on Mosaic operations, which will mask all values under
 * 1.0. org.geotools.coverage.processing.operation.Resample2D line 631 in
 * gt-coverage-12.1
 *
 * This is mostly the same as MosaicDescriptor with the one key difference being
 * that the default source threshold is Double.MIN_VALUE instead of 1.0
 *
 */
public class SourceThresholdFixMosaicDescriptor extends
		MosaicDescriptor
{

	/**
	 * An array of <code>ParameterListDescriptor</code> for each mode.
	 */
	private final ParameterListDescriptor defaultParamListDescriptor;
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	/** The parameter class list for this operation. */
	private static final Class[] paramClasses = {
		javax.media.jai.operator.MosaicType.class,
		javax.media.jai.PlanarImage[].class,
		javax.media.jai.ROI[].class,
		double[][].class,
		double[].class
	};

	/** The parameter name list for this operation. */
	private static final String[] paramNames = {
		"mosaicType",
		"sourceAlpha",
		"sourceROI",
		"sourceThreshold",
		"backgroundValues"
	};

	/** The parameter default value list for this operation. */
	private static final Object[] paramDefaults = {
		MOSAIC_TYPE_OVERLAY,
		null,
		null,
		new double[][] {
			{
				Double.MIN_VALUE
			// if this is less than or equal to 0, it will only work on the
			// first band because of a bug with the source extender within JAI's
			// Mosaic operation
			}
		},
		new double[] {
			0.0
		}
	};
	static boolean registered = false;

	public synchronized static void register(
			final boolean force ) {
		if (!registered || force) {
			final OperationRegistry registry = JAI.getDefaultInstance().getOperationRegistry();
			registry.unregisterDescriptor(new MosaicDescriptor());
			registry.registerDescriptor(new SourceThresholdFixMosaicDescriptor());
			registry.registerFactory(
					"rendered",
					"mosaic",
					"com.sun.media.jai",
					new MosaicRIF());
			registered = true;
		}
	}

	public SourceThresholdFixMosaicDescriptor() {
		super();

		defaultParamListDescriptor = new ParameterListDescriptorImpl(
				this,
				paramNames,
				paramClasses,
				paramDefaults,
				null);
	}

	@Override
	public PropertyGenerator[] getPropertyGenerators(
			final String modeName ) {
		return new PropertyGenerator[] {
			new MosaicPropertyGenerator()
		};
	}

	@Override
	public ParameterListDescriptor getParameterListDescriptor(
			final String modeName ) {
		return defaultParamListDescriptor;
	}

}
