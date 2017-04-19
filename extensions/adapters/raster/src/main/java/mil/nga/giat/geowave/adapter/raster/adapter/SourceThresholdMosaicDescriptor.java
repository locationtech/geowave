package mil.nga.giat.geowave.adapter.raster.adapter;

import javax.media.jai.ParameterListDescriptor;
import javax.media.jai.ParameterListDescriptorImpl;
import javax.media.jai.PropertyGenerator;
import javax.media.jai.operator.MosaicDescriptor;

public class SourceThresholdMosaicDescriptor extends
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
			}
		},
		new double[] {
			0.0
		}
	};

	public SourceThresholdMosaicDescriptor() {
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
