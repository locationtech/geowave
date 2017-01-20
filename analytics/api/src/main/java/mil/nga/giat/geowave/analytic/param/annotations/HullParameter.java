package mil.nga.giat.geowave.analytic.param.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import mil.nga.giat.geowave.analytic.param.HullParameters.Hull;

/**
 * This is a stop-gap measure to allow using JCommander with Analytics, until we
 * figure out how to deal with PropertyEnum.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({
	ElementType.FIELD,
	ElementType.METHOD
})
public @interface HullParameter {
	Hull[] value();
}