package mil.nga.giat.geowave.test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import org.junit.internal.runners.statements.RunAfters;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveITSuiteRunner extends
		Suite
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveITSuiteRunner.class);

	@Override
	protected Statement withAfterClasses(
			Statement statement ) {
		try {
			Statement newStatement = super.withAfterClasses(statement);
			final Method tearDownMethod = GeoWaveITSuiteRunner.class.getDeclaredMethod("tearDown");
			tearDownMethod.setAccessible(true);
			return new RunAfters(
					newStatement,
					Collections.singletonList(new FrameworkMethod(
							tearDownMethod)),
					this);
		}
		catch (NoSuchMethodException | SecurityException e) {
			LOGGER.warn(
					"Unable to find tearDown method",
					e);
		}
		return super.withAfterClasses(statement);
	}
	private GeoWaveIT itRunner;
	protected void tearDown() throws Exception{
		if (itRunner != null){
			itRunner.tearDown();
		}		
	}

	@Override
	protected void runChild(
			Runner runner,
			RunNotifier notifier ) {
		if (runner instanceof GeoWaveIT){
			itRunner = (GeoWaveIT) runner;
		}
		super.runChild(
				runner,
				notifier);
	}

	public GeoWaveITSuiteRunner(
			final Class<?> klass,
			final List<Runner> runners )
			throws InitializationError {
		super(
				klass,
				runners);
	}

	public GeoWaveITSuiteRunner(
			final Class<?> klass,
			final RunnerBuilder builder )
			throws InitializationError {
		super(
				klass,
				builder);
	}

	public GeoWaveITSuiteRunner(
			final RunnerBuilder builder,
			final Class<?> klass,
			final Class<?>[] suiteClasses )
			throws InitializationError {
		super(
				builder,
				klass,
				suiteClasses);
	}

	public GeoWaveITSuiteRunner(
			final RunnerBuilder builder,
			final Class<?>[] classes )
			throws InitializationError {
		super(
				builder,
				classes);
	}

	protected GeoWaveITSuiteRunner(
			final Class<?> klass,
			final Class<?>[] suiteClasses )
			throws InitializationError {
		super(
				klass,
				suiteClasses);
	}

}
