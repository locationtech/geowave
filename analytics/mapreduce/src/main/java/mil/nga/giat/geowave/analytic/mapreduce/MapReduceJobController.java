package mil.nga.giat.geowave.analytic.mapreduce;

import java.io.IOException;
import java.util.Set;

import mil.nga.giat.geowave.analytic.IndependentJobRunner;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.param.MapReduceParameters;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run a series of jobs in a sequence. Use the {@link PostOperationTask} to
 * allow job definitions to perform an action after running. The purpose of this
 * added task is to support information from a prior job in the sequence(such as
 * temporary file names, job IDs, stats) to be provided to the next job or set
 * of jobs.
 * 
 */
public class MapReduceJobController implements
		MapReduceJobRunner,
		IndependentJobRunner
{

	final static Logger LOGGER = LoggerFactory.getLogger(MapReduceJobController.class);

	private MapReduceJobRunner[] runners;
	private PostOperationTask[] runSetUpTasks;

	public MapReduceJobController() {}

	protected void init(
			final MapReduceJobRunner[] runners,
			final PostOperationTask[] runSetUpTasks ) {
		this.runners = runners;
		this.runSetUpTasks = runSetUpTasks;
	}

	public MapReduceJobRunner[] getRunners() {
		return runners;
	}

	public static interface PostOperationTask
	{
		public void runTask(
				Configuration config,
				MapReduceJobRunner runner );
	}

	public static final PostOperationTask DoNothingTask = new PostOperationTask() {
		@Override
		public void runTask(
				final Configuration config,
				final MapReduceJobRunner runner ) {}
	};

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {
		for (int i = 0; i < runners.length; i++) {
			final MapReduceJobRunner runner = runners[i];
			LOGGER.info("Running " + runner.getClass().toString());
			final int status = runner.run(
					config,
					runTimeProperties);

			if (status != 0) {
				return status;
			}
			runSetUpTasks[i].runTask(
					config,
					runner);
		}
		return 0;
	}

	@Override
	public void fillOptions(
			final Set<Option> options ) {
		MapReduceParameters.fillOptions(options);

		for (int i = 0; i < runners.length; i++) {
			final MapReduceJobRunner runner = runners[i];
			if (runner instanceof IndependentJobRunner) {
				((IndependentJobRunner) runner).fillOptions(options);
			}
		}
	}

	@Override
	public int run(
			final PropertyManagement runTimeProperties )
			throws Exception {
		return this.run(
				getConfiguration(runTimeProperties),
				runTimeProperties);
	}

	public static Configuration getConfiguration(
			final PropertyManagement pm )
			throws IOException {
		return new HadoopOptions(
				pm).getConfiguration();
	}

}
