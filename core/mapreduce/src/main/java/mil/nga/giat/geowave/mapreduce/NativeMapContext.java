/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.mapreduce;

import java.io.IOException;
import java.net.URI;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

/**
 * This class wraps an existing map context that will write hadoop writable
 * objects as a map context that writes the native object for ease of
 * implementing mapreduce jobs.
 * 
 * @param <KEYIN>
 *            The map context's input type
 * @param <VALUEIN>
 *            The map context's output type
 */
public class NativeMapContext<KEYIN, VALUEIN> implements
		MapContext<KEYIN, VALUEIN, GeoWaveInputKey, Object>
{
	private final MapContext<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable> context;
	private final HadoopWritableSerializationTool serializationTool;

	public NativeMapContext(
			final MapContext<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable> context,
			final AdapterStore adapterStore ) {
		this.context = context;
		this.serializationTool = new HadoopWritableSerializationTool(
				adapterStore);
	}

	public NativeMapContext(
			final MapContext<KEYIN, VALUEIN, GeoWaveInputKey, ObjectWritable> context,
			final HadoopWritableSerializationTool serializationTool ) {
		this.context = context;
		this.serializationTool = serializationTool;
	}

	@Override
	public TaskAttemptID getTaskAttemptID() {
		return context.getTaskAttemptID();
	}

	@Override
	public void setStatus(
			final String msg ) {
		context.setStatus(msg);
	}

	@Override
	public String getStatus() {
		return context.getStatus();
	}

	@Override
	public InputSplit getInputSplit() {
		return context.getInputSplit();
	}

	@Override
	public Configuration getConfiguration() {
		return context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue()
			throws IOException,
			InterruptedException {
		return context.nextKeyValue();
	}

	@Override
	public float getProgress() {
		return context.getProgress();
	}

	@Override
	public int hashCode() {
		return context.hashCode();
	}

	@Override
	public Credentials getCredentials() {
		return context.getCredentials();
	}

	@Override
	public Counter getCounter(
			final Enum<?> counterName ) {
		return context.getCounter(counterName);
	}

	@Override
	public KEYIN getCurrentKey()
			throws IOException,
			InterruptedException {
		return context.getCurrentKey();
	}

	@Override
	public JobID getJobID() {
		return context.getJobID();
	}

	@Override
	public int getNumReduceTasks() {
		return context.getNumReduceTasks();
	}

	@Override
	public Counter getCounter(
			final String groupName,
			final String counterName ) {
		return context.getCounter(
				groupName,
				counterName);
	}

	@Override
	public VALUEIN getCurrentValue()
			throws IOException,
			InterruptedException {
		return context.getCurrentValue();
	}

	@Override
	public Path getWorkingDirectory()
			throws IOException {
		return context.getWorkingDirectory();
	}

	@Override
	public void write(
			final GeoWaveInputKey key,
			final Object value )
			throws IOException,
			InterruptedException {
		context.write(
				key,
				serializationTool.toWritable(
						key.getAdapterId(),
						value));
	}

	@Override
	public Class<?> getOutputKeyClass() {
		return context.getOutputKeyClass();
	}

	@Override
	public OutputCommitter getOutputCommitter() {
		return context.getOutputCommitter();
	}

	@Override
	public Class<?> getOutputValueClass() {
		return context.getOutputValueClass();
	}

	@Override
	public Class<?> getMapOutputKeyClass() {
		return context.getMapOutputKeyClass();
	}

	@Override
	public Class<?> getMapOutputValueClass() {
		return context.getMapOutputValueClass();
	}

	@Override
	public String getJobName() {
		return context.getJobName();
	}

	public boolean userClassesTakesPrecedence() {
		return context.getConfiguration().getBoolean(
				MAPREDUCE_JOB_USER_CLASSPATH_FIRST,
				false);
	}

	@Override
	public boolean equals(
			final Object obj ) {
		return context.equals(obj);
	}

	@Override
	public Class<? extends InputFormat<?, ?>> getInputFormatClass()
			throws ClassNotFoundException {
		return context.getInputFormatClass();
	}

	@Override
	public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
			throws ClassNotFoundException {
		return context.getMapperClass();
	}

	@Override
	public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
			throws ClassNotFoundException {
		return context.getCombinerClass();
	}

	@Override
	public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
			throws ClassNotFoundException {
		return context.getReducerClass();
	}

	@Override
	public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
			throws ClassNotFoundException {
		return context.getOutputFormatClass();
	}

	@Override
	public Class<? extends Partitioner<?, ?>> getPartitionerClass()
			throws ClassNotFoundException {
		return context.getPartitionerClass();
	}

	@Override
	public RawComparator<?> getSortComparator() {
		return context.getSortComparator();
	}

	@Override
	public String getJar() {
		return context.getJar();
	}

	@Override
	public RawComparator<?> getCombinerKeyGroupingComparator() {
		return context.getCombinerKeyGroupingComparator();
	}

	@Override
	public RawComparator<?> getGroupingComparator() {
		return context.getGroupingComparator();
	}

	@Override
	public boolean getJobSetupCleanupNeeded() {
		return context.getJobSetupCleanupNeeded();
	}

	@Override
	public boolean getTaskCleanupNeeded() {
		return context.getTaskCleanupNeeded();
	}

	@Override
	public boolean getProfileEnabled() {
		return context.getProfileEnabled();
	}

	@Override
	public String getProfileParams() {
		return context.getProfileParams();
	}

	@Override
	public IntegerRanges getProfileTaskRange(
			final boolean isMap ) {
		return context.getProfileTaskRange(isMap);
	}

	@Override
	public String getUser() {
		return context.getUser();
	}

	@Override
	public boolean getSymlink() {
		return context.getSymlink();
	}

	@Override
	public Path[] getArchiveClassPaths() {
		return context.getArchiveClassPaths();
	}

	@Override
	public URI[] getCacheArchives()
			throws IOException {
		return context.getCacheArchives();
	}

	@Override
	public URI[] getCacheFiles()
			throws IOException {
		return context.getCacheFiles();
	}

	@Override
	public Path[] getLocalCacheArchives()
			throws IOException {
		return context.getLocalCacheArchives();
	}

	@Override
	public Path[] getLocalCacheFiles()
			throws IOException {
		return context.getLocalCacheFiles();
	}

	@Override
	public Path[] getFileClassPaths() {
		return context.getFileClassPaths();
	}

	@Override
	public String[] getArchiveTimestamps() {
		return context.getArchiveTimestamps();
	}

	@Override
	public String[] getFileTimestamps() {
		return context.getFileTimestamps();
	}

	@Override
	public int getMaxMapAttempts() {
		return context.getMaxMapAttempts();
	}

	@Override
	public int getMaxReduceAttempts() {
		return context.getMaxReduceAttempts();
	}

	@Override
	public void progress() {
		context.progress();
	}

	@Override
	public String toString() {
		return context.toString();
	}
}
