package mil.nga.giat.geowave.format.stanag4676;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TrackEventWritable implements
		Writable
{

	public IntWritable EventType; // 0 = point event 1 = motion event
	public BytesWritable Geometry;
	public BytesWritable Image;
	public Text Mission;
	public Text TrackNumber;
	public Text TrackUUID;
	public Text TrackStatus;
	public Text TrackClassification;
	public Text TrackItemUUID;
	public LongWritable TimeStamp;
	public LongWritable EndTimeStamp;
	public DoubleWritable Speed;
	public DoubleWritable Course;
	public Text TrackItemClassification;
	public DoubleWritable Latitude;
	public DoubleWritable Longitude;
	public DoubleWritable Elevation;
	public IntWritable PixelRow;
	public IntWritable PixelColumn;
	public Text MotionEvent;
	public IntWritable FrameNumber;

	public static TrackEventWritable clone(
			final TrackEventWritable tw ) {
		final TrackEventWritable tw2 = new TrackEventWritable();
		tw2.EventType = new IntWritable(
				tw.EventType.get());
		tw2.Geometry = new BytesWritable(
				tw.Geometry.copyBytes());
		tw2.Image = new BytesWritable(
				tw.Image.copyBytes());
		tw2.Mission = new Text(
				tw.Mission.toString());
		tw2.TrackNumber = new Text(
				tw.TrackNumber.toString());
		tw2.TrackUUID = new Text(
				tw.TrackUUID.toString());
		tw2.TrackStatus = new Text(
				tw.TrackStatus.toString());
		tw2.TrackClassification = new Text(
				tw.TrackClassification.toString());
		tw2.TrackItemUUID = new Text(
				tw.TrackItemUUID.toString());
		tw2.TimeStamp = new LongWritable(
				tw.TimeStamp.get());
		tw2.EndTimeStamp = new LongWritable(
				tw.EndTimeStamp.get());
		tw2.Speed = new DoubleWritable(
				tw.Speed.get());
		tw2.Course = new DoubleWritable(
				tw.Course.get());
		tw2.TrackItemClassification = new Text(
				tw.TrackItemClassification.toString());
		tw2.Latitude = new DoubleWritable(
				tw.Latitude.get());
		tw2.Longitude = new DoubleWritable(
				tw.Longitude.get());
		tw2.Elevation = new DoubleWritable(
				tw.Elevation.get());
		tw2.PixelRow = new IntWritable(
				tw.PixelRow.get());
		tw2.PixelColumn = new IntWritable(
				tw.PixelColumn.get());
		tw2.MotionEvent = new Text(
				tw.MotionEvent.toString());
		tw2.FrameNumber = new IntWritable(
				tw.FrameNumber.get());

		return tw2;

	}

	public TrackEventWritable() {
		EventType = new IntWritable();
		Geometry = new BytesWritable();
		Image = new BytesWritable();
		Mission = new Text();
		TrackNumber = new Text();
		TrackUUID = new Text();
		TrackStatus = new Text();
		TrackClassification = new Text();
		TrackItemUUID = new Text();
		TimeStamp = new LongWritable();
		EndTimeStamp = new LongWritable();
		Speed = new DoubleWritable();
		Course = new DoubleWritable();
		TrackItemClassification = new Text();
		Latitude = new DoubleWritable();
		Longitude = new DoubleWritable();
		Elevation = new DoubleWritable();
		PixelRow = new IntWritable();
		PixelColumn = new IntWritable();
		MotionEvent = new Text();
		FrameNumber = new IntWritable();
	}

	public TrackEventWritable(
			final int eventType,
			final byte[] geometry,
			final byte[] image,
			final String mission,
			final String trackNumber,
			final String trackUUID,
			final String trackStatus,
			final String trackClassification,
			final String trackItemUUID,
			final long timeStamp,
			final long endTimeStamp,
			final double speed,
			final double course,
			final String trackItemClassification,
			final double latitude,
			final double longitude,
			final double elevation,
			final int pixelRow,
			final int pixelColumn,
			final String motionEvent,
			final int frameNumber ) {
		EventType = new IntWritable(
				eventType);
		Geometry = new BytesWritable(
				geometry);
		Image = new BytesWritable(
				image);
		Mission = new Text(
				mission);
		TrackNumber = new Text(
				trackNumber);
		TrackUUID = new Text(
				trackUUID);
		TrackStatus = new Text(
				trackStatus);
		TrackClassification = new Text(
				trackClassification);
		TrackItemUUID = new Text(
				trackItemUUID);
		TimeStamp = new LongWritable(
				timeStamp);
		EndTimeStamp = new LongWritable(
				endTimeStamp);
		Speed = new DoubleWritable(
				speed);
		Course = new DoubleWritable(
				course);
		TrackItemClassification = new Text(
				trackItemClassification);
		Latitude = new DoubleWritable(
				latitude);
		Longitude = new DoubleWritable(
				longitude);
		Elevation = new DoubleWritable(
				elevation);
		PixelRow = new IntWritable(
				pixelRow);
		PixelColumn = new IntWritable(
				pixelColumn);
		MotionEvent = new Text(
				motionEvent);
		FrameNumber = new IntWritable(
				frameNumber);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		EventType.readFields(in);
		Geometry.readFields(in);
		Image.readFields(in);
		Mission.readFields(in);
		TrackNumber.readFields(in);
		TrackUUID.readFields(in);
		TrackStatus.readFields(in);
		TrackClassification.readFields(in);
		TrackItemUUID.readFields(in);
		TimeStamp.readFields(in);
		EndTimeStamp.readFields(in);
		Speed.readFields(in);
		Course.readFields(in);
		TrackItemClassification.readFields(in);
		Latitude.readFields(in);
		Longitude.readFields(in);
		Elevation.readFields(in);
		PixelRow.readFields(in);
		PixelColumn.readFields(in);
		MotionEvent.readFields(in);
		FrameNumber.readFields(in);

	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		EventType.write(out);
		Geometry.write(out);
		Image.write(out);
		Mission.write(out);
		TrackNumber.write(out);
		TrackUUID.write(out);
		TrackStatus.write(out);
		TrackClassification.write(out);
		TrackItemUUID.write(out);
		TimeStamp.write(out);
		EndTimeStamp.write(out);
		Speed.write(out);
		Course.write(out);
		TrackItemClassification.write(out);
		Latitude.write(out);
		Longitude.write(out);
		Elevation.write(out);
		PixelRow.write(out);
		PixelColumn.write(out);
		MotionEvent.write(out);
		FrameNumber.write(out);
	}

}
