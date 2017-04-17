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

public class Stanag4676EventWritable implements
		Writable
{
	public static final double NO_DETAIL = Double.MIN_VALUE;

	// 0 = point event
	// 1 = motion event
	// 2 = track object classification event
	// 3 = mission frame event
	// 4 = mission summary event
	public IntWritable EventType;
	public BytesWritable Geometry;
	public BytesWritable DetailGeometry;
	public BytesWritable Image;
	public Text MissionUUID;
	public Text MissionName;
	public IntWritable MissionNumFrames;
	public Text TrackNumber;
	public Text TrackUUID;
	public Text TrackStatus;
	public Text TrackClassification;
	public Text TrackItemUUID;
	public Text TrackPointSource;
	public LongWritable TimeStamp;
	public LongWritable EndTimeStamp;
	public DoubleWritable Speed;
	public DoubleWritable Course;
	public Text TrackItemClassification;
	public DoubleWritable Latitude;
	public DoubleWritable Longitude;
	public DoubleWritable Elevation;
	public DoubleWritable DetailLatitude;
	public DoubleWritable DetailLongitude;
	public DoubleWritable DetailElevation;
	public IntWritable PixelRow;
	public IntWritable PixelColumn;
	public Text MotionEvent;
	public IntWritable FrameNumber;
	public Text ObjectClass;
	public IntWritable ObjectClassConf;
	public IntWritable ObjectClassRel;

	public static Stanag4676EventWritable clone(
			final Stanag4676EventWritable sw ) {
		final Stanag4676EventWritable sw2 = new Stanag4676EventWritable();
		sw2.EventType = new IntWritable(
				sw.EventType.get());
		sw2.Geometry = new BytesWritable(
				sw.Geometry.copyBytes());
		sw2.DetailGeometry = new BytesWritable(
				sw.DetailGeometry.copyBytes());
		sw2.Image = new BytesWritable(
				sw.Image.copyBytes());
		sw2.MissionUUID = new Text(
				sw.MissionUUID.toString());
		sw2.MissionName = new Text(
				sw.MissionName.toString());
		sw2.MissionNumFrames = new IntWritable(
				sw.MissionNumFrames.get());
		sw2.TrackNumber = new Text(
				sw.TrackNumber.toString());
		sw2.TrackUUID = new Text(
				sw.TrackUUID.toString());
		sw2.TrackStatus = new Text(
				sw.TrackStatus.toString());
		sw2.TrackClassification = new Text(
				sw.TrackClassification.toString());
		sw2.TrackItemUUID = new Text(
				sw.TrackItemUUID.toString());
		sw2.TrackPointSource = new Text(
				sw.TrackPointSource.toString());
		sw2.TimeStamp = new LongWritable(
				sw.TimeStamp.get());
		sw2.EndTimeStamp = new LongWritable(
				sw.EndTimeStamp.get());
		sw2.Speed = new DoubleWritable(
				sw.Speed.get());
		sw2.Course = new DoubleWritable(
				sw.Course.get());
		sw2.TrackItemClassification = new Text(
				sw.TrackItemClassification.toString());
		sw2.Latitude = new DoubleWritable(
				sw.Latitude.get());
		sw2.Longitude = new DoubleWritable(
				sw.Longitude.get());
		sw2.Elevation = new DoubleWritable(
				sw.Elevation.get());
		sw2.DetailLatitude = new DoubleWritable(
				sw.DetailLatitude.get());
		sw2.DetailLongitude = new DoubleWritable(
				sw.DetailLongitude.get());
		sw2.DetailElevation = new DoubleWritable(
				sw.DetailElevation.get());
		sw2.PixelRow = new IntWritable(
				sw.PixelRow.get());
		sw2.PixelColumn = new IntWritable(
				sw.PixelColumn.get());
		sw2.MotionEvent = new Text(
				sw.MotionEvent.toString());
		sw2.FrameNumber = new IntWritable(
				sw.FrameNumber.get());
		sw2.ObjectClass = new Text(
				sw.ObjectClass.toString());
		sw2.ObjectClassConf = new IntWritable(
				sw.ObjectClassConf.get());
		sw2.ObjectClassRel = new IntWritable(
				sw.ObjectClassRel.get());

		return sw2;

	}

	public Stanag4676EventWritable() {
		EventType = new IntWritable();
		Geometry = new BytesWritable();
		DetailGeometry = new BytesWritable();
		Image = new BytesWritable();
		MissionUUID = new Text();
		MissionName = new Text();
		MissionNumFrames = new IntWritable();
		TrackNumber = new Text();
		TrackUUID = new Text();
		TrackStatus = new Text();
		TrackClassification = new Text();
		TrackItemUUID = new Text();
		TrackPointSource = new Text();
		TimeStamp = new LongWritable();
		EndTimeStamp = new LongWritable();
		Speed = new DoubleWritable();
		Course = new DoubleWritable();
		TrackItemClassification = new Text();
		Latitude = new DoubleWritable();
		Longitude = new DoubleWritable();
		Elevation = new DoubleWritable();
		DetailLatitude = new DoubleWritable();
		DetailLongitude = new DoubleWritable();
		DetailElevation = new DoubleWritable();
		PixelRow = new IntWritable();
		PixelColumn = new IntWritable();
		MotionEvent = new Text();
		FrameNumber = new IntWritable();
		ObjectClass = new Text();
		ObjectClassConf = new IntWritable();
		ObjectClassRel = new IntWritable();
	}

	public void setTrackPointData(
			final byte[] geometry,
			final byte[] detailGeometry,
			final byte[] image,
			final String missionUUID,
			final String trackNumber,
			final String trackUUID,
			final String trackStatus,
			final String trackClassification,
			final String trackItemUUID,
			final String trackPointSource,
			final long timeStamp,
			final long endTimeStamp,
			final double speed,
			final double course,
			final String trackItemClassification,
			final double latitude,
			final double longitude,
			final double elevation,
			final double detailLatitude,
			final double detailLongitude,
			final double detailElevation,
			final int pixelRow,
			final int pixelColumn,
			final int frameNumber ) {
		EventType = new IntWritable(
				0);
		Geometry = new BytesWritable(
				geometry);
		if (detailGeometry != null) {
			DetailGeometry = new BytesWritable(
					detailGeometry);
		}
		if (image != null) {
			Image = new BytesWritable(
					image);
		}
		MissionUUID = new Text(
				missionUUID);
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
		TrackPointSource = new Text(
				trackPointSource);
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
		DetailLatitude = new DoubleWritable(
				detailLatitude);
		DetailLongitude = new DoubleWritable(
				detailLongitude);
		DetailElevation = new DoubleWritable(
				detailElevation);
		PixelRow = new IntWritable(
				pixelRow);
		PixelColumn = new IntWritable(
				pixelColumn);
		FrameNumber = new IntWritable(
				frameNumber);
	}

	public void setMotionPointData(
			final byte[] geometry,
			final byte[] image,
			final String missionUUID,
			final String trackNumber,
			final String trackUUID,
			final String trackStatus,
			final String trackClassification,
			final String trackItemUUID,
			final String trackPointSource,
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
			final int frameNumber,
			final String motionEvent ) {
		EventType = new IntWritable(
				1);
		Geometry = new BytesWritable(
				geometry);
		if (image != null) {
			Image = new BytesWritable(
					image);
		}

		MissionUUID = new Text(
				missionUUID);
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
		TrackPointSource = new Text(
				trackPointSource);
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
		FrameNumber = new IntWritable(
				frameNumber);
		MotionEvent = new Text(
				motionEvent);
	}

	public void setTrackObjectClassData(
			final long timeStamp,
			final String objectClass,
			final int objectConf,
			final int objectRel ) {
		EventType = new IntWritable(
				2);
		TimeStamp = new LongWritable(
				timeStamp);
		ObjectClass = new Text(
				objectClass);
		ObjectClassConf = new IntWritable(
				objectConf);
		ObjectClassRel = new IntWritable(
				objectRel);
	}

	public void setMissionFrameData(
			final byte[] geometry,
			final String missionUUID,
			final int number,
			final long timeStamp ) {
		EventType = new IntWritable(
				3);
		Geometry = new BytesWritable(
				geometry);
		MissionUUID = new Text(
				missionUUID);
		FrameNumber = new IntWritable(
				number);
		TimeStamp = new LongWritable(
				timeStamp);
	}

	public void setMissionSummaryData(
			final byte[] geometry,
			final String missionUUID,
			final String missionName,
			final int missionNumFrames,
			final long timeStamp,
			final long endTimeStamp,
			final String classification,
			final String objectClass ) {
		EventType = new IntWritable(
				4);
		Geometry = new BytesWritable(
				geometry);
		MissionUUID = new Text(
				missionUUID);
		MissionName = new Text(
				missionName);
		MissionNumFrames = new IntWritable(
				missionNumFrames);
		TimeStamp = new LongWritable(
				timeStamp);
		EndTimeStamp = new LongWritable(
				endTimeStamp);
		TrackClassification = new Text(
				classification);
		ObjectClass = new Text(
				objectClass);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		EventType.readFields(in);
		Geometry.readFields(in);
		DetailGeometry.readFields(in);
		Image.readFields(in);
		MissionUUID.readFields(in);
		MissionName.readFields(in);
		MissionNumFrames.readFields(in);
		TrackNumber.readFields(in);
		TrackUUID.readFields(in);
		TrackStatus.readFields(in);
		TrackClassification.readFields(in);
		TrackItemUUID.readFields(in);
		TrackPointSource.readFields(in);
		TimeStamp.readFields(in);
		EndTimeStamp.readFields(in);
		Speed.readFields(in);
		Course.readFields(in);
		TrackItemClassification.readFields(in);
		Latitude.readFields(in);
		Longitude.readFields(in);
		Elevation.readFields(in);
		DetailLatitude.readFields(in);
		DetailLongitude.readFields(in);
		DetailElevation.readFields(in);
		PixelRow.readFields(in);
		PixelColumn.readFields(in);
		FrameNumber.readFields(in);
		MotionEvent.readFields(in);
		ObjectClass.readFields(in);
		ObjectClassConf.readFields(in);
		ObjectClassRel.readFields(in);
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		EventType.write(out);
		Geometry.write(out);
		DetailGeometry.write(out);
		Image.write(out);
		MissionUUID.write(out);
		MissionName.write(out);
		MissionNumFrames.write(out);
		TrackNumber.write(out);
		TrackUUID.write(out);
		TrackStatus.write(out);
		TrackClassification.write(out);
		TrackItemUUID.write(out);
		TrackPointSource.write(out);
		TimeStamp.write(out);
		EndTimeStamp.write(out);
		Speed.write(out);
		Course.write(out);
		TrackItemClassification.write(out);
		Latitude.write(out);
		Longitude.write(out);
		Elevation.write(out);
		DetailLatitude.write(out);
		DetailLongitude.write(out);
		DetailElevation.write(out);
		PixelRow.write(out);
		PixelColumn.write(out);
		FrameNumber.write(out);
		MotionEvent.write(out);
		ObjectClass.write(out);
		ObjectClassConf.write(out);
		ObjectClassRel.write(out);
	}
}