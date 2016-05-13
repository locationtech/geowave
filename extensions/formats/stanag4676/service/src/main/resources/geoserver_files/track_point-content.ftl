<#list features as feature>
<b>${feature.Classification.value}</b>
<br>
<i>Mission:</i> ${feature.Mission.value}
<br>
<i>Track #:</i> ${feature.TrackNumber.value}
<br>
<i>Track ID:</i> ${feature.TrackUUID.value}
<br>
<i>Event ID:</i> ${feature.TrackItemUUID.value}
<br>
<i>Timestamp:</i> ${feature.TimeStamp.rawValue?string("yyyy-MMM-dd HH:mm:ss.SSS'Z'")}
<br>
<i>Speed:</i> ${feature.Speed.value} m/s
<br>
<i>Course:</i> ${feature.Course.value} degrees
<br>
<i>Frame #:</i> ${feature.FrameNumber.value}
<br>
<i>Pixel Row:</i> ${feature.PixelRow.value}
<br>
<i>Pixel Column:</i> ${feature.PixelColumn.value}
<br>
<br>
<center><image src="http://c1-app-01:8080/geowave-service-4676/stanag4676/image/${fea
ture.Mission.value}/${feature.TrackUUID.value}/${feature.TimeStamp.rawValue?string("y
yyy-MM-dd'T'HH:mm:ss.SSS")}.jpg?size=200"/></center>
</#list>
