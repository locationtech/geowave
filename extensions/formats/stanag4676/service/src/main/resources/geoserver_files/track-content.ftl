<#list features as feature>
<b>${feature.Classification.value}</b>
<br>
<i>Mission:</i> ${feature.Mission.value}
<br>
<i>Track #:</i> ${feature.TrackNumber.value}
<br>
<i>Track ID:</i> ${feature.TrackUUID.value}
<br>
<i>Start Time:</i> ${feature.StartTime.rawValue?string("yyyy-MMM-dd HH:mm:ss.SSS'Z'")
}
<br>
<i>End Time:</i> ${feature.EndTime.rawValue?string("yyyy-MMM-dd HH:mm:ss.SSS'Z'")}
<br>
<i>Duration:</i> ${feature.Duration.value} s
<br>
<i>Min Speed:</i> ${feature.MinSpeed.value} m/s
<br>
<i>Max Speed:</i> ${feature.MaxSpeed.value} m/s
<br>
<i>Average Speed:</i> ${feature.AvgSpeed.value} m/s
<br>
<i>Distance:</i> ${feature.Distance.value} km
<br>
<i># Points in Track:</i> ${feature.PointCount.value}
<br>
<i># Events in Track:</i> ${feature.EventCount.value}
<br>
<i># of Turns:</i> ${feature.TurnCount.value}
<br>
<i># of U-Turns:</i> ${feature.UTurnCount.value}
<br>
<i># of Stops:</i> ${feature.StopCount.value}
<br>
<br>
<br>
<center><video autoplay=true loop=true src="http://c1-app-01:8080/geowave-service-467
6/stanag4676/video/${feature.Mission.value}/${feature.TrackUUID.value}.webm?size=200&
speed=2"/></center>
</#list>
