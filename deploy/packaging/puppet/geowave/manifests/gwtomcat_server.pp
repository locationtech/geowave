class geowave::gwtomcat_server {
  $http_port     = $geowave::http_port
  $ajp_port      = $geowave::ajp_port
  $shutdown_port = $geowave::shutdown_port

  if !defined(Package["geowave-${geowave::geowave_version}-core"]) {
    package { "geowave-${geowave::geowave_version}-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat":
    ensure => latest,
    tag    => 'geowave-package',
    notify  => Service['gwtomcat'],
  }

  file_line {'change_http_port':
    ensure  => present,
    path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/conf/server.xml",
    line    => "<Connector port=\"${http_port}\" protocol=\"HTTP/1.1\"",
    match   => '.Connector\ port="(\d{1,5})".protocol="HTTP.*"$',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"],
    notify  => Service['gwtomcat'],
  }

  file_line {'change_ajp_port':
    ensure  => present,
    path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/conf/server.xml",
    line    => "<Connector port=\"${ajp_port}\" protocol=\"AJP/1.3\" redirectPort=\"8443\" />",
    match   => '.Connector\ port="(\d{1,5})".protocol="AJP.*$',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"],
    notify  => Service['gwtomcat'],
  }

  file_line {'change_shutdown_port':
    ensure  => present,
    path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/conf/server.xml",
    line    => "<Server port=\"${shutdown_port}\" shutdown=\"SHUTDOWN\">",
    match   => '.Server\ port="(\d{1,5})" shutdown="SHUTDOWN">$',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"],
    notify  => Service['gwtomcat'],
  }

}
