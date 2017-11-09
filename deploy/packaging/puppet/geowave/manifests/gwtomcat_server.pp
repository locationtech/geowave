class geowave::gwtomcat_server {
  $http_port          = $geowave::http_port
  $http_port_ajp      = $geowave::http_port_ajp
  $http_port_shutdown = $geowave::http_port_shutdown

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

  file_line {'Change_http_port':
    ensure  => present,
    path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/conf/server.xml",
    line    => "<Connector port=\"${http_port}\" protocol=\"HTTP/1.1\"",
    match   => '.Connector\ port="(\d{1,5})".protocol="HTTP.*"$',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"],
    notify  => Service['gwtomcat'],
  }

  file_line {'Change_ajp_port':
    ensure  => present,
    path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/conf/server.xml",
    line    => "<Connector port=\"${http_port_ajp}\" protocol=\"AJP/1.3\" redirectPort=\"8443\" />",
    match   => '.Connector\ port="(\d{1,5})".protocol="AJP.*$',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"],
    notify  => Service['gwtomcat'],
  }

  file_line {'Change_shutdown_port':
    ensure  => present,
    path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/conf/server.xml",
    line    => "<Server port=\"${http_port_shutdown}\" shutdown=\"SHUTDOWN\">",
    match   => '.Server\ port="(\d{1,5})" shutdown="SHUTDOWN">$',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"],
    notify  => Service['gwtomcat'],
  }

}
