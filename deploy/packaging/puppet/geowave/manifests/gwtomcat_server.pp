class geowave::gwtomcat_server {
  $http_port = $geowave::http_port

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

  file_line {'Change_default_port':
    ensure  => present,
    path    => "/usr/local/geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}/tomcat8/conf/server.xml",
    line    => "<Connector port=\"${http_port}\" protocol=\"HTTP/1.1\"",
    match   => '.Connector\ port="(\d{1,5}".protocol="HTTP.*"$)',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-gwtomcat"],
    notify  => Service['gwtomcat'],
  }
}
