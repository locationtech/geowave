class geowave::server {

  $http_port = $geowave::http_port

  package { "geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-jetty":
    ensure => latest,
    tag    => 'geowave-package',
    notify => Service['geowave']
  }

  if !defined(Package["geowave-${geowave::geowave_version}-core"]) {
    package { "geowave-${geowave::geowave_version}-core":
      ensure => latest,
      tag    => 'geowave-package',
    }
  }

  file {'/usr/local/geowave/geoserver/start.ini':
    ensure  => file,
    content => template('geowave/start.ini.erb'),
    owner   => 'geowave',
    group   => 'geowave',
    mode    => '644',
    require => Package["geowave-${geowave::geowave_version}-${geowave::hadoop_vendor_version}-jetty"],
    notify  => Service['geowave']
  }

}
