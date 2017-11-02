class geowave::tomcat_service {
  service { 'gw_tomcat8':
    ensure     => 'running',
    enable     => true,
    hasstatus  => true,
  }
}
