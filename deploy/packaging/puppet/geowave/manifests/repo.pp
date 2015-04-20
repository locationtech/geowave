class geowave::repo(
  $repo_name       = 'geowave',
  $repo_desc       = 'GeoWave Repo',
  $repo_enabled    = 0,
  $repo_base_url   = 'http://s3.amazonaws.com/geowave-rpms/release/noarch/',
  $repo_refresh_md = 21600, # Repo metadata is good for 6 hours by default 
  $repo_priority   = 15,
  $repo_gpg_check  = 0,
) {

  yumrepo {$repo_name:
    baseurl         => $repo_base_url,
    descr           => $repo_desc,
    enabled         => $repo_enabled,
    gpgcheck        => $repo_gpg_check,
    priority        => $repo_priority,
    metadata_expire => $repo_refresh_md,
  }

  Yumrepo[$repo_name] -> Package<|tag == 'geowave-package' |>

}
