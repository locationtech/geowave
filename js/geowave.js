// Navbar Fade In
// ==============================
var scrollFadePixels = 20;

var fadeNavbar = function (window) {
  var opacity = window.scrollTop () / scrollFadePixels;
  $ ('.navbar')
    .css ('background-color', 'rgba(23,33,46,' + opacity + ')')
    .addClass ('shadow-heavy');

  if ($ (window).scrollTop () <= scrollFadePixels) {
    $ ('.navbar').removeClass ('shadow-heavy');
  }
};

fadeNavbar ($ (window));

$ (window).scroll (function () {
  fadeNavbar ($ (this));
});

var documentationMenuItems = {
  'GeoWave Overview': 'overview.html',
  'Installation Guide': 'installation-guide.html',
  'Quickstart Guide': 'quickstart.html',
  'EMR Quickstart Guide': 'quickstart-emr.html',
  'User Guide': 'userguide.html',
  'Developer Guide': 'devguide.html',
  'Command-Line Interface': 'commands.html',
  'sep1': null,
  'Javadocs': 'apidocs/index.html',
  'Python Bindings': 'pydocs/index.html',
};

var supportMenuItems = {
  'GitHub Issues': 'https://github.com/locationtech/geowave/issues',
  'Gitter': 'https://gitter.im/locationtech/geowave',
  'Mailing List': 'mailto:geowave-dev@eclipse.org',
  'sep1': null,
  'Downloads': 'downloads.html'
};

var githubMenuItems = {
  'GeoWave Repository': 'https://github.com/locationtech/geowave',
  'Download Source (Zip)': 'https://github.com/locationtech/geowave/zipball/master',
  'Download Source (Tar)': 'https://github.com/locationtech/geowave/tarball/master',
};

// Initialize BS4 tooltips
$ (function () {
  $ ('[data-toggle="tooltip"]').tooltip ();
});

// Image Slider and Lightbox Combination
// Swiper JS: https://swiperjs.com/
// PhotoSwipe: https://photoswipe.com/
// ==============================

/* 1 of 2 : SWIPER */
var initPhotoSwipe = function () {
  var mySwiper = new Swiper ('.swiper-container', {
    // If loop true set photoswipe - counterEl: false
    loop: true,
    /* slidesPerView || auto - if you want to set width by css like flickity.js layout - in this case width:80% by CSS */
    slidesPerView: 'auto',
    spaceBetween: 24,
    centeredSlides: true,
    // If we need pagination
    pagination: {
      el: '.swiper-pagination',
      clickable: true,
      renderBullet: function (index, className) {
        return '<span class="' + className + '">' + '</span>';
      },
    },
    // Navigation arrows
    // navigation: {
    //   nextEl: '.swiper-button-next',
    //   prevEl: '.swiper-button-prev',
    // },
  });

  // 2 of 2 : PHOTOSWIPE
  var initPhotoSwipeFromDOM = function (gallerySelector) {
    // parse slide data (url, title, size ...) from DOM elements
    // (children of gallerySelector)
    var parseThumbnailElements = function (el) {
      var thumbElements = el.childNodes,
        numNodes = thumbElements.length,
        items = [],
        figureEl,
        linkEl,
        size,
        item;

      for (var i = 0; i < numNodes; i++) {
        figureEl = thumbElements[i]; // <figure> element

        // include only element nodes
        if (figureEl.nodeType !== 1) {
          continue;
        }

        linkEl = figureEl.children[0]; // <a> element

        size = linkEl.getAttribute ('data-size').split ('x');

        // create slide object
        item = {
          src: linkEl.getAttribute ('href'),
          w: parseInt (size[0], 10),
          h: parseInt (size[1], 10),
        };

        if (figureEl.children.length > 1) {
          // <figcaption> content
          item.title = figureEl.children[1].innerHTML;
        }

        if (linkEl.children.length > 0) {
          // <img> thumbnail element, retrieving thumbnail url
          item.msrc = linkEl.children[0].getAttribute ('src');
        }

        item.el = figureEl; // save link to element for getThumbBoundsFn
        items.push (item);
      }

      return items;
    };

    // find nearest parent element
    var closest = function closest (el, fn) {
      return el && (fn (el) ? el : closest (el.parentNode, fn));
    };

    // triggers when user clicks on thumbnail
    var onThumbnailsClick = function (e) {
      e = e || window.event;
      e.preventDefault ? e.preventDefault () : (e.returnValue = false);

      var eTarget = e.target || e.srcElement;

      // find root element of slide
      var clickedListItem = closest (eTarget, function (el) {
        return el.tagName && el.tagName.toUpperCase () === 'LI';
      });

      if (!clickedListItem) {
        return;
      }

      // find index of clicked item by looping through all child nodes
      // alternatively, you may define index via data- attribute
      var clickedGallery = clickedListItem.parentNode,
        childNodes = clickedListItem.parentNode.childNodes,
        numChildNodes = childNodes.length,
        nodeIndex = 0,
        index;

      for (var i = 0; i < numChildNodes; i++) {
        if (childNodes[i].nodeType !== 1) {
          continue;
        }

        if (childNodes[i] === clickedListItem) {
          index = nodeIndex;
          break;
        }
        nodeIndex++;
      }

      if (index >= 0) {
        // open PhotoSwipe if valid index found
        openPhotoSwipe (index, clickedGallery);
      }
      return false;
    };

    // parse picture index and gallery index from URL (#&pid=1&gid=2)
    var photoswipeParseHash = function () {
      var hash = window.location.hash.substring (1), params = {};

      if (hash.length < 5) {
        return params;
      }

      var vars = hash.split ('&');
      for (var i = 0; i < vars.length; i++) {
        if (!vars[i]) {
          continue;
        }
        var pair = vars[i].split ('=');
        if (pair.length < 2) {
          continue;
        }
        params[pair[0]] = pair[1];
      }

      if (params.gid) {
        params.gid = parseInt (params.gid, 10);
      }

      return params;
    };

    var openPhotoSwipe = function (
      index,
      galleryElement,
      disableAnimation,
      fromURL
    ) {
      var pswpElement = document.querySelectorAll ('.pswp')[0],
        gallery,
        options,
        items;

      items = parseThumbnailElements (galleryElement);

      // define options (if needed)

      options = {
        /* "showHideOpacity" uncomment this If dimensions of your small thumbnail don't match dimensions of large image */
        //showHideOpacity:true,

        // Buttons/elements
        closeEl: true,
        captionEl: true,
        fullscreenEl: true,
        zoomEl: true,
        shareEl: true,
        counterEl: false,
        arrowEl: true,
        preloaderEl: true,
        // define gallery index (for URL)
        galleryUID: galleryElement.getAttribute ('data-pswp-uid'),

        getThumbBoundsFn: function (index) {
          // See Options -> getThumbBoundsFn section of documentation for more info
          var thumbnail = items[index].el.getElementsByTagName ('img')[0], // find thumbnail
            pageYScroll =
              window.pageYOffset || document.documentElement.scrollTop,
            rect = thumbnail.getBoundingClientRect ();

          return {x: rect.left, y: rect.top + pageYScroll, w: rect.width};
        },
      };

      // PhotoSwipe opened from URL
      if (fromURL) {
        if (options.galleryPIDs) {
          // parse real index when custom PIDs are used
          // http://photoswipe.com/documentation/faq.html#custom-pid-in-url
          for (var j = 0; j < items.length; j++) {
            if (items[j].pid == index) {
              options.index = j;
              break;
            }
          }
        } else {
          // in URL indexes start from 1
          options.index = parseInt (index, 10) - 1;
        }
      } else {
        options.index = parseInt (index, 10);
      }

      // exit if index not found
      if (isNaN (options.index)) {
        return;
      }

      if (disableAnimation) {
        options.showAnimationDuration = 0;
      }

      // Pass data to PhotoSwipe and initialize it
      gallery = new PhotoSwipe (
        pswpElement,
        PhotoSwipeUI_Default,
        items,
        options
      );
      gallery.init ();

      /* EXTRA CODE (NOT FROM THE CORE) - UPDATE SWIPER POSITION TO THE CURRENT ZOOM_IN IMAGE (BETTER UI) */

      // photoswipe event: Gallery unbinds events
      // (triggers before closing animation)
      gallery.listen ('unbindEvents', function () {
        // This is index of current photoswipe slide
        var getCurrentIndex = gallery.getCurrentIndex ();
        // Update position of the slider
        mySwiper.slideTo (getCurrentIndex, false);
      });
    };

    // loop through all gallery elements and bind events
    var galleryElements = document.querySelectorAll (gallerySelector);

    for (var i = 0, l = galleryElements.length; i < l; i++) {
      galleryElements[i].setAttribute ('data-pswp-uid', i + 1);
      galleryElements[i].onclick = onThumbnailsClick;
    }

    // Parse URL and open gallery if it contains #&pid=3&gid=1
    var hashData = photoswipeParseHash ();
    if (hashData.pid && hashData.gid) {
      openPhotoSwipe (
        hashData.pid,
        galleryElements[hashData.gid - 1],
        true,
        true
      );
    }
  };

  // execute above function
  initPhotoSwipeFromDOM ('.my-gallery');
};

$ (document).ready (function () {
  // Replace Footer
  $ ('#footer').replaceWith ($ ('.geowave-footer'));

  // Update Document Title
  var docTitle = $ ('#doc-title');
  if (docTitle !== null) {
    if (typeof doc_name !== 'undefined') {
      docTitle.text (doc_name.toUpperCase ());
    } else {
      $ ('#doc-title-separator').remove ();
      docTitle.remove ();
    }
  }

  var populateMenu = function (menu, menuItems) {
    if (menu !== null) {
      for (var item in menuItems) {
        if (menuItems[item] === null) {
          menu.append ('<hr class="my-1">');
        } else {
          menu.append (
            '<a class="dropdown-item" href="' +
              menuItems[item] +
              '">' +
              item +
              '</a>'
          );
        }
      }
    }
  };

  // Populate Menus
  populateMenu ($ ('#documentation-menu'), documentationMenuItems);
  populateMenu ($ ('#support-menu'), supportMenuItems);
  populateMenu ($ ('#github-menu'), githubMenuItems);

  // Populate Versions
  var path = window.location.pathname;
  var currentPage = 'index.html';
  if (path.endsWith ('.html')) {
    var currentPage = path.split ('/').pop ();
  }
  var currentVersion = $ ('#current-version');
  var latest = true;
  if (currentVersion !== null) {
    currentVersion.text ('Version ' + geowave_version);
    if (geowave_version in versions) {
      latest = false;
    }
  }

  var versionContents = function (name) {
    if (name === null) {
      if (latest) {
        return '<b>Latest Snapshot</b>';
      } else {
        return 'Latest Snapshot';
      }
    } else if (!latest && name == geowave_version) {
      return '<b>Version ' + name + '</b>';
    } else {
      return 'Version ' + name;
    }
  };

  var versionMenu = $ ('#version-menu');
  if (versionMenu !== null) {
    versionMenu.append (
      '<a class="dropdown-item" href="https://locationtech.github.io/geowave/latest/' +
        currentPage +
        '">' +
        versionContents (null) +
        '</a>'
    );
    for (var version in versions) {
      versionMenu.append (
        '<a class="dropdown-item" href="' +
          versions[version].replace ('%%page%%', currentPage) +
          '">' +
          versionContents (version) +
          '</a>'
      );
    }
  }

  // Init Swiper
  if (typeof Swiper !== 'undefined') {
    initPhotoSwipe ();
  }

  // Fade out preloader
  var preloader = $ ('.preloader')[0];
  if (preloader != null) {
    setTimeout(() => {
      preloader.style.opacity = 0;
      var fadeEffect = setInterval (() => {
        preloader.style.display = 'none';
        clearInterval (fadeEffect);
      }, 300);
    }, 100);
  }
});
