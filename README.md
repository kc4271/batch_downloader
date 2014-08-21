Batch Downloader
================

Download huge number of small files.

----------------
* Usage: `python batch_downloader.py urllist.lst urllist.log`

* This script download files in `urllist.lst` to `data` folder in current directory. The url in `urllist.lst` has to have a normal filename(such as XXX.jpg).

* `urllist.log` will record download errors.

* Third-party library `eventlet` need python 2.7.5+.   