ElasticSearch FollowUp Plugin 
=============

The plugin tracks CRUD changes made in an ElasticSearch index.

Installation
-----------

Target folder contains compiled binaries ready for use. The first 3 numbers of the version tag refers to the version of ElasticSearch the plugin built for.

Usage
-----------
Track changes in myindex:  `http://localhost:9200/myindex/_followup?start`

Get changes: `http://localhost:9200/myindex/_followup`

Stop tracking: `http://localhost:9200/myindex/_followup?stop`

The tracking buffer is only limited by heap size, don't forget to turn it off. :)
