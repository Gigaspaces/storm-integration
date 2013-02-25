storm-integration  --- EXPERIMENTAL ---
=================

##XAP/Storm integration

This repo contains projects related to the effort to integrate Storm and XAP in various ways.  Cloudify is used as a means of packaging the various projects in the form of recipes.  Currently the recipes are targeted at AWS only.  As such, the repo is divided into two folders: _Cloudify_ and _XAP_.  Cloudify recipes are contained in the _Cloudify_ folder, and source code used to create system components (e.g. plugins, streaming support) are placed in the _XAP_ folder.

Oddly enough, the central service recipes are not stored here, but are contained in the cloudify-recipes repo.  These recipes are _storm-nimbus_, storm-supervisor_, and _zookeeper_.  These are general purpose recipes.  The recipes and code in this repo are, for various reasons, not for public consumption.  An exception that spans both worlds is the _nimbus_thrift_plugin_, a java monitoring plugin for the _storm-nimbus_ recipe.

####Cloudify Components

#####Recipes
* xap9.1-lite service
* xapstream service
* streamdriver service

####XAP Components
* storm
* streamspace-pu
* storm-test-topology
* streaming
