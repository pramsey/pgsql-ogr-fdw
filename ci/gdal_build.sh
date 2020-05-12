#!/bin/bash

GDAL_VERSION=$1

echo "Building GDAL $GDAL_VERSION"
wget http://download.osgeo.org/gdal/$GDAL_VERSION/gdal-$GDAL_VERSION.tar.xz
tar xJf gdal-$GDAL_VERSION.tar.xz; cd gdal-$GDAL_VERSION
./configure --prefix=/usr --enable-debug --without-libtool
make -j4
sudo make install
cd ..
gdalinfo --version

