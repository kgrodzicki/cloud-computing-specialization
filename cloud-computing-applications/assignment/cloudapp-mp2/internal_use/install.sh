#!/usr/bin/env bash

pwd
rm -rf $PREFIX/dataset/
mkdir -p $PREFIX/dataset/misc $PREFIX/dataset/titles $PREFIX/dataset/links
tar -xjf $XL_HOME/$DATASET.tar.bz2 -C $PREFIX/dataset/misc
mv $PREFIX/dataset/misc/titles* $PREFIX/dataset/titles/
mv $PREFIX/dataset/misc/links* $PREFIX/dataset/links/

cp ./internal_use/patch/$DATASET_PATCH.txt $PREFIX/dataset/misc/league.txt