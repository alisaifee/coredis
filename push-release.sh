#!/bin/bash
cur=$(git rev-parse --abbrev-ref HEAD)
git checkout 2.x
git push origin 2.x --tags
git checkout $cur
