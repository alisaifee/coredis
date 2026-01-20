#!/bin/bash
cur=$(git rev-parse --abbrev-ref HEAD)
git checkout 5.x
git push origin 5.x --tags
git checkout stable
git merge 5.x
git push origin stable
git checkout $cur
