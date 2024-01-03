/*
Copyright 2024 The Fluid Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cachefs

import (
	"regexp"
	"strings"

	"github.com/fluid-cloudnative/fluid/pkg/ddc/cachefs/operations"
)

// GetPodMetrics get cachefs pod metrics
func (c *CacheFSEngine) GetPodMetrics(podName, containerName string) (metrics string, err error) {
	fileUtils := operations.NewCacheFileUtils(podName, containerName, c.namespace, c.Log)
	metrics, err = fileUtils.GetMetric(c.getMountPoint())
	if err != nil {
		return "", err
	}
	return
}

// parseMetric parse cachefs report metric to cache
func (c CacheFSEngine) parseMetric(metrics string) (podMetric fuseMetrics) {
	blockCacheBytes := BlockCacheBytes
	blockCacheHits := BlockCacheHits
	blockCacheMiss := BlockCacheMiss
	blockCacheHitBytes := BlockCacheHitBytes
	blockCacheMissBytes := BlockCacheMissBytes

	counterPattern := regexp.MustCompile(`([^:\s]*):?\s?(.*)`)
	strs := strings.Split(metrics, "\n")
	for _, str := range strs {

		result := counterPattern.FindStringSubmatch(str)
		if len(result) != 3 {
			continue
		}

		switch result[1] {
		case blockCacheBytes:
			podMetric.blockCacheBytes, _ = parseInt64Size(result[2])
		case blockCacheHits:
			podMetric.blockCacheHits, _ = parseInt64Size(result[2])
		case blockCacheMiss:
			podMetric.blockCacheMiss, _ = parseInt64Size(result[2])
		case blockCacheHitBytes:
			podMetric.blockCacheHitsBytes, _ = parseInt64Size(result[2])
		case blockCacheMissBytes:
			podMetric.blockCacheMissBytes, _ = parseInt64Size(result[2])
		default:

		}
	}
	return
}
