/*
Copyright 2021 The Fluid Authors.

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
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
)

// queryCacheStatus checks the cache status
func (c *CacheFSEngine) queryCacheStatus() (states cacheStates, err error) {
	var cachesize uint64
	if len(c.runtime.Spec.TieredStore.Levels) != 0 {
		cachesize, err = strconv.ParseUint(strconv.FormatInt(c.runtime.Spec.TieredStore.Levels[0].Quota.Value(), 10), 10, 64)
		if err != nil {
			return
		}
	}
	// if cacheSize is overwritten in worker options
	if cacheSizeStr := c.runtime.Spec.Worker.Options["cache-size"]; cacheSizeStr != "" {
		var cacheSizeMB uint64
		cacheSizeMB, err = strconv.ParseUint(cacheSizeStr, 10, 64)
		if err != nil {
			return
		}

		// cacheSize is in MiB
		cachesize = cacheSizeMB * 1024 * 1024
	}
	if cachesize != 0 {
		states.cacheCapacity = utils.BytesSize(float64(cachesize * uint64(c.runtime.Status.WorkerNumberReady)))
	}

	var pods []corev1.Pod
	// cachefs use cache of workers which form a cache group
	containerName := common.CacheFSWorkerContainer
	stsName := c.getWorkerName()
	pods, err = c.GetRunningPodsOfStatefulSet(stsName, c.namespace)
	if err != nil || len(pods) == 0 {
		return
	}

	podMetrics := []fuseMetrics{}
	for _, pod := range pods {
		podMetricStr, err := c.GetPodMetrics(pod.Name, containerName)
		if err != nil {
			return states, err
		}
		podMetric := c.parseMetric(podMetricStr)
		podMetrics = append(podMetrics, podMetric)
	}

	var totalSpace int64
	if len(podMetrics) != 0 {
		totalSpace, _ = c.UsedStorageBytes()
	}
	var totalCache, totalCacheHits, totalCacheMiss, totalCacheHitThroughput, totalCacheMissThroughput int64
	for _, p := range podMetrics {
		totalCache += p.blockCacheBytes
		totalCacheHits += p.blockCacheHits
		totalCacheMiss += p.blockCacheMiss
		totalCacheHitThroughput += p.blockCacheHitsBytes
		totalCacheMissThroughput += p.blockCacheMissBytes
	}

	// caches = total cache of worker pod num
	states.cached = utils.BytesSize(float64(totalCache))

	// cachePercent = cached / total space
	if totalSpace != 0 {
		states.cachedPercentage = fmt.Sprintf("%.1f%%", float64(totalCache)*100.0/float64(totalSpace))
	} else {
		states.cachedPercentage = "0.0%"
	}

	// cacheHitRatio = total cache hits / (total cache hits + total cache miss)
	totalCacheCounts := totalCacheHits + totalCacheMiss
	if totalCacheCounts != 0 {
		states.cacheHitRatio = fmt.Sprintf("%.1f%%", float64(totalCacheHits)*100.0/float64(totalCacheCounts))
	} else {
		states.cacheHitRatio = "0.0%"
	}

	// cacheHitRatio = total cache hits / (total cache hits + total cache miss)
	totalCacheThroughput := totalCacheHitThroughput + totalCacheMissThroughput
	if totalCacheThroughput != 0 {
		states.cacheThroughputRatio = fmt.Sprintf("%.1f%%", float64(totalCacheHitThroughput)*100.0/float64(totalCacheThroughput))
	} else {
		states.cacheThroughputRatio = "0.0%"
	}
	return
}
