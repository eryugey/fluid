/*
Copyright 2023 The Fluid Authors.

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

const (
	BlockCacheBytes     = "cachefs_blockcache_bytes"
	BlockCacheHits      = "cachefs_blockcache_hits"
	BlockCacheMiss      = "cachefs_blockcache_miss"
	BlockCacheHitBytes  = "cachefs_blockcache_hit_bytes"
	BlockCacheMissBytes = "cachefs_blockcache_miss_bytes"

	workerPodRole      = "cachefs-worker"
	DefaultMetricsPort = 9567

	MetadataSyncNotDoneMsg               = "[Calculating]"
	CheckMetadataSyncDoneTimeoutMillisec = 500

	DefaultCacheDir = "/var/cfsCache"

	CacheFSStorage   = "storage"
	CacheFSBucket    = "bucket"
	CacheFSMetaUrl   = "metaurl"
	CacheFSAccessKey = "access-key"
	CacheFSSecretKey = "secret-key"

	MountPath              = "mountpath"
	DataSourceMountPath    = "/datasource"
	DefaultDataLoadTimeout = "30m"
)
