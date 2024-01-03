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

package common

// Runtime for CacheFS
const (
	CacheFSRuntime = "cachefs"

	CacheFSMountType = "fuse.cachefs"

	CacheFSNamespace = "cachefs-system"

	CacheFSChart = CacheFSRuntime

	CacheFSFuseContainer = "cachefs-fuse"

	CacheFSWorkerContainer = "cachefs-worker"

	CacheFSEngineImpl = CacheFSRuntime
)

// Constants for CacheFS images
const (
	CacheFSImageEnv = "CACHEFS_IMAGE_ENV"

	CacheFSDefaultImage = "kangaroo.registry.cn-hangzhou.cr.aliyuncs.com/storage/cachefs:1.0.5-2.1"

	CacheFSDefaultMasterImage = "redis:7.2-alpine"
)

// Constants for CacheFS path conventions
const (
	CacheFSMountPath       = "/bin/mount.cachefs"
	CacheFSCliPath         = "/bin/cachefs"
	CacheFSDefaultCacheDir = "/var/cfsCache"
)
