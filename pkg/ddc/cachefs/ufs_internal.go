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
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/cachefs/operations"
)

func (c *CacheFSEngine) totalStorageBytesInternal() (total int64, err error) {
	stsName := c.getWorkerName()
	pods, err := c.GetRunningPodsOfStatefulSet(stsName, c.namespace)
	if err != nil || len(pods) == 0 {
		return
	}
	fileUtils := operations.NewCacheFileUtils(pods[0].Name, common.CacheFSWorkerContainer, c.namespace, c.Log)
	total, err = fileUtils.GetUsedSpace(c.getMountPoint())
	if err != nil {
		return
	}

	return
}

func (c *CacheFSEngine) totalFileNumsInternal() (fileCount int64, err error) {
	stsName := c.getWorkerName()
	pods, err := c.GetRunningPodsOfStatefulSet(stsName, c.namespace)
	if err != nil || len(pods) == 0 {
		return
	}
	fileUtils := operations.NewCacheFileUtils(pods[0].Name, common.CacheFSWorkerContainer, c.namespace, c.Log)
	fileCount, err = fileUtils.GetFileCount(c.getMountPoint())
	if err != nil {
		return
	}

	return
}

func (c *CacheFSEngine) usedSpaceInternal() (usedSpace int64, err error) {
	stsName := c.getWorkerName()
	pods, err := c.GetRunningPodsOfStatefulSet(stsName, c.namespace)
	if err != nil || len(pods) == 0 {
		return
	}
	fileUtils := operations.NewCacheFileUtils(pods[0].Name, common.CacheFSWorkerContainer, c.namespace, c.Log)
	usedSpace, err = fileUtils.GetUsedSpace(c.getMountPoint())
	if err != nil {
		return
	}

	return
}
