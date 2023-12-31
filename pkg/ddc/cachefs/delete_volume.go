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

import volumehelper "github.com/fluid-cloudnative/fluid/pkg/utils/dataset/volume"

func (c CacheFSEngine) DeleteVolume() (err error) {
	if c.runtime == nil {
		c.runtime, err = c.getRuntime()
		if err != nil {
			return
		}
	}

	err = c.deleteFusePersistentVolumeClaim()
	if err != nil {
		return
	}

	err = c.deleteFusePersistentVolume()
	if err != nil {
		return
	}

	return
}

// deleteFusePersistentVolume
func (c *CacheFSEngine) deleteFusePersistentVolume() (err error) {
	runtimeInfo, err := c.getRuntimeInfo()
	if err != nil {
		return err
	}

	return volumehelper.DeleteFusePersistentVolume(c.Client, runtimeInfo, c.Log)
}

// deleteFusePersistentVolume
func (c *CacheFSEngine) deleteFusePersistentVolumeClaim() (err error) {
	runtimeInfo, err := c.getRuntimeInfo()
	if err != nil {
		return err
	}

	return volumehelper.DeleteFusePersistentVolumeClaim(c.Client, runtimeInfo, c.Log)
}
