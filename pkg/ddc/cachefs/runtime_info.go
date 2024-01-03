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
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/dataset/volume"
)

// getRuntimeInfo gets runtime info
func (c *CacheFSEngine) getRuntimeInfo() (base.RuntimeInfoInterface, error) {
	if c.runtimeInfo == nil {
		runtime, err := c.getRuntime()
		if err != nil {
			return c.runtimeInfo, err
		}

		c.runtimeInfo, err = base.BuildRuntimeInfo(c.name, c.namespace, c.runtimeType, runtime.Spec.TieredStore, base.WithMetadataList(base.GetMetadataListFromAnnotation(runtime)))
		if err != nil {
			return c.runtimeInfo, err
		}

		// Setup Fuse Deploy Mode
		c.runtimeInfo.SetupFuseDeployMode(true, runtime.Spec.Fuse.NodeSelector)

		if !c.UnitTest {
			// Check if the runtime is using deprecated labels
			isLabelDeprecated, err := c.HasDeprecatedCommonLabelName()
			if err != nil {
				return c.runtimeInfo, err
			}
			c.runtimeInfo.SetDeprecatedNodeLabel(isLabelDeprecated)

			// Check if the runtime is using deprecated naming style for PersistentVolumes
			isPVNameDeprecated, err := volume.HasDeprecatedPersistentVolumeName(c.Client, c.runtimeInfo, c.Log)
			if err != nil {
				return c.runtimeInfo, err
			}
			c.runtimeInfo.SetDeprecatedPVName(isPVNameDeprecated)

			c.Log.Info("Deprecation check finished", "isLabelDeprecated", c.runtimeInfo.IsDeprecatedNodeLabel(), "isPVNameDeprecated", c.runtimeInfo.IsDeprecatedPVName())

			// Setup with Dataset Info
			dataset, err := utils.GetDataset(c.Client, c.name, c.namespace)
			if err != nil {
				if utils.IgnoreNotFound(err) == nil {
					c.Log.Info("Dataset is notfound", "name", c.name, "namespace", c.namespace)
					return c.runtimeInfo, nil
				}

				c.Log.Info("Failed to get dataset when getruntimeInfo")
				return c.runtimeInfo, err
			}

			c.runtimeInfo.SetupWithDataset(dataset)

			c.Log.Info("Setup with dataset done", "exclusive", c.runtimeInfo.IsExclusive())
		}
	}

	return c.runtimeInfo, nil
}
