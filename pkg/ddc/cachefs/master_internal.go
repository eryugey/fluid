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

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"

	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubectl"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
)

// setup fuse
func (c *CacheFSEngine) setupMasterInternal() (err error) {
	var (
		chartName = utils.GetChartsDirectory() + "/" + common.CacheFSChart
	)

	runtime, err := c.getRuntime()
	if err != nil {
		return
	}

	valuefileName, err := c.generateCachefsValueFile(runtime)
	if err != nil {
		return
	}

	found, err := helm.CheckRelease(c.name, c.namespace)
	if err != nil {
		return
	}

	if found {
		c.Log.Info("The release is already installed", "name", c.name, "namespace", c.namespace)
		return
	}

	return helm.InstallRelease(c.name, c.namespace, valuefileName, chartName)
}

// generate cachefs struct
func (c *CacheFSEngine) generateCachefsValueFile(runtime *datav1alpha1.CacheFSRuntime) (valueFileName string, err error) {
	// 0. Check if the configmap exists
	err = kubeclient.DeleteConfigMap(c.Client, c.getHelmValuesConfigMapName(), c.namespace)
	if err != nil {
		c.Log.Error(err, "Failed to clean value files")
		return
	}

	// labelName := common.LabelAnnotationStorageCapacityPrefix + e.runtimeType + "-" + e.name
	// configmapName := e.name + "-" + e.runtimeType + "-values"
	// 1. Transform the runtime to value
	value, err := c.transform(runtime)
	if err != nil {
		return
	}

	c.Log.Info("Generate values", "value", value)

	data, err := yaml.Marshal(value)
	if err != nil {
		return
	}

	// 2. Get the template value file
	valueFile, err := os.CreateTemp(os.TempDir(), fmt.Sprintf("%s-%s-values.yaml", c.name, c.engineImpl))
	if err != nil {
		c.Log.Error(err, "failed to create value file", "valueFile", valueFile.Name())
		return valueFileName, err
	}

	valueFileName = valueFile.Name()
	c.Log.Info("Save the values file", "valueFile", valueFileName)

	err = os.WriteFile(valueFileName, data, 0400)
	if err != nil {
		return
	}

	// 3. Save the configfile into configmap
	err = kubectl.CreateConfigMapFromFile(c.getHelmValuesConfigMapName(), "data", valueFileName, c.namespace)
	if err != nil {
		return
	}

	return valueFileName, err
}

func (c *CacheFSEngine) getHelmValuesConfigMapName() string {
	return fmt.Sprintf("%s-%s-values", c.name, c.engineImpl)
}
