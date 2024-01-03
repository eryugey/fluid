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
	"fmt"
	"strconv"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
)

func (c *CacheFSEngine) transformMaster(runtime *datav1alpha1.CacheFSRuntime, value *CacheFS) (err error) {
	// Skip if using external master endpoint
	if !value.Master.Enabled {
		return nil
	}

	// transform image
	image := runtime.Spec.Master.Image
	tag := runtime.Spec.Master.ImageTag
	imagePullPolicy := runtime.Spec.Master.ImagePullPolicy
	value.Master.Image, value.Master.ImageTag, value.Master.ImagePullPolicy, err = c.parseCacheFSImage(true, image, tag, imagePullPolicy)
	if err != nil {
		return err
	}

	// transform envs
	value.Master.Envs = runtime.Spec.Master.Env

	// transform options
	value.Master.Port = DefaultMasterPort
	if len(value.Master.Options) == 0 {
		value.Master.Options = map[string]string{}
	}
	value.Master.Options["port"] = fmt.Sprintf("%d", DefaultMasterPort)
	value.Master.Options["protected-mode"] = "no"
	for k, v := range runtime.Spec.Master.Options {
		if k == "port" {
			port, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("Parse master port %v: %s", v, err)
			}
			value.Master.Port = int(port)
		}
		// Don't allow set protected-mode option, it's always "no".
		if k == "protected-mode" {
			continue
		}
		value.Master.Options[k] = v
	}

	// transform nodeSelector
	value.Master.NodeSelector = map[string]string{}
	if len(runtime.Spec.Master.NodeSelector) > 0 {
		value.Master.NodeSelector = runtime.Spec.Master.NodeSelector
	}

	// transform resource
	err = c.transformResourcesForMaster(runtime, value)
	if err != nil {
		return err
	}

	// transform volumes for master
	err = c.transformMasterVolumes(runtime, value)
	if err != nil {
		c.Log.Error(err, "failed to transform volumes for master")
		return err
	}

	// parse master container network mode
	value.Master.HostNetwork = datav1alpha1.IsHostNetwork(runtime.Spec.Master.NetworkMode)
	return
}
