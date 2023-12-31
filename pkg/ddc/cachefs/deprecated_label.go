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
	"github.com/fluid-cloudnative/fluid/pkg/common/deprecated"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
)

func (c *CacheFSEngine) getDeprecatedCommonLabelName() string {
	return deprecated.LabelAnnotationStorageCapacityPrefix + c.namespace + "-" + c.name
}

func (c *CacheFSEngine) HasDeprecatedCommonLabelName() (deprecated bool, err error) {
	// return deprecated.LabelAnnotationStorageCapacityPrefix + e.namespace + "-" + e.name

	var (
		fuseName  string = c.getFuseDaemonsetName()
		namespace string = c.namespace
	)

	fuses, err := c.getDaemonset(fuseName, namespace)
	if err != nil {
		if apierrs.IsNotFound(err) {
			c.Log.Info("Fuses with deprecated label not found")
			deprecated = false
			err = nil
			return
		}
		c.Log.Error(err, "Failed to get fuse", "fuseName", fuseName)
		return deprecated, err
	}

	nodeSelectors := fuses.Spec.Template.Spec.NodeSelector
	c.Log.Info("The current node selectors for worker", "fuseName", fuseName, "nodeSelector", nodeSelectors)

	if _, deprecated = nodeSelectors[c.getDeprecatedCommonLabelName()]; deprecated {
		c.Log.Info("the deprecated node selector exists", "nodeselector", c.getDeprecatedCommonLabelName())
	} else {
		c.Log.Info("The deprecated node selector doesn't exist", "nodeselector", c.getDeprecatedCommonLabelName())
	}

	return
}
