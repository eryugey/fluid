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

import "github.com/fluid-cloudnative/fluid/pkg/common"

func (c *CacheFSEngine) getCommonLabelName() string {
	return common.LabelAnnotationStorageCapacityPrefix + c.namespace + "-" + c.name
}

func (c *CacheFSEngine) getFuseLabelName() string {
	return common.LabelAnnotationFusePrefix + c.namespace + "-" + c.name
}
