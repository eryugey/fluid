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
	"github.com/fluid-cloudnative/fluid/pkg/utils"
)

func (c CacheFSEngine) UsedStorageBytes() (int64, error) {
	return c.usedSpaceInternal()
}

func (c CacheFSEngine) FreeStorageBytes() (int64, error) {
	return 0, nil
}

func (c CacheFSEngine) TotalStorageBytes() (int64, error) {
	return c.totalStorageBytesInternal()
}

func (c CacheFSEngine) TotalFileNums() (int64, error) {
	return c.totalFileNumsInternal()
}

func (c CacheFSEngine) ShouldCheckUFS() (should bool, err error) {
	return false, nil
}

func (c CacheFSEngine) PrepareUFS() (err error) {
	return
}

// ShouldUpdateUFS CacheFSEngine hasn't support UpdateOnUFSChange
func (c CacheFSEngine) ShouldUpdateUFS() (ufsToUpdate *utils.UFSToUpdate) {
	return nil
}

func (c CacheFSEngine) UpdateOnUFSChange(ufsToUpdate *utils.UFSToUpdate) (ready bool, err error) {
	return true, nil
}
