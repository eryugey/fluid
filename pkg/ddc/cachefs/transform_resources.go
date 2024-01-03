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
	"context"
	"fmt"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
)

func (c *CacheFSEngine) transformResourcesForFuse(runtime *datav1alpha1.CacheFSRuntime, value *CacheFS) (err error) {
	value.Fuse.Resources = common.Resources{
		Requests: common.ResourceList{},
		Limits:   common.ResourceList{},
	}
	if runtime.Spec.Fuse.Resources.Limits != nil {
		c.Log.Info("setting fuse Resources limit")
		for k, v := range runtime.Spec.Fuse.Resources.Limits {
			value.Fuse.Resources.Limits[k] = v.String()
		}
	}

	if runtime.Spec.Fuse.Resources.Requests != nil {
		c.Log.Info("setting fuse Resources request")
		for k, v := range runtime.Spec.Fuse.Resources.Requests {
			value.Fuse.Resources.Requests[k] = v.String()
		}
	}

	// mem set request
	if c.hasTieredStore(runtime) && c.getTieredStoreType(runtime) == 0 && runtime.Spec.Fuse.Options["cache-size"] == "" {
		userQuota := runtime.Spec.TieredStore.Levels[0].Quota
		if userQuota == nil {
			return
		}
		needUpdated := false
		if runtime.Spec.Fuse.Resources.Requests == nil ||
			runtime.Spec.Fuse.Resources.Requests.Memory() == nil ||
			runtime.Spec.Fuse.Resources.Requests.Memory().IsZero() ||
			userQuota.Cmp(*runtime.Spec.Fuse.Resources.Requests.Memory()) > 0 {
			needUpdated = true
		}
		if !runtime.Spec.Fuse.Resources.Limits.Memory().IsZero() &&
			userQuota.Cmp(*runtime.Spec.Fuse.Resources.Limits.Memory()) > 0 {
			return fmt.Errorf("the fuse memory tierdStore's size %v is greater than master limits memory %v",
				userQuota, runtime.Spec.Fuse.Resources.Limits.Memory())
		}

		if needUpdated {
			value.Fuse.Resources.Requests[corev1.ResourceMemory] = userQuota.String()
			err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				runtime, err := c.getRuntime()
				if err != nil {
					return err
				}
				runtimeToUpdate := runtime.DeepCopy()
				if len(runtimeToUpdate.Spec.Fuse.Resources.Requests) == 0 {
					runtimeToUpdate.Spec.Fuse.Resources.Requests = make(corev1.ResourceList)
				}
				runtimeToUpdate.Spec.Fuse.Resources.Requests[corev1.ResourceMemory] = *userQuota
				if !reflect.DeepEqual(runtimeToUpdate, runtime) {
					err = c.Client.Update(context.TODO(), runtimeToUpdate)
					if err != nil {
						return err
					}
				}
				return nil
			})

			if err != nil {
				return err
			}
		}
	}
	return
}

func (c *CacheFSEngine) transformResourcesForWorker(runtime *datav1alpha1.CacheFSRuntime, value *CacheFS) (err error) {
	value.Worker.Resources = common.Resources{
		Requests: common.ResourceList{},
		Limits:   common.ResourceList{},
	}
	if runtime.Spec.Worker.Resources.Limits != nil {
		c.Log.Info("setting worker Resources limit")
		for k, v := range runtime.Spec.Worker.Resources.Limits {
			value.Worker.Resources.Limits[k] = v.String()
		}
	}

	if runtime.Spec.Worker.Resources.Requests != nil {
		c.Log.Info("setting worker Resources request")
		for k, v := range runtime.Spec.Worker.Resources.Requests {
			value.Worker.Resources.Requests[k] = v.String()
		}
	}

	// mem set request
	if c.hasTieredStore(runtime) && c.getTieredStoreType(runtime) == 0 {
		userQuota := runtime.Spec.TieredStore.Levels[0].Quota
		if userQuota == nil {
			return
		}
		needUpdated := false
		if runtime.Spec.Worker.Resources.Requests == nil ||
			runtime.Spec.Worker.Resources.Requests.Memory() == nil ||
			runtime.Spec.Worker.Resources.Requests.Memory().IsZero() ||
			userQuota.Cmp(*runtime.Spec.Worker.Resources.Requests.Memory()) > 0 {
			needUpdated = true
		}
		if !runtime.Spec.Worker.Resources.Limits.Memory().IsZero() &&
			userQuota.Cmp(*runtime.Spec.Worker.Resources.Limits.Memory()) > 0 {
			return fmt.Errorf("the worker memory tierdStore's size %v is greater than master limits memory %v",
				userQuota, runtime.Spec.Worker.Resources.Limits.Memory())
		}

		if needUpdated {
			value.Worker.Resources.Requests[corev1.ResourceMemory] = userQuota.String()
			err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				runtime, err := c.getRuntime()
				if err != nil {
					return err
				}
				runtimeToUpdate := runtime.DeepCopy()
				if len(runtimeToUpdate.Spec.Worker.Resources.Requests) == 0 {
					runtimeToUpdate.Spec.Worker.Resources.Requests = make(corev1.ResourceList)
				}
				runtimeToUpdate.Spec.Worker.Resources.Requests[corev1.ResourceMemory] = *userQuota
				if !reflect.DeepEqual(runtimeToUpdate, runtime) {
					err = c.Client.Update(context.TODO(), runtimeToUpdate)
					if err != nil {
						return err
					}
				}

				return nil
			})

			if err != nil {
				return err
			}
		}
	}
	return
}

func (c *CacheFSEngine) transformResourcesForMaster(runtime *datav1alpha1.CacheFSRuntime, value *CacheFS) (err error) {
	value.Master.Resources = common.Resources{
		Requests: common.ResourceList{},
		Limits:   common.ResourceList{},
	}
	if runtime.Spec.Master.Resources.Limits != nil {
		c.Log.Info("setting master Resources limit")
		for k, v := range runtime.Spec.Master.Resources.Limits {
			value.Master.Resources.Limits[k] = v.String()
		}
	}

	if runtime.Spec.Master.Resources.Requests != nil {
		c.Log.Info("setting master Resources request")
		for k, v := range runtime.Spec.Master.Resources.Requests {
			value.Master.Resources.Requests[k] = v.String()
		}
	}

	maxmem, ok := value.Master.Options["maxmemory"]
	if !ok {
		return
	}

	maxMemory, err := maxMemoryToQuantity(maxmem)
	if err != nil {
		return fmt.Errorf("Invalid maxmemory config \"%s\": %s", maxmem, err)
	}

	// mem set request
	needUpdated := false
	if runtime.Spec.Master.Resources.Requests == nil ||
		runtime.Spec.Master.Resources.Requests.Memory() == nil ||
		runtime.Spec.Master.Resources.Requests.Memory().IsZero() ||
		maxMemory.Cmp(*runtime.Spec.Master.Resources.Requests.Memory()) > 0 {
		needUpdated = true
	}
	if !runtime.Spec.Master.Resources.Limits.Memory().IsZero() &&
		maxMemory.Cmp(*runtime.Spec.Master.Resources.Limits.Memory()) > 0 {
		return fmt.Errorf("the master maxmemory size %v is greater than master limits memory %v",
			maxMemory, runtime.Spec.Master.Resources.Limits.Memory())
	}

	if needUpdated {
		value.Master.Resources.Requests[corev1.ResourceMemory] = maxMemory.String()
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			runtime, err := c.getRuntime()
			if err != nil {
				return err
			}
			runtimeToUpdate := runtime.DeepCopy()
			if len(runtimeToUpdate.Spec.Master.Resources.Requests) == 0 {
				runtimeToUpdate.Spec.Master.Resources.Requests = make(corev1.ResourceList)
			}
			runtimeToUpdate.Spec.Master.Resources.Requests[corev1.ResourceMemory] = maxMemory
			if !reflect.DeepEqual(runtimeToUpdate, runtime) {
				err = c.Client.Update(context.TODO(), runtimeToUpdate)
				if err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			return err
		}
	}
	return
}
