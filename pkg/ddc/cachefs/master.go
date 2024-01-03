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
	"reflect"

	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"

	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
)

// Check if master is ready
func (c CacheFSEngine) CheckMasterReady() (ready bool, err error) {
	runtime, err := c.getRuntime()
	if err != nil {
		return false, err
	}
	masterSpec := runtime.Spec.Master
	if masterSpec.ExternalEndpoint.MetaUrl != "" || len(masterSpec.ExternalEndpoint.EncryptOptions) > 0 {
		return true, nil
	}

	// 1. Setup
	masterName := c.getMasterName()
	master, err := kubeclient.GetStatefulSet(c.Client, masterName, c.namespace)
	if err != nil {
		return
	}

	// Only has 1 replica for internal master
	var masterReplicas int32 = 1
	if masterReplicas == master.Status.ReadyReplicas {
		ready = true
	} else {
		c.Log.Info("The master is not ready.", "replicas", masterReplicas,
			"readyReplicas", master.Status.ReadyReplicas)
	}

	// 2. Update the phase
	if ready {
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			runtime, err := c.getRuntime()
			if err != nil {
				return err
			}
			runtimeToUpdate := runtime.DeepCopy()

			runtimeToUpdate.Status.CurrentMasterNumberScheduled = int32(master.Status.ReadyReplicas)

			runtimeToUpdate.Status.MasterPhase = datav1alpha1.RuntimePhaseReady

			if len(runtimeToUpdate.Status.Conditions) == 0 {
				runtimeToUpdate.Status.Conditions = []datav1alpha1.RuntimeCondition{}
			}
			cond := utils.NewRuntimeCondition(datav1alpha1.RuntimeMasterReady, datav1alpha1.RuntimeMasterReadyReason,
				"The master is ready.", corev1.ConditionTrue)
			runtimeToUpdate.Status.Conditions =
				utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions, cond)

			if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
				return c.Client.Status().Update(context.TODO(), runtimeToUpdate)
			}

			return nil
		})

		if err != nil {
			c.Log.Error(err, "Update runtime status")
			return
		}
	}

	return
}

func (c CacheFSEngine) ShouldSetupMaster() (should bool, err error) {
	runtime, err := c.getRuntime()
	if err != nil {
		return
	}

	switch runtime.Status.MasterPhase {
	case datav1alpha1.RuntimePhaseNone:
		should = true
	default:
		should = false
	}
	return
}

func (c CacheFSEngine) SetupMaster() (err error) {
	masterName := c.getMasterName()

	// 1. Setup
	_, err = kubeclient.GetStatefulSet(c.Client, masterName, c.namespace)
	if err != nil && apierrs.IsNotFound(err) {
		//1. Is not found error
		c.Log.V(1).Info("SetupMaster", "master", masterName)
		return c.setupMasterInternal()
	} else if err != nil {
		//2. Other errors
		return
	} else {
		//3.The fuse has been set up
		c.Log.V(1).Info("The master has been set.")
	}

	// 2. Update the status of the runtime
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		runtime, err := c.getRuntime()
		if err != nil {
			return err
		}
		runtimeToUpdate := runtime.DeepCopy()

		runtimeToUpdate.Status.MasterPhase = datav1alpha1.RuntimePhaseNotReady

		// Init selector for worker
		runtimeToUpdate.Status.Selector = c.getWorkerSelectors()
		runtimeToUpdate.Status.DesiredMasterNumberScheduled = 1
		runtimeToUpdate.Status.ValueFileConfigmap = c.getHelmValuesConfigMapName()

		if len(runtimeToUpdate.Status.Conditions) == 0 {
			runtimeToUpdate.Status.Conditions = []datav1alpha1.RuntimeCondition{}
		}
		cond := utils.NewRuntimeCondition(datav1alpha1.RuntimeMasterInitialized, datav1alpha1.RuntimeMasterInitializedReason,
			"The master is initialized.", corev1.ConditionTrue)
		runtimeToUpdate.Status.Conditions =
			utils.UpdateRuntimeCondition(runtimeToUpdate.Status.Conditions, cond)

		if !reflect.DeepEqual(runtime.Status, runtimeToUpdate.Status) {
			return c.Client.Status().Update(context.TODO(), runtimeToUpdate)
		}

		return nil
	})

	if err != nil {
		c.Log.Error(err, "Update runtime status")
		return err
	}

	return
}
