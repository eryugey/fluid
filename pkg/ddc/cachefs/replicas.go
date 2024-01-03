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
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
)

// SyncReplicas syncs the replicas
func (c *CacheFSEngine) SyncReplicas(ctx cruntime.ReconcileRequestContext) (err error) {
	var (
		workerName string = c.getWorkerName()
		namespace  string = c.namespace
	)

	workers, err := kubeclient.GetStatefulSet(c.Client, workerName, namespace)
	if err != nil {
		return err
	}

	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		runtime, err := c.getRuntime()
		if err != nil {
			return err
		}
		runtimeToUpdate := runtime.DeepCopy()
		err = c.Helper.SyncReplicas(ctx, runtimeToUpdate, runtimeToUpdate.Status, workers)
		return err
	})
	if err != nil {
		return utils.LoggingErrorExceptConflict(c.Log, err, "Failed to sync the replicas",
			types.NamespacedName{Namespace: c.namespace, Name: c.name})
	}

	return
}
