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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/ctrl"
	fluiderrs "github.com/fluid-cloudnative/fluid/pkg/errors"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
)

// SyncRuntime syncs the runtime spec
func (c *CacheFSEngine) SyncRuntime(ctx cruntime.ReconcileRequestContext) (changed bool, err error) {
	runtime, err := c.getRuntime()
	if err != nil {
		return changed, err
	}

	var value *CacheFS
	value, err = c.transform(runtime)
	if err != nil {
		return
	}

	// 1. sync workers
	workerChanged, err := c.syncWorkerSpec(ctx, runtime, value)
	if err != nil {
		return
	}
	if workerChanged {
		c.Log.Info("Worker Spec is updated", "name", ctx.Name, "namespace", ctx.Namespace)
		return workerChanged, err
	}

	// 2. sync fuse
	fuseChanged, err := c.syncFuseSpec(ctx, runtime, value)
	if err != nil {
		return
	}
	if fuseChanged {
		c.Log.Info("Fuse Spec is updated", "name", ctx.Name, "namespace", ctx.Namespace)
		return fuseChanged, err
	}
	return
}

func (c *CacheFSEngine) syncWorkerSpec(ctx cruntime.ReconcileRequestContext, runtime *datav1alpha1.CacheFSRuntime, value *CacheFS) (changed bool, err error) {
	c.Log.V(1).Info("syncWorkerSpec")
	var cmdChanged bool
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		workers, err := ctrl.GetWorkersAsStatefulset(c.Client,
			types.NamespacedName{Namespace: c.namespace, Name: c.getWorkerName()})
		if err != nil {
			return err
		}

		workersToUpdate := workers.DeepCopy()

		// nodeSelector
		if nodeSelectorChanged, newSelector := c.isNodeSelectorChanged(workersToUpdate.Spec.Template.Spec.NodeSelector, value.Worker.NodeSelector); nodeSelectorChanged {
			workersToUpdate.Spec.Template.Spec.NodeSelector = newSelector
			changed = true
		}

		// volumes
		if volumeChanged, newVolumes := c.isVolumesChanged(workersToUpdate.Spec.Template.Spec.Volumes, value.Worker.Volumes); volumeChanged {
			workersToUpdate.Spec.Template.Spec.Volumes = newVolumes
			changed = true
		}

		// labels
		if labelChanged, newLabels := c.isLabelsChanged(workersToUpdate.Spec.Template.ObjectMeta.Labels, value.Worker.Labels); labelChanged {
			workersToUpdate.Spec.Template.ObjectMeta.Labels = newLabels
			changed = true
		}

		// annotations
		if annoChanged, newAnnos := c.isAnnotationsChanged(workersToUpdate.Spec.Template.ObjectMeta.Annotations, value.Worker.Annotations); annoChanged {
			workersToUpdate.Spec.Template.ObjectMeta.Annotations = newAnnos
			changed = true
		}

		// options -> configmap
		workerCommand, err := c.getWorkerCommand()
		if err != nil || workerCommand == "" {
			c.Log.Error(err, "Failed to get worker command")
			return err
		}
		cmdChanged, _ = c.isCommandChanged(workerCommand, value.Worker.Command)

		if len(workersToUpdate.Spec.Template.Spec.Containers) == 1 {
			// resource
			if resourcesChanged, newResources := c.isResourcesChanged(workersToUpdate.Spec.Template.Spec.Containers[0].Resources, runtime.Spec.Worker.Resources); resourcesChanged {
				workersToUpdate.Spec.Template.Spec.Containers[0].Resources = newResources
				changed = true
			}

			// env
			if envChanged, newEnvs := c.isEnvsChanged(workersToUpdate.Spec.Template.Spec.Containers[0].Env, value.Worker.Envs); envChanged {
				workersToUpdate.Spec.Template.Spec.Containers[0].Env = newEnvs
				changed = true
			}

			// volumeMounts
			if volumeMountChanged, newVolumeMounts := c.isVolumeMountsChanged(workersToUpdate.Spec.Template.Spec.Containers[0].VolumeMounts, value.Worker.VolumeMounts); volumeMountChanged {
				workersToUpdate.Spec.Template.Spec.Containers[0].VolumeMounts = newVolumeMounts
				changed = true
			}

			// image
			runtimeImage := value.Image
			if value.ImageTag != "" {
				runtimeImage = runtimeImage + ":" + value.ImageTag
			}
			if imageChanged, newImage := c.isImageChanged(workersToUpdate.Spec.Template.Spec.Containers[0].Image, runtimeImage); imageChanged {
				workersToUpdate.Spec.Template.Spec.Containers[0].Image = newImage
				changed = true
			}
		}

		if cmdChanged {
			c.Log.Info("The worker config is updated")
			err = c.updateWorkerScript(value.Worker.Command)
			if err != nil {
				c.Log.Error(err, "Failed to update the sts config")
				return err
			}
			if !changed {
				// if worker sts not changed, rollout worker sts to reload the script
				c.Log.Info("rollout restart worker", "sts", workersToUpdate.Name)
				workersToUpdate.Spec.Template.ObjectMeta.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)
				changed = true
			}
		} else {
			c.Log.V(1).Info("The worker config is not changed")
		}

		if changed {
			if reflect.DeepEqual(workers, workersToUpdate) {
				changed = false
				c.Log.V(1).Info("The worker is not changed, skip")
				return nil
			}
			c.Log.Info("The worker is updated")

			err = c.Client.Update(context.TODO(), workersToUpdate)
			if err != nil {
				c.Log.Error(err, "Failed to update the sts spec")
			}
		} else {
			c.Log.V(1).Info("The worker is not changed")
		}

		return err
	})

	if fluiderrs.IsDeprecated(err) {
		c.Log.Info("Warning: the current runtime is created by runtime controller before v0.7.0, update specs are not supported. To support these features, please create a new dataset", "details", err)
		return false, nil
	}

	return
}

func (c *CacheFSEngine) syncFuseSpec(ctx cruntime.ReconcileRequestContext, runtime *datav1alpha1.CacheFSRuntime, value *CacheFS) (changed bool, err error) {
	c.Log.V(1).Info("syncFuseSpec")
	var cmdChanged bool
	err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fuses, err := kubeclient.GetDaemonset(c.Client, c.getFuseDaemonsetName(), c.namespace)
		if err != nil {
			return err
		}

		fusesToUpdate := fuses.DeepCopy()

		// nodeSelector
		if nodeSelectorChanged, newSelector := c.isNodeSelectorChanged(fusesToUpdate.Spec.Template.Spec.NodeSelector, value.Fuse.NodeSelector); nodeSelectorChanged {
			fusesToUpdate.Spec.Template.Spec.NodeSelector = newSelector
			changed = true
		}

		// volumes
		if volumeChanged, newVolumes := c.isVolumesChanged(fusesToUpdate.Spec.Template.Spec.Volumes, value.Fuse.Volumes); volumeChanged {
			fusesToUpdate.Spec.Template.Spec.Volumes = newVolumes
			changed = true
		}

		// labels
		if labelChanged, newLabels := c.isLabelsChanged(fusesToUpdate.Spec.Template.ObjectMeta.Labels, value.Fuse.Labels); labelChanged {
			fusesToUpdate.Spec.Template.ObjectMeta.Labels = newLabels
			changed = true
		}

		// annotations
		if annoChanged, newAnnos := c.isAnnotationsChanged(fusesToUpdate.Spec.Template.ObjectMeta.Annotations, value.Fuse.Annotations); annoChanged {
			fusesToUpdate.Spec.Template.ObjectMeta.Annotations = newAnnos
			changed = true
		}

		// options -> configmap
		fuseCommand, err := c.getFuseCommand()
		if err != nil || fuseCommand == "" {
			c.Log.Error(err, "Failed to get fuse command")
			return err
		}
		cmdChanged, _ = c.isCommandChanged(fuseCommand, value.Fuse.Command)

		if len(fusesToUpdate.Spec.Template.Spec.Containers) == 1 {
			// resource
			if resourcesChanged, newResources := c.isResourcesChanged(fusesToUpdate.Spec.Template.Spec.Containers[0].Resources, runtime.Spec.Fuse.Resources); resourcesChanged {
				fusesToUpdate.Spec.Template.Spec.Containers[0].Resources = newResources
				changed = true
			}

			// env
			if envChanged, newEnvs := c.isEnvsChanged(fusesToUpdate.Spec.Template.Spec.Containers[0].Env, value.Fuse.Envs); envChanged {
				fusesToUpdate.Spec.Template.Spec.Containers[0].Env = newEnvs
				changed = true
			}

			// volumeMounts
			if volumeMountChanged, newVolumeMounts := c.isVolumeMountsChanged(fusesToUpdate.Spec.Template.Spec.Containers[0].VolumeMounts, value.Fuse.VolumeMounts); volumeMountChanged {
				fusesToUpdate.Spec.Template.Spec.Containers[0].VolumeMounts = newVolumeMounts
				changed = true
			}

			// image
			fuseImage := value.Fuse.Image
			if value.ImageTag != "" {
				fuseImage = fuseImage + ":" + value.Fuse.ImageTag
			}
			if imageChanged, newImage := c.isImageChanged(fusesToUpdate.Spec.Template.Spec.Containers[0].Image, fuseImage); imageChanged {
				fusesToUpdate.Spec.Template.Spec.Containers[0].Image = newImage
				changed = true
			}
		}

		if cmdChanged {
			c.Log.Info("The fuse config is updated")
			err = c.updateFuseScript(value.Fuse.Command)
			if err != nil {
				c.Log.Error(err, "Failed to update the ds config")
				return err
			}
		} else {
			c.Log.V(1).Info("The fuse config is not changed")
		}

		if changed {
			if reflect.DeepEqual(fuses, fusesToUpdate) {
				changed = false
				c.Log.V(1).Info("The fuse is not changed, skip")
				return nil
			}
			c.Log.Info("The fuse is updated")

			err = c.Client.Update(context.TODO(), fusesToUpdate)
			if err != nil {
				c.Log.Error(err, "Failed to update the ds spec")
			}
		} else {
			c.Log.V(1).Info("The fuse is not changed")
		}

		return err
	})

	if fluiderrs.IsDeprecated(err) {
		c.Log.Info("Warning: the current runtime is created by runtime controller before v0.7.0, update specs are not supported. To support these features, please create a new dataset", "details", err)
		return false, nil
	}
	return
}

func (c *CacheFSEngine) isVolumeMountsChanged(crtVolumeMounts, runtimeVolumeMounts []corev1.VolumeMount) (changed bool, newVolumeMounts []corev1.VolumeMount) {
	mounts := make(map[string]corev1.VolumeMount)
	for _, mount := range crtVolumeMounts {
		mounts[mount.Name] = mount
	}
	for _, mount := range runtimeVolumeMounts {
		if m, ok := mounts[mount.Name]; !ok || !reflect.DeepEqual(m, mount) {
			c.Log.Info("The volumeMounts is different.", "current sts", crtVolumeMounts, "runtime", runtimeVolumeMounts)
			mounts[mount.Name] = mount
			changed = true
		}
	}
	vms := []corev1.VolumeMount{}
	for _, mount := range mounts {
		vms = append(vms, mount)
	}
	newVolumeMounts = vms
	return
}

func (c CacheFSEngine) isEnvsChanged(crtEnvs, runtimeEnvs []corev1.EnvVar) (changed bool, newEnvs []corev1.EnvVar) {
	envMap := make(map[string]corev1.EnvVar)
	for _, env := range crtEnvs {
		envMap[env.Name] = env
	}
	for _, env := range runtimeEnvs {
		if envMap[env.Name].Value != env.Value || !reflect.DeepEqual(envMap[env.Name].ValueFrom, env.ValueFrom) {
			c.Log.Info("The env is different.", "current sts", crtEnvs, "runtime", runtimeEnvs)
			envMap[env.Name] = env
			changed = true
		}
	}
	envs := []corev1.EnvVar{}
	for _, env := range envMap {
		envs = append(envs, env)
	}
	newEnvs = envs
	return
}

func (c CacheFSEngine) isResourcesChanged(crtResources, runtimeResources corev1.ResourceRequirements) (changed bool, newResources corev1.ResourceRequirements) {
	if !utils.ResourceRequirementsEqual(crtResources, runtimeResources) {
		c.Log.Info("The resource requirement is different.", "current sts", crtResources, "runtime", runtimeResources)
		changed = true
	}
	newResources = runtimeResources
	return
}

func (c CacheFSEngine) isVolumesChanged(crtVolumes, runtimeVolumes []corev1.Volume) (changed bool, newVolumes []corev1.Volume) {
	volumes := make(map[string]corev1.Volume)
	for _, mount := range crtVolumes {
		volumes[mount.Name] = mount
	}
	for _, volume := range runtimeVolumes {
		if m, ok := volumes[volume.Name]; !ok || !reflect.DeepEqual(m, volume) {
			c.Log.Info("The volumes is different.", "current sts", crtVolumes, "runtime", runtimeVolumes)
			volumes[volume.Name] = volume
			changed = true
		}
	}
	vs := []corev1.Volume{}
	for _, volume := range volumes {
		vs = append(vs, volume)
	}
	newVolumes = vs
	return
}

func (c CacheFSEngine) isLabelsChanged(crtLabels, runtimeLabels map[string]string) (changed bool, newLabels map[string]string) {
	newLabels = crtLabels
	for k, v := range runtimeLabels {
		if crtv, ok := crtLabels[k]; !ok || crtv != v {
			c.Log.Info("The labels is different.", "current sts", crtLabels, "runtime", runtimeLabels)
			newLabels[k] = v
			changed = true
		}
	}
	return
}

func (c CacheFSEngine) isAnnotationsChanged(crtAnnotations, runtimeAnnotations map[string]string) (changed bool, newAnnotations map[string]string) {
	newAnnotations = crtAnnotations
	for k, v := range runtimeAnnotations {
		if crtv, ok := crtAnnotations[k]; !ok || crtv != v {
			c.Log.Info("The annotations is different.", "current sts", crtAnnotations, "runtime", runtimeAnnotations)
			newAnnotations[k] = v
			changed = true
		}
	}
	return
}

func (c CacheFSEngine) isImageChanged(crtImage, runtimeImage string) (changed bool, newImage string) {
	if crtImage != runtimeImage {
		c.Log.Info("The image is different.", "current sts", crtImage, "runtime", runtimeImage)
		changed = true
	}
	newImage = runtimeImage
	return
}

func (c CacheFSEngine) isNodeSelectorChanged(crtNodeSelector, runtimeNodeSelector map[string]string) (changed bool, newNodeSelector map[string]string) {
	if crtNodeSelector == nil {
		crtNodeSelector = map[string]string{}
	}
	if runtimeNodeSelector == nil {
		runtimeNodeSelector = map[string]string{}
	}
	if !reflect.DeepEqual(crtNodeSelector, runtimeNodeSelector) {
		c.Log.Info("The nodeSelector is different.", "current sts", crtNodeSelector, "runtime", runtimeNodeSelector)
		changed = true
	}
	newNodeSelector = runtimeNodeSelector
	return
}

func (c CacheFSEngine) isCommandChanged(crtCommand, runtimeCommand string) (changed bool, newCommand string) {
	getOption := func(command string) map[string]string {
		commands := strings.Split(command, "-o")
		if len(commands) == 1 {
			return map[string]string{}
		}
		options := strings.Split(commands[1], ",")
		optionMap := make(map[string]string)
		for _, option := range options {
			// ignore metrics option, because it may be different when using hostNetwork
			if strings.Contains(option, "metrics") {
				continue
			}
			o := strings.TrimSpace(option)
			os := strings.Split(o, "=")
			if len(os) == 1 {
				optionMap[o] = ""
			} else {
				optionMap[os[0]] = os[1]
			}
		}
		return optionMap
	}
	workerOption := getOption(crtCommand)
	runtimeOption := getOption(runtimeCommand)
	for k, v := range runtimeOption {
		if wv, ok := workerOption[k]; !ok || wv != v {
			c.Log.Info("The command is different.", "current sts", crtCommand, "runtime", runtimeCommand)
			changed = true
		}
	}
	newCommand = runtimeCommand
	return
}
