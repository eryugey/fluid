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
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	options "sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/docker"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
)

func (c *CacheFSEngine) getTieredStoreType(runtime *datav1alpha1.CacheFSRuntime) int {
	var mediumType int
	for _, level := range runtime.Spec.TieredStore.Levels {
		mediumType = common.GetDefaultTieredStoreOrder(level.MediumType)
	}
	return mediumType
}

func (c *CacheFSEngine) hasTieredStore(runtime *datav1alpha1.CacheFSRuntime) bool {
	return len(runtime.Spec.TieredStore.Levels) > 0
}

func (c *CacheFSEngine) getDataSetFileNum() (string, error) {
	fileCount, err := c.TotalFileNums()
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(fileCount, 10), err
}

// getRuntime gets the cachefs runtime
func (c *CacheFSEngine) getRuntime() (*datav1alpha1.CacheFSRuntime, error) {
	key := types.NamespacedName{
		Name:      c.name,
		Namespace: c.namespace,
	}

	var runtime datav1alpha1.CacheFSRuntime
	if err := c.Get(context.TODO(), key, &runtime); err != nil {
		return nil, err
	}
	return &runtime, nil
}

func (c *CacheFSEngine) getFuseDaemonsetName() (dsName string) {
	return c.name + "-fuse"
}
func (c *CacheFSEngine) getWorkerName() (dsName string) {
	return c.name + "-worker"
}

func (c *CacheFSEngine) getDaemonset(name string, namespace string) (fuse *appsv1.DaemonSet, err error) {
	fuse = &appsv1.DaemonSet{}
	err = c.Client.Get(context.TODO(), types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}, fuse)

	return fuse, err
}

func (c *CacheFSEngine) GetRunningPodsOfDaemonset(dsName string, namespace string) (pods []corev1.Pod, err error) {
	ds, err := c.getDaemonset(dsName, namespace)
	if err != nil {
		return pods, err
	}

	selector := ds.Spec.Selector.MatchLabels

	pods = []corev1.Pod{}
	podList := &corev1.PodList{}
	err = c.Client.List(context.TODO(), podList, options.InNamespace(namespace), options.MatchingLabels(selector))
	if err != nil {
		return pods, err
	}

	for _, pod := range podList.Items {
		if !podutil.IsPodReady(&pod) {
			c.Log.Info("Skip the pod because it's not ready", "pod", pod.Name, "namespace", pod.Namespace)
			continue
		}
		pods = append(pods, pod)
	}

	return pods, nil
}

func (c *CacheFSEngine) GetRunningPodsOfStatefulSet(stsName string, namespace string) (pods []corev1.Pod, err error) {
	sts, err := kubeclient.GetStatefulSet(c.Client, stsName, namespace)
	if err != nil {
		return pods, err
	}

	selector := sts.Spec.Selector.MatchLabels

	pods = []corev1.Pod{}
	podList := &corev1.PodList{}
	err = c.Client.List(context.TODO(), podList, options.InNamespace(namespace), options.MatchingLabels(selector))
	if err != nil {
		return pods, err
	}

	for _, pod := range podList.Items {
		if !podutil.IsPodReady(&pod) {
			c.Log.Info("Skip the pod because it's not ready", "pod", pod.Name, "namespace", pod.Namespace)
			continue
		}
		pods = append(pods, pod)
	}

	return pods, nil
}

func (c *CacheFSEngine) parseCacheFSImage(image string, tag string, imagePullPolicy string) (string, string, string, error) {
	defaultRuntimeImage := common.CacheFSDefaultImage
	imageFromEnv := docker.GetImageRepoFromEnv(common.CacheFSImageEnv)
	imageTagFromEnv := docker.GetImageTagFromEnv(common.CacheFSImageEnv)
	if len(imagePullPolicy) == 0 {
		imagePullPolicy = common.DefaultImagePullPolicy
	}

	if len(image) == 0 {
		image = imageFromEnv
		if len(image) == 0 {
			runtimeImageInfo := strings.Split(defaultRuntimeImage, ":")
			if len(runtimeImageInfo) < 1 {
				return "", "", "", fmt.Errorf("invalid default cachefs runtime image")
			} else {
				image = runtimeImageInfo[0]
			}
		}
	}

	if len(tag) == 0 {
		tag = imageTagFromEnv
		if len(tag) == 0 {
			runtimeImageInfo := strings.Split(defaultRuntimeImage, ":")
			if len(runtimeImageInfo) < 2 {
				return "", "", "", fmt.Errorf("invalid default cachefs runtime image")
			} else {
				tag = runtimeImageInfo[1]
			}
		}
	}

	return image, tag, imagePullPolicy, nil
}

func (c *CacheFSEngine) getMountPoint() (mountPath string) {
	mountRoot := getMountRoot()
	c.Log.Info("mountRoot", "path", mountRoot)
	return fmt.Sprintf("%s/%s/%s/cachefs-fuse", mountRoot, c.namespace, c.name)
}

func (c *CacheFSEngine) getHostMountPoint() (mountPath string) {
	mountRoot := getMountRoot()
	c.Log.Info("mountRoot", "path", mountRoot)
	return fmt.Sprintf("%s/%s/%s", mountRoot, c.namespace, c.name)
}

func (c *CacheFSEngine) GetValuesConfigMap() (cm *corev1.ConfigMap, err error) {
	cfsValues := c.getHelmValuesConfigMapName()

	cm = &corev1.ConfigMap{}
	err = c.Client.Get(context.TODO(), types.NamespacedName{
		Name:      cfsValues,
		Namespace: c.namespace,
	}, cm)
	if apierrs.IsNotFound(err) {
		err = nil
		cm = nil
	}

	return
}

// getMountRoot returns the default path, if it's not set
func getMountRoot() (path string) {
	path, err := utils.GetMountRoot()
	if err != nil {
		path = "/" + common.CacheFSRuntime
	} else {
		path = path + "/" + common.CacheFSRuntime
	}
	return
}

func parseInt64Size(sizeStr string) (int64, error) {
	size, err := strconv.ParseFloat(sizeStr, 64)
	return int64(size), err
}

func ParseSubPathFromMountPoint(mountPoint string) (string, error) {
	cPath := strings.Split(mountPoint, "cachefs://")
	if len(cPath) != 2 {
		return "", fmt.Errorf("MountPoint error, can not parse cfs path %s", cPath)
	}
	return cPath[1], nil
}

func GetMetricsPort(options map[string]string) (int, error) {
	port := int64(9567)
	if options == nil {
		return int(port), nil
	}

	for k, v := range options {
		if k == "metrics" {
			re := regexp.MustCompile(`.*:([0-9]{1,6})`)
			match := re.FindStringSubmatch(v)
			if len(match) == 0 {
				return DefaultMetricsPort, fmt.Errorf("invalid metrics port: %s", v)
			}
			port, _ = strconv.ParseInt(match[1], 10, 32)
			break
		}
	}

	return int(port), nil
}

func parseVersion(version string) (*ClientVersion, error) {
	re := regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)(?:-(.+))?$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(version))
	if matches == nil || len(matches) < 4 {
		return nil, fmt.Errorf("invalid version string: %s", version)
	}
	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid major version: %s", matches[1])
	}
	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, fmt.Errorf("invalid minor version: %s", matches[2])
	}
	patch, err := strconv.Atoi(matches[3])
	if err != nil {
		return nil, fmt.Errorf("invalid patch version: %s", matches[3])
	}
	var tag string
	if len(matches) > 4 {
		tag = matches[4]
	}

	return &ClientVersion{
		Major: major,
		Minor: minor,
		Patch: patch,
		Tag:   tag,
	}, nil
}

type ClientVersion struct {
	Major, Minor, Patch int
	Tag                 string
}

func (v *ClientVersion) LessThan(other *ClientVersion) bool {
	if v.Major < other.Major {
		return true
	}
	if v.Major > other.Major {
		return false
	}
	if v.Minor < other.Minor {
		return true
	}
	if v.Minor > other.Minor {
		return false
	}
	if v.Patch < other.Patch {
		return true
	}
	return false
}

func (c *CacheFSEngine) getWorkerCommand() (command string, err error) {
	cm, err := kubeclient.GetConfigmapByName(c.Client, c.getWorkerScriptName(), c.namespace)
	if err != nil {
		return "", err
	}
	if cm == nil {
		c.Log.Info("value configMap not found")
		return "", nil
	}
	data := cm.Data
	script := data["script.sh"]
	scripts := strings.Split(script, "\n")
	c.Log.V(1).Info("get worker script", "script", script)

	// mount command is the last one
	for i := len(scripts) - 1; i >= 0; i-- {
		if scripts[i] != "" {
			return scripts[i], nil
		}
	}
	return "", nil
}

func (c *CacheFSEngine) getFuseCommand() (command string, err error) {
	cm, err := kubeclient.GetConfigmapByName(c.Client, c.getFuseScriptName(), c.namespace)
	if err != nil {
		return "", err
	}
	if cm == nil {
		c.Log.Info("value configMap not found")
		return "", nil
	}
	data := cm.Data
	script := data["script.sh"]
	scripts := strings.Split(script, "\n")
	c.Log.V(1).Info("get fuse script", "script", script)

	// mount command is the last one
	for i := len(scripts) - 1; i >= 0; i-- {
		if scripts[i] != "" {
			return scripts[i], nil
		}
	}
	return "", nil
}

func (c CacheFSEngine) updateWorkerScript(command string) error {
	cm, err := kubeclient.GetConfigmapByName(c.Client, c.getWorkerScriptName(), c.namespace)
	if err != nil {
		return err
	}
	if cm == nil {
		c.Log.Info("value configMap not found")
		return nil
	}
	data := cm.Data
	script := data["script.sh"]

	newScript := script
	newScripts := strings.Split(newScript, "\n")
	// mount command is the last one, replace it
	for i := len(newScripts) - 1; i >= 0; i-- {
		if newScripts[i] != "" {
			newScripts[i] = command
			break
		}
	}

	newValues := make(map[string]string)
	newValues["script.sh"] = strings.Join(newScripts, "\n")
	cm.Data = newValues
	return c.Client.Update(context.Background(), cm)
}

func (c CacheFSEngine) updateFuseScript(command string) error {
	cm, err := kubeclient.GetConfigmapByName(c.Client, c.getFuseScriptName(), c.namespace)
	if err != nil {
		return err
	}
	if cm == nil {
		c.Log.Info("value configMap not found")
		return nil
	}
	data := cm.Data
	script := data["script.sh"]

	newScript := script
	newScripts := strings.Split(newScript, "\n")
	// mount command is the last one, replace it
	for i := len(newScripts) - 1; i >= 0; i-- {
		if newScripts[i] != "" {
			newScripts[i] = command
			break
		}
	}

	newValues := make(map[string]string)
	newValues["script.sh"] = strings.Join(newScripts, "\n")
	cm.Data = newValues
	return c.Client.Update(context.Background(), cm)
}

func (c *CacheFSEngine) getWorkerScriptName() string {
	return fmt.Sprintf("%s-worker-script", c.name)
}

func (c *CacheFSEngine) getFuseScriptName() string {
	return fmt.Sprintf("%s-fuse-script", c.name)
}
