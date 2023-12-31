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
	"path/filepath"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/fluid-cloudnative/fluid/pkg/utils/transfromer"

	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	cdataload "github.com/fluid-cloudnative/fluid/pkg/dataload"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/cachefs/operations"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/docker"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
)

// CreateDataLoadJob creates the job to load data
func (c *CacheFSEngine) CreateDataLoadJob(ctx cruntime.ReconcileRequestContext, targetDataload datav1alpha1.DataLoad) (err error) {
	log := ctx.Log.WithName("createDataLoadJob")

	// 1. Check if the helm release already exists
	releaseName := utils.GetDataLoadReleaseName(targetDataload.Name)
	jobName := utils.GetDataLoadJobName(releaseName)
	var existed bool
	existed, err = helm.CheckRelease(releaseName, targetDataload.Namespace)
	if err != nil {
		log.Error(err, "failed to check if release exists", "releaseName", releaseName, "namespace", targetDataload.Namespace)
		return err
	}

	// 2. install the helm chart if not exists
	if !existed {
		log.Info("DataLoad job helm chart not installed yet, will install")
		valueFileName, err := c.generateDataLoadValueFile(ctx, &targetDataload)
		if err != nil {
			log.Error(err, "failed to generate dataload chart's value file")
			return err
		}
		chartName := utils.GetChartsDirectory() + "/" + cdataload.DataloadChart + "/" + common.CacheFSRuntime
		err = helm.InstallRelease(releaseName, targetDataload.Namespace, valueFileName, chartName)
		if err != nil {
			log.Error(err, "failed to install dataload chart")
			return err
		}
		log.Info("DataLoad job helm chart successfully installed", "namespace", targetDataload.Namespace, "releaseName", releaseName)
		ctx.Recorder.Eventf(&targetDataload, v1.EventTypeNormal, common.DataLoadJobStarted, "The DataLoad job %s started", jobName)
	}
	return err
}

// generateDataLoadValueFile builds a DataLoadValue by extracted specifications from the given DataLoad, and
// marshals the DataLoadValue to a temporary yaml file where stores values that'll be used by fluid dataloader helm chart
func (c *CacheFSEngine) generateDataLoadValueFile(r cruntime.ReconcileRequestContext, object client.Object) (valueFileName string, err error) {
	dataload, ok := object.(*datav1alpha1.DataLoad)
	if !ok {
		err = fmt.Errorf("object %v is not a DataLoad", object)
		return "", err
	}

	targetDataset, err := utils.GetDataset(r.Client, dataload.Spec.Dataset.Name, dataload.Spec.Dataset.Namespace)
	if err != nil {
		return "", err
	}
	c.Log.Info("target dataset", "dataset", targetDataset)

	imageName, imageTag := docker.GetWorkerImage(r.Client, dataload.Spec.Dataset.Name, "cachefs", dataload.Spec.Dataset.Namespace)

	var defaultCacheFSImage string
	if len(imageName) == 0 || len(imageTag) == 0 {
		defaultCacheFSImage = common.CacheFSDefaultImage
	}

	if len(imageName) == 0 {
		defaultImageInfo := strings.Split(defaultCacheFSImage, ":")
		if len(defaultImageInfo) < 1 {
			err = fmt.Errorf("invalid default dataload image")
			return
		} else {
			imageName = defaultImageInfo[0]
		}
	}

	if len(imageTag) == 0 {
		defaultImageInfo := strings.Split(defaultCacheFSImage, ":")
		if len(defaultImageInfo) < 2 {
			err = fmt.Errorf("invalid default dataload image")
			return
		} else {
			imageTag = defaultImageInfo[1]
		}
	}

	cacheinfo, err := GetCacheInfoFromConfigmap(c.Client, dataload.Spec.Dataset.Name, dataload.Spec.Dataset.Namespace)
	if err != nil {
		return
	}

	stsName := c.getWorkerName()
	pods, err := c.GetRunningPodsOfStatefulSet(stsName, c.namespace)
	if err != nil || len(pods) == 0 {
		return
	}

	image := fmt.Sprintf("%s:%s", imageName, imageTag)
	dataLoadValue := c.genDataLoadValue(image, cacheinfo, pods, targetDataset, dataload)
	data, err := yaml.Marshal(dataLoadValue)
	if err != nil {
		return
	}
	c.Log.Info("dataload value", "value", string(data))

	valueFile, err := os.CreateTemp(os.TempDir(), fmt.Sprintf("%s-%s-loader-values.yaml", dataload.Namespace, dataload.Name))
	if err != nil {
		return
	}
	err = os.WriteFile(valueFile.Name(), data, 0o400)
	if err != nil {
		return
	}
	return valueFile.Name(), nil
}

func (c *CacheFSEngine) genDataLoadValue(image string, cacheinfo map[string]string, pods []v1.Pod, targetDataset *datav1alpha1.Dataset, dataload *datav1alpha1.DataLoad) *cdataload.DataLoadValue {
	imagePullSecrets := docker.GetImagePullSecretsFromEnv(common.EnvImagePullSecretsKey)

	dataloadInfo := cdataload.DataLoadInfo{
		BackoffLimit:     3,
		TargetDataset:    dataload.Spec.Dataset.Name,
		LoadMetadata:     dataload.Spec.LoadMetadata,
		Image:            image,
		Labels:           dataload.Spec.PodMetadata.Labels,
		Annotations:      dataload.Spec.PodMetadata.Annotations,
		ImagePullSecrets: imagePullSecrets,
		Policy:           string(dataload.Spec.Policy),
		Schedule:         dataload.Spec.Schedule,
		Resources:        dataload.Spec.Resources,
	}

	// pod affinity
	if dataload.Spec.Affinity != nil {
		dataloadInfo.Affinity = dataload.Spec.Affinity
	}

	// node selector
	if dataload.Spec.NodeSelector != nil {
		if dataloadInfo.NodeSelector == nil {
			dataloadInfo.NodeSelector = make(map[string]string)
		}
		dataloadInfo.NodeSelector = dataload.Spec.NodeSelector
	}

	// pod tolerations
	if len(dataload.Spec.Tolerations) > 0 {
		if dataloadInfo.Tolerations == nil {
			dataloadInfo.Tolerations = make([]v1.Toleration, 0)
		}
		dataloadInfo.Tolerations = dataload.Spec.Tolerations
	}

	// scheduler name
	if len(dataload.Spec.SchedulerName) > 0 {
		dataloadInfo.SchedulerName = dataload.Spec.SchedulerName
	}

	targetPaths := []cdataload.TargetPath{}
	for _, target := range dataload.Spec.Target {
		fluidNative := utils.IsTargetPathUnderFluidNativeMounts(target.Path, *targetDataset)
		path := strings.TrimSpace(target.Path)
		targetPaths = append(targetPaths, cdataload.TargetPath{
			Path:        path,
			Replicas:    target.Replicas,
			FluidNative: fluidNative,
		})
	}
	dataloadInfo.TargetPaths = targetPaths

	options := map[string]string{}
	// resolve spec options
	if dataload.Spec.Options != nil {
		for key, value := range dataload.Spec.Options {
			options[key] = value
		}
	}

	for key, value := range cacheinfo {
		options[key] = value
	}

	podNames := []string{}
	for _, pod := range pods {
		podNames = append(podNames, pod.Name)
	}
	options["podNames"] = podNames[0]
	options["runtimeName"] = c.name
	if _, ok := options["timeout"]; !ok {
		options["timeout"] = DefaultDataLoadTimeout
	}

	dataloadInfo.Options = options

	dataLoadValue := &cdataload.DataLoadValue{
		Name:         dataload.Name,
		DataLoadInfo: dataloadInfo,
		Owner:        transfromer.GenerateOwnerReferenceFromObject(dataload),
	}

	return dataLoadValue
}

func (c *CacheFSEngine) CheckRuntimeReady() (ready bool) {
	stsName := c.getWorkerName()
	pods, err := c.GetRunningPodsOfStatefulSet(stsName, c.namespace)
	if err != nil || len(pods) == 0 {
		return false
	}
	return true
}

func (c *CacheFSEngine) CheckExistenceOfPath(targetDataload datav1alpha1.DataLoad) (notExist bool, err error) {
	// get mount path
	cacheinfo, err := GetCacheInfoFromConfigmap(c.Client, targetDataload.Spec.Dataset.Name, targetDataload.Spec.Dataset.Namespace)
	if err != nil {
		return
	}
	mountPath := cacheinfo[MountPath]
	if mountPath == "" {
		return true, fmt.Errorf("fail to find mountpath in dataset %s %s", targetDataload.Spec.Dataset.Name, targetDataload.Spec.Dataset.Namespace)
	}

	// get worker pod
	stsName := c.getWorkerName()
	pods, err := c.GetRunningPodsOfStatefulSet(stsName, c.namespace)
	if err != nil || len(pods) == 0 {
		return true, err
	}

	// check path exist
	pod := pods[0]
	fileUtils := operations.NewCacheFileUtils(pod.Name, common.CacheFSWorkerContainer, c.namespace, c.Log)
	for _, target := range targetDataload.Spec.Target {
		targetPath := filepath.Join(mountPath, target.Path)
		isExist, err := fileUtils.IsExist(targetPath)
		if err != nil {
			return true, err
		}
		if !isExist {
			return true, nil
		}
	}
	return false, err
}
