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
	"strconv"
	"strings"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
)

func (c *CacheFSEngine) transformFuse(runtime *datav1alpha1.CacheFSRuntime, dataset *datav1alpha1.Dataset, value *CacheFS) (err error) {
	if len(dataset.Spec.Mounts) <= 0 {
		return errors.New("do not assign mount point")
	}
	mount := dataset.Spec.Mounts[0]

	value.Configs.Name = mount.Name

	// transform image
	image := runtime.Spec.Fuse.Image
	tag := runtime.Spec.Fuse.ImageTag
	imagePullPolicy := runtime.Spec.Fuse.ImagePullPolicy
	value.Fuse.Image, value.Fuse.ImageTag, value.Fuse.ImagePullPolicy, err = c.parseCacheFSImage(image, tag, imagePullPolicy)
	if err != nil {
		return
	}

	// transform envs
	value.Fuse.Envs = runtime.Spec.Fuse.Env

	// transform options
	var tiredStoreLevel *datav1alpha1.Level
	if len(runtime.Spec.TieredStore.Levels) != 0 {
		tiredStoreLevel = &runtime.Spec.TieredStore.Levels[0]
	}
	optionsFromDataset, err := c.genValue(mount, tiredStoreLevel, value, dataset.Spec.SharedOptions, dataset.Spec.SharedEncryptOptions)
	if err != nil {
		return err
	}

	// transform format cmd
	c.genFormatCmd(value, runtime.Spec.Configs, optionsFromDataset)

	// transform mount options
	options, err := c.genMountOptions(mount, tiredStoreLevel)
	if err != nil {
		return err
	}

	// Keep mount options in dataset still work, but it can be overwrited by fuse speicifed option
	options = utils.UnionMapsWithOverride(optionsFromDataset, options)
	for k, v := range runtime.Spec.Fuse.Options {
		options[k] = v
	}

	// transform mount cmd & stat cmd
	err = c.genFuseMount(value, options, (runtime.Spec.ImportDataset != ""))
	if err != nil {
		return err
	}

	// transform nodeSelector
	c.transformFuseNodeSelector(runtime, value)
	value.Fuse.Enabled = true

	// transform resource
	err = c.transformResourcesForFuse(runtime, value)
	if err != nil {
		return err
	}
	// transform volumes for fuse
	err = c.transformFuseVolumes(runtime, value)
	if err != nil {
		c.Log.Error(err, "failed to transform volumes for fuse")
		return err
	}
	// transform cache volumes for fuse
	err = c.transformFuseCacheVolumes(runtime, value)
	if err != nil {
		c.Log.Error(err, "failed to transform cache volumes for fuse")
		return err
	}

	// set critical fuse pod to avoid eviction
	value.Fuse.CriticalPod = common.CriticalFusePodEnabled()

	// parse fuse container network mode
	value.Fuse.HostNetwork = datav1alpha1.IsHostNetwork(runtime.Spec.Fuse.NetworkMode)
	return
}

func (c *CacheFSEngine) transformFuseNodeSelector(runtime *datav1alpha1.CacheFSRuntime, value *CacheFS) {
	value.Fuse.NodeSelector = map[string]string{}
	if len(runtime.Spec.Fuse.NodeSelector) > 0 {
		value.Fuse.NodeSelector = runtime.Spec.Fuse.NodeSelector
	}

	// The label will be added by CSI Plugin when any workload pod is scheduled on the node.
	value.Fuse.NodeSelector[c.getFuseLabelName()] = "true"
}

// genValue: generate the value of cachefs
func (c *CacheFSEngine) genValue(mount datav1alpha1.Mount, tiredStoreLevel *datav1alpha1.Level, value *CacheFS,
	sharedOptions map[string]string, sharedEncryptOptions []datav1alpha1.EncryptOption) (map[string]string, error) {
	options := make(map[string]string)
	value.Configs.Name = mount.Name
	value.Configs.EncryptEnvOptions = make([]EncryptEnvOption, 0)
	source := ""

	for k, v := range sharedOptions {
		switch k {
		case CacheFSStorage:
			value.Configs.Storage = v
			continue
		case CacheFSBucket:
			value.Configs.Bucket = v
			continue
		default:
			options[k] = v
		}
	}

	for k, v := range mount.Options {
		switch k {
		case CacheFSStorage:
			value.Configs.Storage = v
			continue
		case CacheFSBucket:
			value.Configs.Bucket = v
			continue
		default:
			options[k] = v
		}
	}

	for _, encryptOption := range sharedEncryptOptions {
		key := encryptOption.Name
		secretKeyRef := encryptOption.ValueFrom.SecretKeyRef

		switch key {
		case CacheFSMetaUrl:
			source = "${METAURL}"
			value.Configs.MetaUrlSecret = secretKeyRef.Name
			value.Configs.MetaUrlSecretKey = secretKeyRef.Key
		case CacheFSAccessKey:
			value.Configs.AccessKeySecret = secretKeyRef.Name
			value.Configs.AccessKeySecretKey = secretKeyRef.Key
		case CacheFSSecretKey:
			value.Configs.SecretKeySecret = secretKeyRef.Name
			value.Configs.SecretKeySecretKey = secretKeyRef.Key
		default:
			envName := utils.ConvertDashToUnderscore(key)
			err := utils.CheckValidateEnvName(envName)
			if err != nil {
				return options, err
			}
			value.Configs.EncryptEnvOptions = append(value.Configs.EncryptEnvOptions,
				EncryptEnvOption{
					Name:             key,
					EnvName:          envName,
					SecretKeyRefName: secretKeyRef.Name,
					SecretKeyRefKey:  secretKeyRef.Key,
				})
		}
	}

	for _, encryptOption := range mount.EncryptOptions {
		key := encryptOption.Name
		secretKeyRef := encryptOption.ValueFrom.SecretKeyRef

		switch key {
		case CacheFSMetaUrl:
			source = "${METAURL}"
			value.Configs.MetaUrlSecret = secretKeyRef.Name
			value.Configs.MetaUrlSecretKey = secretKeyRef.Key
		case CacheFSAccessKey:
			value.Configs.AccessKeySecret = secretKeyRef.Name
			value.Configs.AccessKeySecretKey = secretKeyRef.Key
		case CacheFSSecretKey:
			value.Configs.SecretKeySecret = secretKeyRef.Name
			value.Configs.SecretKeySecretKey = secretKeyRef.Key
		default:
			envName := utils.ConvertDashToUnderscore(key)
			err := utils.CheckValidateEnvName(envName)
			if err != nil {
				return options, err
			}
			// options[key] = "${" + envName + "}"
			value.Configs.EncryptEnvOptions = append(value.Configs.EncryptEnvOptions,
				EncryptEnvOption{
					Name:             key,
					EnvName:          envName,
					SecretKeyRefName: secretKeyRef.Name,
					SecretKeyRefKey:  secretKeyRef.Key,
				})
		}
	}

	if source == "" {
		source = mount.Name
	}

	// transform source
	value.Source = source

	// transform mountPath & subPath
	subPath, err := ParseSubPathFromMountPoint(mount.MountPoint)
	if err != nil {
		return options, err
	}
	value.Fuse.MountPath = c.getMountPoint()
	value.Worker.MountPath = c.getMountPoint()
	value.Fuse.HostMountPath = c.getHostMountPoint()
	if subPath != "/" {
		value.Fuse.SubPath = subPath
	}

	var storagePath = DefaultCacheDir
	var volumeType = common.VolumeTypeHostPath
	var volumeSource datav1alpha1.VolumeSource
	if tiredStoreLevel != nil {
		// cachefs cache-dir use colon (:) to separate multiple paths
		storagePath = tiredStoreLevel.Path
		volumeType = tiredStoreLevel.VolumeType
		volumeSource = tiredStoreLevel.VolumeSource
	}
	originPath := strings.Split(storagePath, ":")

	// transform cacheDir
	value.CacheDirs = make(map[string]cache)
	for i, v := range originPath {
		value.CacheDirs[strconv.Itoa(i+1)] = cache{
			Path:         v,
			Type:         string(volumeType),
			VolumeSource: &volumeSource,
		}
	}

	return options, nil
}

func (c *CacheFSEngine) genMountOptions(mount datav1alpha1.Mount, tiredStoreLevel *datav1alpha1.Level) (options map[string]string, err error) {
	options = map[string]string{}
	var subPath string
	subPath, err = ParseSubPathFromMountPoint(mount.MountPoint)
	if subPath != "/" {
		options["subdir"] = subPath
	}

	var storagePath = DefaultCacheDir
	if tiredStoreLevel != nil {
		storagePath = tiredStoreLevel.Path
		if tiredStoreLevel.Quota != nil {
			q := tiredStoreLevel.Quota
			// cachefs cache-size should be integer in MiB
			cacheSize := q.Value() >> 20
			options["cache-size"] = strconv.FormatInt(cacheSize, 10)
		}
		if tiredStoreLevel.Low != "" {
			options["free-space-ratio"] = tiredStoreLevel.Low
		}
	}
	options["cache-dir"] = storagePath
	return
}

// genFuseMount: generate fuse mount args
func (c *CacheFSEngine) genFuseMount(value *CacheFS, optionMap map[string]string, imported bool) (err error) {
	var mountArgs []string
	if optionMap == nil {
		optionMap = map[string]string{}
	}
	readonly := false
	runtimeInfo := c.runtimeInfo
	if runtimeInfo != nil {
		accessModes, err := utils.GetAccessModesOfDataset(c.Client, runtimeInfo.GetName(), runtimeInfo.GetNamespace())
		if err != nil {
			c.Log.Info("Error:", "err", err)
		}
		if len(accessModes) > 0 {
			for _, mode := range accessModes {
				if mode == corev1.ReadOnlyMany {
					optionMap["ro"] = ""
					readonly = true
					break
				}
			}
		}
	}

	if readonly {
		optionMap["attr-cache"] = "7200"
		optionMap["entry-cache"] = "7200"
	}

	// fuse and worker use the same cache-group, fuse use no-sharing
	cacheGroup := fmt.Sprintf("%s-%s", c.namespace, value.FullnameOverride)
	if _, ok := optionMap["cache-group"]; ok {
		cacheGroup = optionMap["cache-group"]
	}
	optionMap["cache-group"] = cacheGroup
	optionMap["no-sharing"] = ""
	optionMap["no-bgjob"] = ""
	optionMap["backup-meta"] = "0"

	// set metrics port
	if _, ok := optionMap["metrics"]; !ok {
		metricsPort := DefaultMetricsPort
		if value.Fuse.MetricsPort != nil {
			metricsPort = *value.Fuse.MetricsPort
		}
		optionMap["metrics"] = fmt.Sprintf("0.0.0.0:%d", metricsPort)
	}

	// mount imported fs, override source with --source option, fuse doesn't have nor need an
	// actual source dir.
	if imported {
		optionMap["source"] = "/tmp"
	}

	mountArgs = []string{common.CacheFSMountPath, value.Source, value.Fuse.MountPath, "-o", strings.Join(genArgs(optionMap), ",")}

	value.Fuse.Command = strings.Join(mountArgs, " ")
	value.Fuse.StatCmd = "stat -c %i " + value.Fuse.MountPath
	return nil
}

// genArgs: generate mount option as `a=b` format
func genArgs(optionMap map[string]string) []string {
	options := []string{}
	for k, v := range optionMap {
		if v != "" {
			k = fmt.Sprintf("%s=%s", k, v)
		}
		options = append(options, k)
	}
	return options
}

func (c *CacheFSEngine) genFormatCmd(value *CacheFS, config *[]string, options map[string]string) {
	args := make([]string, 0)
	if config != nil {
		for _, option := range *config {
			o := strings.TrimSpace(option)
			if o != "" {
				args = append(args, fmt.Sprintf("--%s", o))
			}
		}
	}
	if value.Configs.AccessKeySecret != "" {
		args = append(args, "--access-key=${ACCESS_KEY}")
	}
	if value.Configs.SecretKeySecret != "" {
		args = append(args, "--secret-key=${SECRET_KEY}")
	}
	if value.Configs.Storage == "" || value.Configs.Bucket == "" {
		args = append(args, "--no-update")
	}
	if value.Configs.Storage != "" {
		args = append(args, fmt.Sprintf("--storage=%s", value.Configs.Storage))
	}
	if value.Configs.Bucket != "" {
		args = append(args, fmt.Sprintf("--bucket=%s", value.Configs.Bucket))
	}
	for k, v := range options {
		args = append(args, fmt.Sprintf("--%s=%s", k, v))
	}
	for _, v := range value.Configs.EncryptEnvOptions {
		args = append(args, fmt.Sprintf("--%s=${%s}", v.Name, v.EnvName))
	}
	args = append(args, value.Source, value.Configs.Name)
	cmd := append([]string{common.CacheFSCliPath, "format"}, args...)
	value.Configs.FormatCmd = strings.Join(cmd, " ")
	return
}

func ParseImageTag(imageTag string) (*ClientVersion, *ClientVersion, error) {
	versions := strings.Split(imageTag, "-")
	if len(versions) < 2 {
		return nil, nil, fmt.Errorf("can not parse version from image tag: %s", imageTag)
	}

	ceVersion, err := parseVersion(versions[0])
	if err != nil {
		return nil, nil, err
	}
	eeVersion, err := parseVersion(versions[1])
	if err != nil {
		return nil, nil, err
	}
	return ceVersion, eeVersion, nil
}
