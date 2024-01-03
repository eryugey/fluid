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

package operations

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
)

type CacheFileUtils struct {
	podName   string
	namespace string
	container string
	log       logr.Logger
}

func NewCacheFileUtils(podName string, containerName string, namespace string, log logr.Logger) CacheFileUtils {
	return CacheFileUtils{
		podName:   podName,
		namespace: namespace,
		container: containerName,
		log:       log,
	}
}

// Load the metadata without timeout
func (c CacheFileUtils) LoadMetadataWithoutTimeout(cachefsPath string) (err error) {
	var (
		command = []string{"ls", "-lR", cachefsPath}
		stdout  string
		stderr  string
	)

	start := time.Now()
	stdout, stderr, err = c.execWithoutTimeout(command)
	duration := time.Since(start)
	c.log.Info("Async Load Metadata took times to run", "period", duration)
	if err != nil {
		err = fmt.Errorf("execute command %v with expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	} else {
		c.log.Info("Async Load Metadata finished", "stdout", stdout)
	}
	return
}

// IsExist checks if the cachePath exists
func (c CacheFileUtils) IsExist(cacheSubPath string) (found bool, err error) {
	var (
		command = []string{"ls", cacheSubPath}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = c.exec(command)
	if err != nil {
		if strings.Contains(stdout, "No such file or directory") || strings.Contains(stderr, "No such file or directory") {
			return false, nil
		} else {
			err = fmt.Errorf("execute command %v with expectedErr: %v, stdout \"%s\", stderr \"%s\"", command, err, stdout, stderr)
			return false, err
		}
	} else {
		found = true
	}
	return
}

// The count of the CacheFS Filesystem
func (c CacheFileUtils) Count(cacheSubPath string) (total int64, err error) {
	var (
		command = []string{"du", "-sb", cacheSubPath}
		stdout  string
		stderr  string
		utotal  uint64
	)

	stdout, stderr, err = c.exec(command)
	if err != nil {
		err = fmt.Errorf("execute command %v with expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}

	// [File Count Folder Count Total Bytes 1152 4 154262709011]
	str := strings.Split(stdout, "\n")

	if len(str) != 1 {
		err = fmt.Errorf("failed to parse %s in Count method", str)
		return
	}

	data := strings.Fields(str[0])
	if len(data) != 2 {
		err = fmt.Errorf("failed to parse %s in Count method", data)
		return
	}

	utotal, err = strconv.ParseUint(data[0], 10, 64)
	if err != nil {
		return
	}

	return int64(utotal), err
}

// file count of the CacheFS Filesystem (except folder)
func (c CacheFileUtils) GetFileCount(cacheSubPath string) (fileCount int64, err error) {
	var (
		// exclude .accesslog .stats .config internal files
		strs    = fmt.Sprintf("find %s ! -path \"%s/.[acs]*\" -type f | wc -l", cacheSubPath, cacheSubPath)
		command = []string{"bash", "-c", strs}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = c.exec(command)
	if err != nil {
		err = fmt.Errorf("execute command %v with expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}

	// eg: Master.FilesCompleted  (Type: COUNTER, Value: 6,367,897)
	str := strings.Split(stdout, "\n")

	if len(str) != 1 {
		err = fmt.Errorf("failed to parse %s in Count method", str)
		return
	}

	data := strings.Fields(str[0])
	if len(data) != 1 {
		err = fmt.Errorf("failed to parse %s in Count method", data)
		return
	}

	fileCount, err = strconv.ParseInt(data[0], 10, 64)
	if err != nil {
		return
	}

	return fileCount, nil
}

// DeleteCacheDirs delete cache dir in pod
func (c CacheFileUtils) DeleteCacheDirs(dirs []string) (err error) {
	for _, dir := range dirs {
		// cache dir check
		match := ValidCacheDir(dir)
		if !match {
			c.log.Info("invalid cache directory, skip cleaning up", "cacheDir", dir)
			return
		}
	}
	var (
		command = []string{"rm", "-rf"}
		stdout  string
		stderr  string
	)
	command = append(command, dirs...)

	stdout, stderr, err = c.exec(command)
	if err != nil {
		err = fmt.Errorf("execute command %v with expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}
	return
}

// DeleteCacheDir delete cache dir in pod
func (c CacheFileUtils) DeleteCacheDir(dir string) (err error) {
	// cache dir check
	match := ValidCacheDir(dir)
	if !match {
		c.log.Info("invalid cache directory, skip cleaning up", "cacheDir", dir)
		return
	}
	var (
		command = []string{"rm", "-rf", dir}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = c.exec(command)
	if err != nil {
		err = fmt.Errorf("execute command %v with expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}
	return
}

// GetStatus get status of volume
func (c CacheFileUtils) GetStatus(source string) (status string, err error) {
	var (
		command = []string{"/bin/sh", "-c", fmt.Sprintf("cachefs status %s", source)}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = c.exec(command)
	if err != nil {
		err = fmt.Errorf("execute command %v with expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}
	status = stdout
	return
}

// GetMetric Get pod metrics
func (c CacheFileUtils) GetMetric(cachefsPath string) (metrics string, err error) {
	var (
		command = []string{"cat", fmt.Sprintf("%s/%s", cachefsPath, ".stats")}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = c.exec(command)
	if err != nil {
		err = fmt.Errorf("execute command %v with expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}
	metrics = stdout
	return
}

// GetUsedSpace Get used space in byte
// use "df --block-size=1 |grep <cachefsPath>'"
func (c CacheFileUtils) GetUsedSpace(cachefsPath string) (usedSpace int64, err error) {
	var (
		strs    = fmt.Sprintf(`df --block-size=1 |grep %s`, cachefsPath)
		command = []string{"bash", "-c", strs}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = c.exec(command)
	if err != nil {
		err = fmt.Errorf("execute command %v with expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}

	// [<Filesystem>       <Size>  <Used> <Avail> <Use>% <Mounted on>]
	str := strings.TrimSuffix(stdout, "\n")

	data := strings.Fields(str)
	if len(data) != 6 {
		err = fmt.Errorf("failed to parse %s in GetUsedSpace method", data)
		return
	}

	usedSpace, err = strconv.ParseInt(data[2], 10, 64)
	if err != nil {
		return
	}

	return usedSpace, err
}

// exec with timeout
func (c CacheFileUtils) exec(command []string) (stdout string, stderr string, err error) {
	c.log.Info("execute begin", "command", command)
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*1500)
	ch := make(chan string, 1)
	defer cancel()

	go func() {
		stdout, stderr, err = c.execWithoutTimeout(command)
		ch <- "done"
	}()

	select {
	case <-ch:
		c.log.Info("execute in time", "command", command)
	case <-ctx.Done():
		err = fmt.Errorf("timeout when executing %v", command)
	}
	return
}

// execWithoutTimeout
func (c CacheFileUtils) execWithoutTimeout(command []string) (stdout string, stderr string, err error) {
	stdout, stderr, err = kubeclient.ExecCommandInContainer(c.podName, c.container, c.namespace, command)
	if err != nil {
		c.log.Info("Stdout", "Command", command, "Stdout", stdout)
		c.log.Error(err, "Failed", "Command", command, "FailedReason", stderr)
		return
	}
	c.log.V(1).Info("Stdout", "Command", command, "Stdout", stdout)
	return
}

/*
MetadataInfoFile is a yaml file to save the metadata info of dataset, such as ufs total and fileNum
it is in the form of：
	dataset: <Dataset>
	namespace: <Namespace>
	ufstotal: <ufstotal>
	filenum: <filenum>
*/

type KeyOfMetaDataFile string

var (
	DatasetName KeyOfMetaDataFile = "dataset"
	Namespace   KeyOfMetaDataFile = "namespace"
	UfsTotal    KeyOfMetaDataFile = "ufstotal"
	FileNum     KeyOfMetaDataFile = "filenum"
)

// QueryMetaDataInfoIntoFile queries the metadata info file.
func (c CacheFileUtils) QueryMetaDataInfoIntoFile(key KeyOfMetaDataFile, filename string) (value string, err error) {
	line := ""
	switch key {
	case DatasetName:
		line = "1p"
	case Namespace:
		line = "2p"
	case UfsTotal:
		line = "3p"
	case FileNum:
		line = "4p"
	default:
		c.log.Error(errors.New("the key not in  metadatafile"), "key", key)
	}
	var (
		str     = "sed -n '" + line + "' " + filename
		command = []string{"bash", "-c", str}
		stdout  string
		stderr  string
	)
	stdout, stderr, err = c.exec(command)
	if err != nil {
		err = fmt.Errorf("execute command %v with  expectedErr: %v stdout %s and stderr %s", command, err, stdout, stderr)
	} else {
		value = strings.TrimPrefix(stdout, string(key)+": ")
	}
	return
}

func ValidCacheDir(dir string) (match bool) {
	return strings.HasSuffix(dir, "raw/chunks")
}
