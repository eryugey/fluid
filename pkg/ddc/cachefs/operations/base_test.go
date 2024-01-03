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
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/brahma-adshonor/gohook"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
)

const (
	NotExist     = "not-exist"
	OtherErr     = "other-err"
	FINE         = "fine"
	CommonStatus = `{
  "Setting": {
    "Name": "zww-cachefs",
    "UUID": "73416457-6f3f-490b-abb6-cbc1f837944e",
    "Storage": "minio",
    "Bucket": "http://10.98.166.242:9000/zww-cachefs",
    "AccessKey": "minioadmin",
    "SecretKey": "removed",
    "BlockSize": 4096,
    "Compression": "none",
    "Shards": 0,
    "HashPrefix": false,
    "Capacity": 0,
    "Inodes": 0,
    "KeyEncrypted": false,
    "TrashDays": 2,
    "MetaVersion": 0,
    "MinClientVersion": "",
    "MaxClientVersion": ""
  },
  "Sessions": [
    {
      "Sid": 14,
      "Expire": "2022-02-09T10:01:50Z",
      "Version": "1.0-dev (2022-02-09 748949ac)",
      "HostName": "cachefs-pvc-33d9bdf3-5fb5-42fe-bf48-d3d6156b424b-createvol2dv4j",
      "MountPoint": "/mnt/cfs",
      "ProcessID": 20
    }
  ]
}`
)

func TestNewCacheFSFileUtils(t *testing.T) {
	var expectedResult = CacheFileUtils{
		podName:   "cachefs",
		namespace: "default",
		container: common.CacheFSFuseContainer,
		log:       fake.NullLogger(),
	}
	result := NewCacheFileUtils("cachefs", common.CacheFSFuseContainer, "default", fake.NullLogger())
	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("fail to create the CacheFSFileUtils, want: %v, got: %v", expectedResult, result)
	}
}

func TestCacheFileUtils_IsExist(t *testing.T) {
	mockExec := func(a CacheFileUtils, p []string) (stdout string, stderr string, e error) {
		if strings.Contains(p[1], NotExist) {
			return "No such file or directory", "", errors.New("No such file or directory")
		} else if strings.Contains(p[1], OtherErr) {
			return "", "", errors.New("other error")
		} else {
			return "", "", nil
		}
	}

	err := gohook.Hook(CacheFileUtils.exec, mockExec, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.exec)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	var tests = []struct {
		in    string
		out   bool
		noErr bool
	}{
		{NotExist, false, true},
		{OtherErr, false, false},
		{FINE, true, true},
	}
	for _, test := range tests {
		found, err := CacheFileUtils{log: fake.NullLogger()}.IsExist(test.in)
		if found != test.out {
			t.Errorf("input parameter is %s,expected %t, got %t", test.in, test.out, found)
		}
		var noErr bool = (err == nil)
		if test.noErr != noErr {
			t.Errorf("input parameter is %s, expected noerr is %t, got %t", test.in, test.noErr, err)
		}
	}
	wrappedUnhookExec()
}

func TestCacheFileUtils_exec(t *testing.T) {
	ExecWithoutTimeoutCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "Type: COUNTER, Value: 6,367,897", "", nil
	}
	ExecWithoutTimeoutErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}

	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.execWithoutTimeout)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := &CacheFileUtils{log: fake.NullLogger()}
	_, _, err = a.exec([]string{"mkdir", "abc"})
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExec()

	err = gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	_, _, err = a.exec([]string{"mkdir", "abc"})
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	wrappedUnhookExec()
}

func TestCacheFileUtils_GetMetric(t *testing.T) {
	ExecCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "cachefs metrics success", "", nil
	}
	ExecErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.exec)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.exec, ExecErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := CacheFileUtils{}
	_, err = a.GetMetric("/tmp")
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExec()

	err = gohook.Hook(CacheFileUtils.exec, ExecCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	m, err := a.GetMetric("/tmp")
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	if m != "cachefs metrics success" {
		t.Errorf("expected cachefs metrics success, got %s", m)
	}
	wrappedUnhookExec()
}

func TestCacheFileUtils_DeleteCacheDirs(t *testing.T) {
	ExecCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "cachefs rmr success", "", nil
	}
	ExecErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.exec)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.exec, ExecErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := CacheFileUtils{}
	err = a.DeleteCacheDirs([]string{"/tmp/raw/chunks"})
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExec()

	err = gohook.Hook(CacheFileUtils.exec, ExecCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = a.DeleteCacheDirs([]string{"/tmp/raw/chunks"})
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	wrappedUnhookExec()
}

func TestCacheFileUtils_DeleteCacheDir(t *testing.T) {
	ExecCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "cachefs rmr success", "", nil
	}
	ExecErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.exec)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	a := CacheFileUtils{}
	// no error
	err := gohook.Hook(CacheFileUtils.exec, ExecCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = a.DeleteCacheDir("/tmp/raw/chunks")
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	wrappedUnhookExec()

	// error
	err = gohook.Hook(CacheFileUtils.exec, ExecErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = a.DeleteCacheDir("/tmp/raw/chunks")
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExec()
}

func TestCacheFileUtils_GetStatus(t *testing.T) {
	ExecCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return CommonStatus, "", nil
	}
	ExecErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.exec)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.exec, ExecErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := CacheFileUtils{}
	err = a.DeleteCacheDir("/tmp/raw/chunks")
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExec()

	err = gohook.Hook(CacheFileUtils.exec, ExecCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	got, err := a.GetStatus("test")
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	if got != CommonStatus {
		t.Errorf("want %s, got: %v", CommonStatus, got)
	}
	wrappedUnhookExec()
}

func TestCacheFileUtils_LoadMetadataWithoutTimeout(t *testing.T) {
	ExecWithoutTimeoutCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "Load cachefs metadata", "", nil
	}
	ExecWithoutTimeoutErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExecWithoutTimeout := func() {
		err := gohook.UnHook(CacheFileUtils.execWithoutTimeout)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := CacheFileUtils{log: fake.NullLogger()}
	err = a.LoadMetadataWithoutTimeout("/tmp")
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExecWithoutTimeout()

	err = gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = a.LoadMetadataWithoutTimeout("/tmp")
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	wrappedUnhookExecWithoutTimeout()
}

func TestCacheFileUtils_Count(t *testing.T) {
	ExecWithoutTimeoutCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "6367897   /tmp", "", nil
	}
	ExecWithoutTimeoutErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.execWithoutTimeout)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := &CacheFileUtils{log: fake.NullLogger()}
	_, err = a.Count("/tmp")
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExec()

	err = gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	fileCount, err := a.Count("/tmp")
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	if fileCount != 6367897 {
		t.Errorf("check failure, want 6367897, got %d", fileCount)
	}
	wrappedUnhookExec()
}

func TestCacheFileUtils_GetFileCount(t *testing.T) {
	ExecWithoutTimeoutCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "6367897", "", nil
	}
	ExecWithoutTimeoutErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.execWithoutTimeout)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := &CacheFileUtils{log: fake.NullLogger()}
	_, err = a.GetFileCount("/tmp")
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExec()

	err = gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	fileCount, err := a.GetFileCount("/tmp")
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	if fileCount != 6367897 {
		t.Errorf("check failure, want 6367897, got %d", fileCount)
	}
	wrappedUnhookExec()
}

func TestCacheFileUtils_GetUsedSpace(t *testing.T) {
	ExecWithoutTimeoutCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "CacheFS:test   87687856128  87687856128            0 100% /runtime-mnt/cachefs/kube-system/cfsdemo/cachefs-fuse", "", nil
	}
	ExecWithoutTimeoutErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.execWithoutTimeout)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := &CacheFileUtils{log: fake.NullLogger()}
	_, err = a.GetUsedSpace("/tmp")
	if err == nil {
		t.Error("check failure, want err, got nil")
	}
	wrappedUnhookExec()

	err = gohook.Hook(CacheFileUtils.execWithoutTimeout, ExecWithoutTimeoutCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	usedSpace, err := a.GetUsedSpace("/tmp")
	if err != nil {
		t.Errorf("check failure, want nil, got err: %v", err)
	}
	if usedSpace != 87687856128 {
		t.Errorf("check failure, want 87687856128, got %d", usedSpace)
	}
	wrappedUnhookExec()
}

func TestCacheFSFileUtils_QueryMetaDataInfoIntoFile(t *testing.T) {
	ExecCommon := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "CacheFS  cluster summary", "", nil
	}
	ExecErr := func(a CacheFileUtils, command []string) (stdout string, stderr string, err error) {
		return "", "", errors.New("fail to run the command")
	}
	wrappedUnhookExec := func() {
		err := gohook.UnHook(CacheFileUtils.exec)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	err := gohook.Hook(CacheFileUtils.exec, ExecErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := CacheFileUtils{log: fake.NullLogger()}

	keySets := []KeyOfMetaDataFile{DatasetName, Namespace, UfsTotal, FileNum, ""}
	for index, keySet := range keySets {
		_, err = a.QueryMetaDataInfoIntoFile(keySet, "/tmp/file")
		if err == nil {
			t.Errorf("%d check failure, want err, got nil", index)
			return
		}
	}
	wrappedUnhookExec()

	err = gohook.Hook(CacheFileUtils.exec, ExecCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	for index, keySet := range keySets {
		_, err = a.QueryMetaDataInfoIntoFile(keySet, "/tmp/file")
		if err != nil {
			t.Errorf("%d check failure, want nil, got err: %v", index, err)
			return
		}
	}
	wrappedUnhookExec()
}

func TestValidDir(t *testing.T) {
	type args struct {
		dir string
	}
	tests := []struct {
		name      string
		args      args
		wantMatch bool
	}{
		{
			name: "test-normal",
			args: args{
				dir: "/tmp/raw/chunks",
			},
			wantMatch: true,
		},
		{
			name: "test1",
			args: args{
				dir: "/t mp/raw/chunks",
			},
			wantMatch: true,
		},
		{
			name: "test2",
			args: args{
				dir: "/t..mp/raw/chunks",
			},
			wantMatch: true,
		},
		{
			name: "test3",
			args: args{
				dir: "/t__mp/raw/chunks",
			},
			wantMatch: true,
		},
		{
			name: "test4",
			args: args{
				dir: "/t--mp/raw/chunks",
			},
			wantMatch: true,
		},
		{
			name: "test5",
			args: args{
				dir: "/",
			},
			wantMatch: false,
		},
		{
			name: "test6",
			args: args{
				dir: ".",
			},
			wantMatch: false,
		},
		{
			name: "test7",
			args: args{
				dir: "/tttt/raw/chunks",
			},
			wantMatch: true,
		},
		{
			name: "test8",
			args: args{
				dir: "//",
			},
			wantMatch: false,
		},
		{
			name: "test9",
			args: args{
				dir: "/0/raw/chunks",
			},
			wantMatch: true,
		},
		{
			name: "test10",
			args: args{
				dir: "/0/1/raw/chunks",
			},
			wantMatch: true,
		},
		{
			name: "test11",
			args: args{
				dir: "/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z/0/raw/chunks",
			},
			wantMatch: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotMatch := ValidCacheDir(tt.args.dir); gotMatch != tt.wantMatch {
				t.Errorf("ValidDir() = %v, want %v", gotMatch, tt.wantMatch)
			}
		})
	}
}
