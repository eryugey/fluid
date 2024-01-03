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
	"fmt"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
)

func TestCacheFSEngine_getDaemonset(t *testing.T) {
	type fields struct {
		runtime   *datav1alpha1.CacheFSRuntime
		name      string
		namespace string
		Client    client.Client
	}
	tests := []struct {
		name          string
		fields        fields
		wantDaemonset *appsv1.DaemonSet
		wantErr       bool
	}{
		{
			name: "test",
			fields: fields{
				runtime: &datav1alpha1.CacheFSRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "runtime1",
						Namespace: "default",
					},
				},
				name:      "runtime1",
				namespace: "default",
			},
			wantDaemonset: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "runtime1",
					Namespace: "default",
				},
				TypeMeta: metav1.TypeMeta{
					Kind:       "DaemonSet",
					APIVersion: "apps/v1",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			s.AddKnownTypes(datav1alpha1.GroupVersion, tt.fields.runtime)
			s.AddKnownTypes(appsv1.SchemeGroupVersion, &appsv1.DaemonSet{})
			_ = corev1.AddToScheme(s)
			mockClient := fake.NewFakeClientWithScheme(s, tt.fields.runtime, tt.wantDaemonset)
			e := &CacheFSEngine{
				runtime:   tt.fields.runtime,
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
				Client:    mockClient,
			}
			gotDaemonset, err := e.getDaemonset(tt.fields.name, tt.fields.namespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("CachefsEngine.getDaemonset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDaemonset, tt.wantDaemonset) {
				t.Errorf("CacheFSEngine.getDaemonset() = %#v, want %#v", gotDaemonset, tt.wantDaemonset)
			}
		})
	}
}

func TestCacheFSEngine_getFuseDaemonsetName(t *testing.T) {
	type fields struct {
		name string
	}
	tests := []struct {
		name       string
		fields     fields
		wantDsName string
	}{
		{
			name: "test",
			fields: fields{
				name: "cachefs",
			},
			wantDsName: "cachefs-fuse",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &CacheFSEngine{
				name: tt.fields.name,
			}
			if gotDsName := e.getFuseDaemonsetName(); gotDsName != tt.wantDsName {
				t.Errorf("CacheFSEngine.getFuseDaemonsetName() = %v, want %v", gotDsName, tt.wantDsName)
			}
		})
	}
}

func TestCacheFSEngine_getWorkerName(t *testing.T) {
	e := CacheFSEngine{
		name: "cachefs",
	}
	wantName := "cachefs-worker"
	workerName := e.getWorkerName()
	if workerName != wantName {
		t.Errorf("CacheFSEngine.getWorkerName() = %s, want %s", workerName, wantName)
	}
}

func TestCacheFSEngine_getMasterName(t *testing.T) {
	e := CacheFSEngine{
		name: "cachefs",
	}
	wantName := "cachefs-master"
	masterName := e.getMasterName()
	if masterName != wantName {
		t.Errorf("CacheFSEngine.getMasterName() = %s, want %s", masterName, wantName)
	}
}

func TestCacheFSEngine_getMountPoint(t *testing.T) {
	type fields struct {
		name      string
		namespace string
		Log       logr.Logger
		MountRoot string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "test",
			fields: fields{
				name:      "cachefs",
				namespace: "default",
				Log:       fake.NullLogger(),
				MountRoot: "/tmp",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &CacheFSEngine{
				Log:       tt.fields.Log,
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
			}
			t.Setenv("MOUNT_ROOT", tt.fields.MountRoot)
			wantMountPath := fmt.Sprintf("%s/%s/%s/cachefs-fuse", tt.fields.MountRoot+"/cachefs", tt.fields.namespace, e.name)
			if gotMountPath := e.getMountPoint(); gotMountPath != wantMountPath {
				t.Errorf("CacheFSEngine.getMountPoint() = %v, want %v", gotMountPath, wantMountPath)
			}
		})
	}
}

func TestCacheFSEngine_getHostMountPoint(t *testing.T) {
	type fields struct {
		name      string
		namespace string
		Log       logr.Logger
		MountRoot string
	}
	var tests = []struct {
		name          string
		fields        fields
		wantMountPath string
	}{
		{
			name: "test",
			fields: fields{
				name:      "cachefs",
				namespace: "default",
				Log:       fake.NullLogger(),
				MountRoot: "/tmp",
			},
			wantMountPath: "/tmp/cachefs/default/cachefs",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CacheFSEngine{
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
				Log:       tt.fields.Log,
			}
			t.Setenv("MOUNT_ROOT", tt.fields.MountRoot)
			if gotMountPath := c.getHostMountPoint(); gotMountPath != tt.wantMountPath {
				t.Errorf("getHostMountPoint() = %v, want %v", gotMountPath, tt.wantMountPath)
			}
		})
	}
}

func TestCacheFSEngine_getRuntime(t *testing.T) {
	type fields struct {
		runtime   *datav1alpha1.CacheFSRuntime
		name      string
		namespace string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *datav1alpha1.CacheFSRuntime
		wantErr bool
	}{
		{
			name: "test",
			fields: fields{
				runtime: &datav1alpha1.CacheFSRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cachefs",
						Namespace: "default",
					},
				},
				name:      "cachefs",
				namespace: "default",
			},
			want: &datav1alpha1.CacheFSRuntime{
				TypeMeta: metav1.TypeMeta{
					Kind:       "CacheFSRuntime",
					APIVersion: "data.fluid.io/v1alpha1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cachefs",
					Namespace: "default",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			s.AddKnownTypes(datav1alpha1.GroupVersion, tt.fields.runtime)
			_ = corev1.AddToScheme(s)
			mockClient := fake.NewFakeClientWithScheme(s, tt.want)
			e := &CacheFSEngine{
				runtime:   tt.fields.runtime,
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
				Client:    mockClient,
			}
			got, err := e.getRuntime()
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheFSEngine.getRuntime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CacheFSEngine.getRuntime() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestCacheFSEngine_parseCacheFSImage(t *testing.T) {
	type args struct {
		master          bool
		image           string
		tag             string
		imagePullPolicy string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   string
		want2   string
		wantErr bool
	}{
		{
			name: "test0",
			args: args{
				master:          false,
				image:           "cachefs",
				tag:             "1.0.4-1.5",
				imagePullPolicy: "IfNotPresent",
			},
			want:    "cachefs",
			want1:   "1.0.4-1.5",
			want2:   "IfNotPresent",
			wantErr: false,
		},
		{
			name: "test1",
			args: args{
				master:          false,
				image:           "",
				tag:             "",
				imagePullPolicy: "Always",
			},
			want:    "cachefs",
			want1:   "1.0.5-1.6",
			want2:   "Always",
			wantErr: false,
		},
		{
			name: "for-master-image",
			args: args{
				master:          true,
				image:           "",
				tag:             "",
				imagePullPolicy: "Always",
			},
			want:    "redis",
			want1:   "7.2-alpine",
			want2:   "Always",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &CacheFSEngine{}
			t.Setenv(common.CacheFSImageEnv, "cachefs:1.0.5-1.6")
			got, got1, got2, err := e.parseCacheFSImage(tt.args.master, tt.args.image, tt.args.tag, tt.args.imagePullPolicy)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCacheFSImage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CacheFSEngine.parseCacheFSImage() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("CacheFSEngine.parseCacheFSImage() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("CacheFSEngine.parseCacheFSImage() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_getMountRoot(t *testing.T) {
	tests := []struct {
		name     string
		wantPath string
	}{
		{
			name:     "test",
			wantPath: "/tmp/cachefs",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("MOUNT_ROOT", "/tmp")
			if gotPath := getMountRoot(); gotPath != tt.wantPath {
				t.Errorf("getMountRoot() = \"%v\", want \"%v\"", gotPath, tt.wantPath)
			}
		})
	}
}

func Test_parseInt64Size(t *testing.T) {
	type args struct {
		sizeStr string
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				sizeStr: "10",
			},
			want:    10,
			wantErr: false,
		},
		{
			name: "test2",
			args: args{
				sizeStr: "v",
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInt64Size(tt.args.sizeStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseInt64Size() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseInt64Size() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseSubPathFromMountPoint(t *testing.T) {
	type args struct {
		mountPoint string
	}
	tests := []struct {
		name    string
		args    args
		want1   string
		want2   string
		want3   string
		wantErr bool
	}{
		{
			name: "cachefs-correct",
			args: args{
				mountPoint: "cachefs:///abc",
			},
			want1:   "cachefs",
			want2:   "",
			want3:   "/abc",
			wantErr: false,
		},
		{
			name: "pvc",
			args: args{
				mountPoint: "pvc://abc",
			},
			want1:   "pvc",
			want2:   "abc",
			want3:   "",
			wantErr: false,
		},
		{
			name: "pvc-subpath",
			args: args{
				mountPoint: "pvc://abc/subpath",
			},
			want1:   "pvc",
			want2:   "abc",
			want3:   "/subpath",
			wantErr: false,
		},
		{
			name: "local-correct",
			args: args{
				mountPoint: "local:///abc",
			},
			want1:   "local",
			want2:   "",
			want3:   "/abc",
			wantErr: false,
		},
		{
			name: "fix-subpath",
			args: args{
				mountPoint: "local://abc",
			},
			want1:   "local",
			want2:   "",
			want3:   "/abc",
			wantErr: false,
		},
		{
			name: "wrong-type",
			args: args{
				mountPoint: "wrongscheme:///abc",
			},
			want1:   "",
			want2:   "",
			want3:   "",
			wantErr: true,
		},
		{
			name: "no-scheme",
			args: args{
				mountPoint: "/abc",
			},
			want1:   "",
			want2:   "",
			want3:   "",
			wantErr: true,
		},
		{
			name: "no-pvcname",
			args: args{
				mountPoint: "pvc://",
			},
			want1:   "",
			want2:   "",
			want3:   "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, got2, got3, err := ParseSubPathFromMountPoint(tt.args.mountPoint)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSubPathFromMountPoint() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got1 != tt.want1 {
				t.Errorf("ParseSubPathFromMountPoint() got1 = %v, want1 %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("ParseSubPathFromMountPoint() got2 = %v, want2 %v", got2, tt.want2)
			}
			if got3 != tt.want3 {
				t.Errorf("ParseSubPathFromMountPoint() got3 = %v, want3 %v", got3, tt.want3)
			}
		})
	}
}

func TestCacheFSEngine_GetRunningPodsOfStatefulSet(t *testing.T) {
	type args struct {
		stsName   string
		namespace string
	}
	tests := []struct {
		name     string
		args     args
		sts      *appsv1.StatefulSet
		podLists *corev1.PodList
		wantPods []corev1.Pod
		wantErr  bool
	}{
		{
			name: "test1",
			args: args{
				stsName:   "test1",
				namespace: "fluid",
			},
			sts: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test1",
					Namespace: "fluid",
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"sts": "test1"},
					},
				},
			},
			podLists: &corev1.PodList{
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test1-pod",
							Namespace: "fluid",
							Labels:    map[string]string{"sts": "test1"},
						},
						Status: corev1.PodStatus{
							Phase: corev1.PodRunning,
							Conditions: []corev1.PodCondition{{
								Type:   corev1.PodReady,
								Status: corev1.ConditionTrue,
							}},
						},
					},
				},
			},
			wantPods: []corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test1-pod",
						Namespace: "fluid",
						Labels:    map[string]string{"sts": "test1"},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
						Conditions: []corev1.PodCondition{{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						}},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "test2",
			args: args{
				stsName:   "app",
				namespace: "fluid",
			},
			sts:      &appsv1.StatefulSet{},
			podLists: &corev1.PodList{},
			wantPods: []corev1.Pod{},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			s.AddKnownTypes(appsv1.SchemeGroupVersion, &appsv1.StatefulSet{})
			s.AddKnownTypes(corev1.SchemeGroupVersion, &corev1.PodList{})
			_ = corev1.AddToScheme(s)
			mockClient := fake.NewFakeClientWithScheme(s, tt.sts.DeepCopy(), tt.podLists.DeepCopy())
			c := &CacheFSEngine{
				Client: mockClient,
			}
			gotPods, err := c.GetRunningPodsOfStatefulSet(tt.args.stsName, tt.args.namespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRunningPodsOfStatefulSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !reflect.DeepEqual(gotPods[0].Status, tt.wantPods[0].Status) {
					t.Errorf("testcase %s GetRunningPodsOfStatefulSet() gotPods = %v, want %v", tt.name, gotPods, tt.wantPods)
				}
			}
		})
	}
}

func TestCacheFSEngine_getValuesConfigMap(t *testing.T) {
	type fields struct {
		runtime     *datav1alpha1.CacheFSRuntime
		runtimeType string
		engineImpl  string
		name        string
		namespace   string
		Client      client.Client
	}
	tests := []struct {
		name    string
		fields  fields
		wantCm  *corev1.ConfigMap
		wantErr bool
	}{
		{
			name: "test",
			fields: fields{
				runtime: &datav1alpha1.CacheFSRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "runtime",
						Namespace: "default",
					},
				},
				name:        "test",
				namespace:   "default",
				runtimeType: "cachefs",
				engineImpl:  "cachefs",
			},
			wantCm: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cachefs-values",
					Namespace: "default",
				},
				TypeMeta: metav1.TypeMeta{
					Kind:       "ConfigMap",
					APIVersion: "v1",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			s.AddKnownTypes(datav1alpha1.GroupVersion, tt.fields.runtime)
			s.AddKnownTypes(corev1.SchemeGroupVersion, &corev1.ConfigMap{})
			_ = corev1.AddToScheme(s)
			mockClient := fake.NewFakeClientWithScheme(s, tt.fields.runtime, tt.wantCm)
			e := &CacheFSEngine{
				runtime:     tt.fields.runtime,
				runtimeType: tt.fields.runtimeType,
				engineImpl:  tt.fields.engineImpl,
				name:        tt.fields.name,
				namespace:   tt.fields.namespace,
				Client:      mockClient,
			}
			gotCm, err := e.GetValuesConfigMap()
			if (err != nil) != tt.wantErr {
				t.Errorf("CachefsEngine.getValuesConfigMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotCm, tt.wantCm) {
				t.Errorf("CacheFSEngine.getValuesConfigMap() = %#v, want %#v", gotCm, tt.wantCm)
			}
		})
	}
}

func TestGetMetricsPort(t *testing.T) {
	type args struct {
		options map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				options: map[string]string{
					"metrics": "0.0.0.0:9567",
				},
			},
			want:    9567,
			wantErr: false,
		},
		{
			name: "test-default",
			args: args{
				options: map[string]string{},
			},
			want:    9567,
			wantErr: false,
		},
		{
			name: "test-wrong1",
			args: args{
				options: map[string]string{
					"metrics": "0.0.0.0:test",
				},
			},
			want:    9567,
			wantErr: true,
		},
		{
			name: "test-wrong2",
			args: args{
				options: map[string]string{
					"metrics": "0.0.0.0",
				},
			},
			want:    9567,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetMetricsPort(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMetricsPort() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetMetricsPort() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseVersion(t *testing.T) {
	type args struct {
		version string
	}
	var tests = []struct {
		name    string
		args    args
		want    *ClientVersion
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				version: "v1.0.0",
			},
			want: &ClientVersion{
				Major: 1,
				Minor: 0,
				Patch: 0,
				Tag:   "",
			},
			wantErr: false,
		},
		{
			name: "test2",
			args: args{
				version: "4.9.0",
			},
			want: &ClientVersion{
				Major: 4,
				Minor: 9,
				Patch: 0,
				Tag:   "",
			},
			wantErr: false,
		},
		{
			name: "test3",
			args: args{
				version: "1.0.0-rc1",
			},
			want: &ClientVersion{
				Major: 1,
				Minor: 0,
				Patch: 0,
				Tag:   "rc1",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseVersion(tt.args.version)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseVersion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCacheFSEngine_getWorkerCommand(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker-script",
			Namespace: "fluid",
		},
		Data: map[string]string{
			"script.sh": `#!/bin/bash

echo "$(date '+%Y/%m/%d %H:%M:%S').$(printf "%03d" $(($(date '+%N')/1000))) cachefs format start."
/bin/cachefs format --access-key=${ACCESS_KEY} --secret-key=${SECRET_KEY} --bucket=http://test4.minio.default.svc.cluster.local:9000 test-fluid-2

echo "$(date '+%Y/%m/%d %H:%M:%S').$(printf "%03d" $(($(date '+%N')/1000))) cachefs mount start."
/bin/mount.cachefs test-fluid-2 /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse -o subdir=/demo,cache-size=2048,free-space-ratio=0.1,cache-dir=/dev/shm,cache-group=default-cfsdemo
`,
		},
	}
	testObjs := []runtime.Object{}
	testObjs = append(testObjs, cm)

	fakeClient := fake.NewFakeClientWithScheme(testScheme, testObjs...)
	tests := []struct {
		name        string
		runtimeName string
		wantCommand string
		wantErr     bool
	}{
		{
			name:        "test-normal",
			runtimeName: "test",
			wantCommand: "/bin/mount.cachefs test-fluid-2 /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse -o subdir=/demo,cache-size=2048,free-space-ratio=0.1,cache-dir=/dev/shm,cache-group=default-cfsdemo",
			wantErr:     false,
		},
		{
			name:        "test-not-found",
			runtimeName: "test1",
			wantCommand: "",
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheFSEngine{
				name:      tt.runtimeName,
				namespace: "fluid",
				Client:    fakeClient,
				Log:       fake.NullLogger(),
			}
			gotCommand, err := c.getWorkerCommand()
			if (err != nil) != tt.wantErr {
				t.Errorf("getWorkerCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotCommand != tt.wantCommand {
				t.Errorf("getWorkerCommand() gotCommand = %v, want %v", gotCommand, tt.wantCommand)
			}
		})
	}
}

func TestCacheFSEngine_updateWorkerScript(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker-script",
			Namespace: "fluid",
		},
		Data: map[string]string{
			"script.sh": `#!/bin/bash

echo "$(date '+%Y/%m/%d %H:%M:%S').$(printf "%03d" $(($(date '+%N')/1000))) cachefs format start."
/bin/cachefs format --access-key=${ACCESS_KEY} --secret-key=${SECRET_KEY} --bucket=http://test4.minio.default.svc.cluster.local:9000 test-fluid-2

echo "$(date '+%Y/%m/%d %H:%M:%S').$(printf "%03d" $(($(date '+%N')/1000))) cachefs mount start."
/bin/mount.cachefs test-fluid-2 /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse -o subdir=/demo,cache-size=2048,free-space-ratio=0.1,cache-dir=/dev/shm,cache-group=default-cfsdemo
`,
		},
	}
	testObjs := []runtime.Object{}
	testObjs = append(testObjs, cm)

	fakeClient := fake.NewFakeClientWithScheme(testScheme, testObjs...)
	tests := []struct {
		name        string
		runtimeName string
		command     string
		wantCommand string
		wantErr     bool
	}{
		{
			name:        "test-normal",
			runtimeName: "test",
			command:     "echo abc",
			wantCommand: "echo abc",
			wantErr:     false,
		},
		{
			name:        "test-not-found",
			runtimeName: "test1",
			command:     "",
			wantCommand: "",
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheFSEngine{
				name:      tt.runtimeName,
				namespace: "fluid",
				Client:    fakeClient,
				Log:       fake.NullLogger(),
			}
			err := c.updateWorkerScript(tt.command)
			if (err != nil) != tt.wantErr {
				t.Errorf("updateWorkerScript() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotCommand, err := c.getWorkerCommand()
			if err != nil {
				t.Errorf("updateWorkerScript get configmap command error = %v", err)
				return
			}
			if gotCommand != tt.command {
				t.Errorf("updateWorkerScript() gotCommand = %v, want %v", gotCommand, tt.command)
			}
		})
	}
}

func Test_maxMemoryToQuantity(t *testing.T) {
	testcases := []struct {
		maxmemory string
		value     int64
		wantErr   bool
	}{
		{
			maxmemory: "10",
			value:     10,
			wantErr:   false,
		},
		{
			maxmemory: "10k",
			value:     10240,
			wantErr:   false,
		},
		{
			maxmemory: "10kb",
			value:     10240,
			wantErr:   false,
		},
		{
			maxmemory: "10KB",
			value:     10240,
			wantErr:   false,
		},
		{
			maxmemory: "10Kb",
			value:     10240,
			wantErr:   false,
		},
		{
			maxmemory: "10m",
			value:     10485760,
			wantErr:   false,
		},
		{
			maxmemory: "10mb",
			value:     10485760,
			wantErr:   false,
		},
		{
			maxmemory: "10MB",
			value:     10485760,
			wantErr:   false,
		},
		{
			maxmemory: "10Mb",
			value:     10485760,
			wantErr:   false,
		},
		{
			maxmemory: "10g",
			value:     10737418240,
			wantErr:   false,
		},
		{
			maxmemory: "10gb",
			value:     10737418240,
			wantErr:   false,
		},
		{
			maxmemory: "10GB",
			value:     10737418240,
			wantErr:   false,
		},
		{
			maxmemory: "10Gb",
			value:     10737418240,
			wantErr:   false,
		},
		{
			maxmemory: "10Gib",
			value:     -1,
			wantErr:   true,
		},
		{
			maxmemory: "10Mi",
			value:     -1,
			wantErr:   true,
		},
	}

	for _, tc := range testcases {
		result, err := maxMemoryToQuantity(tc.maxmemory)
		if tc.wantErr {
			if err != nil {
				continue
			}
			t.Fatalf("convert %s expect err but succeeded", tc.maxmemory)
		} else {
			if err != nil {
				t.Fatalf("convert %s failed unexpectedly: %s", tc.maxmemory, err)
			}
			if tc.value != result.Value() {
				t.Fatalf("convert %s, got %v, expect %v", tc.maxmemory, result.Value(), tc.value)
			}
		}
	}
}
