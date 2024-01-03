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
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/fluid-cloudnative/fluid/pkg/common"

	"github.com/brahma-adshonor/gohook"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	cdataload "github.com/fluid-cloudnative/fluid/pkg/dataload"
	cruntime "github.com/fluid-cloudnative/fluid/pkg/runtime"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	"github.com/fluid-cloudnative/fluid/pkg/utils/helm"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
)

var valuesConfigMapData = `
cacheDirs:
  "1":
    path: /cfs/cache3:/cfs/cache4
    type: hostPath
configs:
  accesskeySecret: cfs-secret
  accesskeySecretKey: accesskey
  bucket: http://minio.default.svc.cluster.local:9000/minio/test2
  formatCmd: /bin/cachefs format --trash-days=0 --access-key=${ACCESS_KEY}
    --secret-key=${SECRET_KEY} --storage=minio --bucket=http://minio.default.svc.cluster.local:9000/minio/test2
    ${METAURL} minio
  metaurlSecret: cfs-secret
  metaurlSecretKey: metaurl
  name: minio
  secretkeySecret: cfs-secret
  secretkeySecretKey: secretkey
  storage: minio
fsGroup: 0
fullnameOverride: cfsdemo
fuse:
  metricsPort: 14001
  command: /bin/mount.cachefs ${METAURL} /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse
    -o metrics=0.0.0.0:9567,cache-size=1024,free-space-ratio=0.1,cache-dir=/cfs/cache3:/cfs/cache4
  criticalPod: true
  enabled: true
  hostMountPath: /runtime-mnt/cachefs/default/cfsdemo
  hostNetwork: true
  image: kangaroo.registry.cn-hangzhou.cr.aliyuncs.com/storage/cachefs
  imagePullPolicy: IfNotPresent
  imageTag: 1.0.5-1.5
  mountPath: /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse
  nodeSelector:
    fluid.io/f-default-cfsdemo: "true"
  privileged: true
  resources: {}
  statCmd: stat -c %i /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse
  volumeMounts:
  - mountPath: /cfs/cache3:/cfs/cache4
    name: cache-dir-1
  volumes:
  - hostPath:
      path: /cfs/cache3:/cfs/cache4
      type: DirectoryOrCreate
    name: cache-dir-1
group: 0
image: kangaroo.registry.cn-hangzhou.cr.aliyuncs.com/storage/cachefs
imagePullPolicy: IfNotPresent
imagePullSecrets: null
imageTag: 1.0.5-1.5
owner:
  apiVersion: data.fluid.io/v1alpha1
  blockOwnerDeletion: false
  controller: true
  enabled: true
  kind: CacheFSRuntime
  name: cfsdemo
  uid: 9ae3312b-d5b6-4a3d-895c-7712bfa7d74e
placement: Exclusive
runtimeIdentity:
  name: cfsdemo
  namespace: default
source: ${METAURL}
user: 0
worker:
  metricsPort: 14000
  command: /bin/mount.cachefs ${METAURL} /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse
    -o cache-size=1024,free-space-ratio=0.1,cache-dir=/cfs/cache1:/cfs/cache2,metrics=0.0.0.0:9567
  hostNetwork: true
  mountPath: /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse
  privileged: true
  resources: {}
  statCmd: stat -c %i /runtime-mnt/cachefs/default/cfsdemo/cachefs-fuse
  volumeMounts:
  - mountPath: /cfs/cache1:/cfs/cache2
    name: cache-dir-1
  volumes:
  - hostPath:
      path: /cfs/cache1:/cfs/cache2
      type: DirectoryOrCreate
    name: cache-dir-1
`

func TestCacheFSEngine_CreateDataLoadJob(t *testing.T) {
	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataset-cachefs-values",
			Namespace: "fluid",
		},
		Data: map[string]string{
			"data": valuesConfigMapData,
		},
	}

	mockExecCheckReleaseCommon := func(name string, namespace string) (exist bool, err error) {
		return false, nil
	}
	mockExecCheckReleaseErr := func(name string, namespace string) (exist bool, err error) {
		return false, errors.New("fail to check release")
	}
	mockExecInstallReleaseCommon := func(name string, namespace string, valueFile string, chartName string) error {
		return nil
	}
	mockExecInstallReleaseErr := func(name string, namespace string, valueFile string, chartName string) error {
		return errors.New("fail to install dataload chart")
	}

	wrappedUnhookCheckRelease := func() {
		err := gohook.UnHook(helm.CheckRelease)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
	wrappedUnhookInstallRelease := func() {
		err := gohook.UnHook(helm.InstallRelease)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	targetDataLoad := datav1alpha1.DataLoad{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hbase",
			Namespace: "fluid",
		},
		Spec: datav1alpha1.DataLoadSpec{
			Dataset: datav1alpha1.TargetDataset{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
		},
	}
	datasetInputs := []datav1alpha1.Dataset{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
		},
	}
	statefulsetInputs := []appsv1.StatefulSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cachefs-worker",
				Namespace: "fluid",
			},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
	}
	podListInputs := []v1.PodList{{
		Items: []v1.Pod{{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{"a": "b"},
			},
		}},
	}}
	testObjs := []runtime.Object{}
	testObjs = append(testObjs, configMap)
	for _, datasetInput := range datasetInputs {
		testObjs = append(testObjs, datasetInput.DeepCopy())
	}
	for _, statefulsetInput := range statefulsetInputs {
		testObjs = append(testObjs, statefulsetInput.DeepCopy())
	}
	for _, podInput := range podListInputs {
		testObjs = append(testObjs, podInput.DeepCopy())
	}
	testScheme.AddKnownTypes(v1.SchemeGroupVersion, configMap)
	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)
	engine := &CacheFSEngine{
		name:      "cachefs",
		namespace: "fluid",
		Client:    client,
		Log:       fake.NullLogger(),
	}
	ctx := cruntime.ReconcileRequestContext{
		Log:      fake.NullLogger(),
		Client:   client,
		Recorder: record.NewFakeRecorder(1),
	}

	err := gohook.Hook(helm.CheckRelease, mockExecCheckReleaseErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = engine.CreateDataLoadJob(ctx, targetDataLoad)
	if err == nil {
		t.Errorf("fail to catch the error: %v", err)
	}
	wrappedUnhookCheckRelease()

	err = gohook.Hook(helm.CheckRelease, mockExecCheckReleaseCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = gohook.Hook(helm.InstallRelease, mockExecInstallReleaseErr, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = engine.CreateDataLoadJob(ctx, targetDataLoad)
	if err == nil {
		t.Errorf("fail to catch the error: %v", err)
	}
	wrappedUnhookInstallRelease()

	err = gohook.Hook(helm.InstallRelease, mockExecInstallReleaseCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	err = engine.CreateDataLoadJob(ctx, targetDataLoad)
	if err != nil {
		t.Errorf("fail to exec the function: %v", err)
	}
	wrappedUnhookCheckRelease()
}

func TestCacheFSEngine_GenerateDataLoadValueFileWithRuntimeHDD(t *testing.T) {
	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataset-cachefs-values",
			Namespace: "fluid",
		},
		Data: map[string]string{
			"data": valuesConfigMapData,
		},
	}

	datasetInputs := []datav1alpha1.Dataset{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
			Spec: datav1alpha1.DatasetSpec{},
		},
	}

	statefulsetInputs := []appsv1.StatefulSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cachefs-worker",
				Namespace: "fluid",
			},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
	}
	podListInputs := []v1.PodList{{
		Items: []v1.Pod{{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "fluid",
				Labels:    map[string]string{"a": "b"},
			},
			Status: v1.PodStatus{
				Phase: v1.PodRunning,
				Conditions: []v1.PodCondition{{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				}},
			},
		}},
	}}
	testObjs := []runtime.Object{}
	testObjs = append(testObjs, configMap)
	for _, datasetInput := range datasetInputs {
		testObjs = append(testObjs, datasetInput.DeepCopy())
	}
	for _, statefulsetInput := range statefulsetInputs {
		testObjs = append(testObjs, statefulsetInput.DeepCopy())
	}
	for _, podInput := range podListInputs {
		testObjs = append(testObjs, podInput.DeepCopy())
	}
	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)

	context := cruntime.ReconcileRequestContext{
		Client: client,
	}

	dataLoadNoTarget := datav1alpha1.DataLoad{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataload",
			Namespace: "fluid",
		},
		Spec: datav1alpha1.DataLoadSpec{
			Dataset: datav1alpha1.TargetDataset{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
		},
	}
	dataLoadWithTarget := datav1alpha1.DataLoad{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataload",
			Namespace: "fluid",
		},
		Spec: datav1alpha1.DataLoadSpec{
			Dataset: datav1alpha1.TargetDataset{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
			Target: []datav1alpha1.TargetPath{
				{
					Path: "/test",
				},
			},
		},
	}

	testCases := []struct {
		dataLoad       datav1alpha1.DataLoad
		expectFileName string
	}{
		{
			dataLoad:       dataLoadNoTarget,
			expectFileName: filepath.Join(os.TempDir(), "fluid-test-dataload-loader-values.yaml"),
		},
		{
			dataLoad:       dataLoadWithTarget,
			expectFileName: filepath.Join(os.TempDir(), "fluid-test-dataload-loader-values.yaml"),
		},
	}

	for _, test := range testCases {
		engine := CacheFSEngine{
			name:      "cachefs",
			namespace: "fluid",
			Client:    client,
			Log:       fake.NullLogger(),
		}
		if fileName, err := engine.generateDataLoadValueFile(context, &test.dataLoad); err != nil || !strings.Contains(fileName, test.expectFileName) {
			t.Errorf("fail to generate the dataload value file: %v", err)
		}
	}
}

func TestCacheFSEngine_GenerateDataLoadValueFileWithRuntime(t *testing.T) {
	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataset-cachefs-values",
			Namespace: "fluid",
		},
		Data: map[string]string{
			"data": ``,
		},
	}

	datasetInputs := []datav1alpha1.Dataset{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
			Spec: datav1alpha1.DatasetSpec{},
		},
	}

	statefulsetInputs := []appsv1.StatefulSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cachefs-worker",
				Namespace: "fluid",
			},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
	}
	podListInputs := []v1.PodList{{
		Items: []v1.Pod{{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "fluid",
				Labels:    map[string]string{"a": "b"},
			},
			Status: v1.PodStatus{
				Phase: v1.PodRunning,
				Conditions: []v1.PodCondition{{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				}},
			},
		}},
	}}
	testObjs := []runtime.Object{}
	testObjs = append(testObjs, configMap)
	for _, datasetInput := range datasetInputs {
		testObjs = append(testObjs, datasetInput.DeepCopy())
	}
	for _, statefulsetInput := range statefulsetInputs {
		testObjs = append(testObjs, statefulsetInput.DeepCopy())
	}
	for _, podInput := range podListInputs {
		testObjs = append(testObjs, podInput.DeepCopy())
	}
	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)

	context := cruntime.ReconcileRequestContext{
		Client: client,
	}

	dataLoadNoTarget := datav1alpha1.DataLoad{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataload",
			Namespace: "fluid",
		},
		Spec: datav1alpha1.DataLoadSpec{
			Dataset: datav1alpha1.TargetDataset{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
			Target: []datav1alpha1.TargetPath{{
				Path:     "/dir",
				Replicas: 1,
			}},
		},
	}
	dataLoadWithTarget := datav1alpha1.DataLoad{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataload",
			Namespace: "fluid",
		},
		Spec: datav1alpha1.DataLoadSpec{
			Dataset: datav1alpha1.TargetDataset{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
			Target: []datav1alpha1.TargetPath{
				{
					Path: "/test",
				},
			},
		},
	}

	testCases := []struct {
		dataLoad       datav1alpha1.DataLoad
		expectFileName string
	}{
		{
			dataLoad:       dataLoadNoTarget,
			expectFileName: filepath.Join(os.TempDir(), "fluid-test-dataload-loader-values.yaml"),
		},
		{
			dataLoad:       dataLoadWithTarget,
			expectFileName: filepath.Join(os.TempDir(), "fluid-test-dataload-loader-values.yaml"),
		},
	}

	for _, test := range testCases {
		engine := CacheFSEngine{
			name:      "cachefs",
			namespace: "fluid",
			Client:    client,
			Log:       fake.NullLogger(),
		}
		if fileName, err := engine.generateDataLoadValueFile(context, &test.dataLoad); err != nil || !strings.Contains(fileName, test.expectFileName) {
			t.Errorf("fail to generate the dataload value file: %v", err)
		}
	}
}

func TestCacheFSEngine_CheckExistenceOfPath(t *testing.T) {
	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-dataset-cachefs-values",
			Namespace: "fluid",
		},
		Data: map[string]string{
			"data": valuesConfigMapData,
		},
	}

	datasetInputs := []datav1alpha1.Dataset{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
			Spec: datav1alpha1.DatasetSpec{},
		},
	}

	statefulsetInputs := []appsv1.StatefulSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "cachefs-worker",
				Namespace: "fluid",
			},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"a": "b"},
				},
			},
		},
	}
	podListInputs := []v1.PodList{{
		Items: []v1.Pod{{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "fluid",
				Labels:    map[string]string{"a": "b"},
			},
			Status: v1.PodStatus{
				Phase: v1.PodRunning,
				Conditions: []v1.PodCondition{{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				}},
			},
		}},
	}}
	testObjs := []runtime.Object{}
	testObjs = append(testObjs, configMap)
	for _, datasetInput := range datasetInputs {
		testObjs = append(testObjs, datasetInput.DeepCopy())
	}
	for _, statefulsetInput := range statefulsetInputs {
		testObjs = append(testObjs, statefulsetInput.DeepCopy())
	}
	for _, podInput := range podListInputs {
		testObjs = append(testObjs, podInput.DeepCopy())
	}
	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)

	mockExecNotExist := func(podName string, containerName string, namespace string, cmd []string) (stdout string, stderr string, e error) {
		return "does not exist", "", errors.New("other error")
	}
	mockExec := func(podName string, containerName string, namespace string, cmd []string) (stdout string, stderr string, e error) {
		return "", "", nil
	}
	wrappedUnhook := func() {
		err := gohook.UnHook(kubeclient.ExecCommandInContainer)
		if err != nil {
			t.Fatal(err.Error())
		}
	}

	engine := CacheFSEngine{
		namespace: "fluid",
		Log:       fake.NullLogger(),
		name:      "cachefs",
		Client:    client,
	}

	err := gohook.Hook(kubeclient.ExecCommandInContainer, mockExecNotExist, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	targetDataload := datav1alpha1.DataLoad{
		Spec: datav1alpha1.DataLoadSpec{
			Dataset: datav1alpha1.TargetDataset{
				Name:      "test-dataset",
				Namespace: "fluid",
			},
			Target: []datav1alpha1.TargetPath{
				{
					Path:     "/tmp",
					Replicas: 1,
				},
			},
		},
	}
	notExist, err := engine.CheckExistenceOfPath(targetDataload)
	if !(err != nil && notExist == true) {
		t.Errorf("fail to exec the function")
	}
	err = gohook.Hook(kubeclient.ExecCommandInContainer, mockExec, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	notExist, err = engine.CheckExistenceOfPath(targetDataload)
	if !(err == nil && notExist == false) {
		t.Errorf("fail to exec the function: notExist %v, err %s", notExist, err)
	}
	wrappedUnhook()
}

func TestCacheFSEngine_CheckRuntimeReady(t *testing.T) {
	type fields struct {
		name      string
		namespace string
	}
	tests := []struct {
		name      string
		fields    fields
		sts       appsv1.StatefulSet
		podList   v1.PodList
		wantReady bool
	}{
		{
			name: "test",
			fields: fields{
				name:      "cachefs-test",
				namespace: "fluid",
			},
			sts: appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cachefs-test-worker",
					Namespace: "fluid",
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
				},
			},
			podList: v1.PodList{
				Items: []v1.Pod{{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "fluid",
						Labels:    map[string]string{"a": "b"},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{{
							Type:   v1.PodReady,
							Status: v1.ConditionTrue,
						}},
					},
				}},
			},
			wantReady: true,
		},
		{
			name: "test-err",
			fields: fields{
				name:      "cachefs",
				namespace: "fluid",
			},
			sts: appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cachefs-worker",
					Namespace: "fluid",
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"a": "b"},
					},
				},
			},
			podList: v1.PodList{
				Items: []v1.Pod{{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "fluid",
						Labels:    map[string]string{"a": "b"},
					},
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						Conditions: []v1.PodCondition{{
							Type:   v1.PodReady,
							Status: v1.ConditionFalse,
						}},
					},
				}},
			},
			wantReady: false,
		},
	}
	for _, tt := range tests {
		testObjs := []runtime.Object{}
		t.Run(tt.name, func(t *testing.T) {
			testObjs = append(testObjs, tt.sts.DeepCopy())
			testObjs = append(testObjs, tt.podList.DeepCopy())
			client := fake.NewFakeClientWithScheme(testScheme, testObjs...)
			e := &CacheFSEngine{
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
				Client:    client,
				Log:       fake.NullLogger(),
			}
			if gotReady := e.CheckRuntimeReady(); gotReady != tt.wantReady {
				t.Errorf("CheckRuntimeReady() = %v, want %v", gotReady, tt.wantReady)
			}
		})
	}
}

func TestCacheFSEngine_genDataLoadValue(t *testing.T) {
	testCases := map[string]struct {
		image         string
		runtimeName   string
		targetDataset *datav1alpha1.Dataset
		dataload      *datav1alpha1.DataLoad
		value         *CacheFS
		cacheInfo     map[string]string
		pods          []v1.Pod
		want          *cdataload.DataLoadValue
	}{
		"test case with scheduler name": {
			image:       "fluid:v0.0.1",
			runtimeName: "cachefs",
			targetDataset: &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataset",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{
						{
							Name:       "spark",
							MountPoint: "local://mnt/data0",
							Path:       "/mnt",
						},
					},
				},
			},
			dataload: &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataload",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DataLoadSpec{
					Dataset: datav1alpha1.TargetDataset{
						Name:      "test-dataset",
						Namespace: "fluid",
					},
					Target: []datav1alpha1.TargetPath{
						{
							Path:     "/test",
							Replicas: 1,
						},
					},
					SchedulerName: "scheduler-test",
					Options: map[string]string{
						"dl-opts-k-1": "dl-opts-v-1",
					},
				},
			},
			value: &CacheFS{
				ImportSource: "/datasource",
				Source:       "${METAURL}",
				Configs: Configs{
					MetaUrlSecret:    "metaurlsecret",
					MetaUrlSecretKey: "metaurlsecretkey",
				},
			},
			pods: []v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pods-1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pods-2",
					},
				},
			},
			cacheInfo: map[string]string{
				"cache-info-k1": "cache-info-v1",
				"cache-info-k2": "cache-info-v2",
				"cache-info-k3": "cache-info-v3",
			},
			want: &cdataload.DataLoadValue{
				Name: "test-dataload",
				Owner: &common.OwnerReference{
					APIVersion:         "/",
					Enabled:            true,
					Name:               "test-dataload",
					BlockOwnerDeletion: false,
					Controller:         true,
				},
				DataLoadInfo: cdataload.DataLoadInfo{
					BackoffLimit:  3,
					Image:         "fluid:v0.0.1",
					TargetDataset: "test-dataset",
					SchedulerName: "scheduler-test",
					TargetPaths: []cdataload.TargetPath{
						{
							Path:     "/test",
							Replicas: 1,
						},
					},
					ImagePullSecrets: []v1.LocalObjectReference{},
					Options: map[string]string{
						// dataload spec options
						"dl-opts-k-1": "dl-opts-v-1",
						// cache info
						"cache-info-k1":    "cache-info-v1",
						"cache-info-k2":    "cache-info-v2",
						"cache-info-k3":    "cache-info-v3",
						"podNames":         "pods-1:pods-2",
						"runtimeName":      "cachefs",
						"timeout":          DefaultDataLoadTimeout,
						"importSource":     "/datasource",
						"metaurlSecret":    "metaurlsecret",
						"metaurlSecretKey": "metaurlsecretkey",
					},
				},
			},
		},
		"test case with affinity": {
			image:       "fluid:v0.0.1",
			runtimeName: "cachefs",
			targetDataset: &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataset",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{
						{
							Name:       "spark",
							MountPoint: "local://mnt/data0",
							Path:       "/mnt",
						},
					},
				},
			},
			dataload: &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataload",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DataLoadSpec{
					Dataset: datav1alpha1.TargetDataset{
						Name:      "test-dataset",
						Namespace: "fluid",
					},
					Target: []datav1alpha1.TargetPath{
						{
							Path:     "/test",
							Replicas: 1,
						},
					},
					Options: map[string]string{
						"dl-opts-k-1": "dl-opts-v-1",
					},
					SchedulerName: "scheduler-test",
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "topology.kubernetes.io/zone",
												Operator: v1.NodeSelectorOpIn,
												Values: []string{
													"antarctica-east1",
													"antarctica-west1",
												},
											},
										},
									},
								},
							},
							PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
								{
									Weight: 1,
									Preference: v1.NodeSelectorTerm{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "another-node-label-key",
												Operator: v1.NodeSelectorOpIn,
												Values: []string{
													"another-node-label-value",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			value: &CacheFS{
				ImportSource: "/datasource",
				Source:       "redis://test.metaurl:6379/1",
			},
			pods: []v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pods-1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pods-2",
					},
				},
			},
			cacheInfo: map[string]string{
				"cache-info-k1": "cache-info-v1",
				"cache-info-k2": "cache-info-v2",
				"cache-info-k3": "cache-info-v3",
			},
			want: &cdataload.DataLoadValue{
				Name: "test-dataload",
				Owner: &common.OwnerReference{
					APIVersion:         "/",
					Enabled:            true,
					Name:               "test-dataload",
					BlockOwnerDeletion: false,
					Controller:         true,
				},
				DataLoadInfo: cdataload.DataLoadInfo{
					BackoffLimit:  3,
					Image:         "fluid:v0.0.1",
					TargetDataset: "test-dataset",
					SchedulerName: "scheduler-test",
					TargetPaths: []cdataload.TargetPath{
						{
							Path:     "/test",
							Replicas: 1,
						},
					},
					ImagePullSecrets: []v1.LocalObjectReference{},
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "topology.kubernetes.io/zone",
												Operator: v1.NodeSelectorOpIn,
												Values: []string{
													"antarctica-east1",
													"antarctica-west1",
												},
											},
										},
									},
								},
							},
							PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{
								{
									Weight: 1,
									Preference: v1.NodeSelectorTerm{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "another-node-label-key",
												Operator: v1.NodeSelectorOpIn,
												Values: []string{
													"another-node-label-value",
												},
											},
										},
									},
								},
							},
						},
					},
					Options: map[string]string{
						// dataload spec options
						"dl-opts-k-1": "dl-opts-v-1",
						// cache info
						"cache-info-k1": "cache-info-v1",
						"cache-info-k2": "cache-info-v2",
						"cache-info-k3": "cache-info-v3",
						"podNames":      "pods-1:pods-2",
						"runtimeName":   "cachefs",
						"timeout":       DefaultDataLoadTimeout,
						"importSource":  "/datasource",
						"source":        "redis://test.metaurl:6379/1",
					},
				},
			},
		},
		"test case with node selector": {
			image:       "fluid:v0.0.1",
			runtimeName: "cachefs",
			targetDataset: &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataset",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{
						{
							Name:       "spark",
							MountPoint: "local://mnt/data0",
							Path:       "/mnt",
						},
					},
				},
			},
			dataload: &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataload",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DataLoadSpec{
					Dataset: datav1alpha1.TargetDataset{
						Name:      "test-dataset",
						Namespace: "fluid",
					},
					Target: []datav1alpha1.TargetPath{
						{
							Path:     "/test",
							Replicas: 1,
						},
					},
					SchedulerName: "scheduler-test",
					NodeSelector: map[string]string{
						"diskType": "ssd",
					},
					Options: map[string]string{
						"dl-opts-k-1": "dl-opts-v-1",
					},
				},
			},
			value: &CacheFS{
				ImportSource: "/datasource",
				Source:       "${METAURL}",
				Configs: Configs{
					MetaUrlSecret:    "metaurlsecret",
					MetaUrlSecretKey: "metaurlsecretkey",
				},
			},
			pods: []v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pods-1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pods-2",
					},
				},
			},
			cacheInfo: map[string]string{
				"cache-info-k1": "cache-info-v1",
				"cache-info-k2": "cache-info-v2",
				"cache-info-k3": "cache-info-v3",
			},
			want: &cdataload.DataLoadValue{
				Name: "test-dataload",
				Owner: &common.OwnerReference{
					APIVersion:         "/",
					Enabled:            true,
					Name:               "test-dataload",
					BlockOwnerDeletion: false,
					Controller:         true,
				},
				DataLoadInfo: cdataload.DataLoadInfo{
					BackoffLimit:  3,
					Image:         "fluid:v0.0.1",
					TargetDataset: "test-dataset",
					SchedulerName: "scheduler-test",
					TargetPaths: []cdataload.TargetPath{
						{
							Path:     "/test",
							Replicas: 1,
						},
					},
					ImagePullSecrets: []v1.LocalObjectReference{},
					NodeSelector: map[string]string{
						"diskType": "ssd",
					},
					Options: map[string]string{
						// dataload spec options
						"dl-opts-k-1": "dl-opts-v-1",
						// cache info
						"cache-info-k1":    "cache-info-v1",
						"cache-info-k2":    "cache-info-v2",
						"cache-info-k3":    "cache-info-v3",
						"podNames":         "pods-1:pods-2",
						"runtimeName":      "cachefs",
						"timeout":          DefaultDataLoadTimeout,
						"importSource":     "/datasource",
						"metaurlSecret":    "metaurlsecret",
						"metaurlSecretKey": "metaurlsecretkey",
					},
				},
			},
		},
		"test case with tolerations": {
			image:       "fluid:v0.0.1",
			runtimeName: "cachefs",
			targetDataset: &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataset",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{
						{
							Name:       "spark",
							MountPoint: "local://mnt/data0",
							Path:       "/mnt",
						},
					},
				},
			},
			dataload: &datav1alpha1.DataLoad{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-dataload",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DataLoadSpec{
					Dataset: datav1alpha1.TargetDataset{
						Name:      "test-dataset",
						Namespace: "fluid",
					},
					Target: []datav1alpha1.TargetPath{
						{
							Path:     "/test",
							Replicas: 1,
						},
					},
					SchedulerName: "scheduler-test",
					Tolerations: []v1.Toleration{
						{
							Key:      "example-key",
							Operator: v1.TolerationOpExists,
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
					Options: map[string]string{
						"dl-opts-k-1":      "dl-opts-v-1",
						"loadMetadataOnly": "true",
					},
				},
			},
			value: &CacheFS{
				ImportSource: "/datasource",
				Source:       "${METAURL}",
				Configs: Configs{
					MetaUrlSecret:    "metaurlsecret",
					MetaUrlSecretKey: "metaurlsecretkey",
				},
			},
			pods: []v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pods-1",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "pods-2",
					},
				},
			},
			cacheInfo: map[string]string{
				"cache-info-k1": "cache-info-v1",
				"cache-info-k2": "cache-info-v2",
				"cache-info-k3": "cache-info-v3",
			},
			want: &cdataload.DataLoadValue{
				Name: "test-dataload",
				Owner: &common.OwnerReference{
					APIVersion:         "/",
					Enabled:            true,
					Name:               "test-dataload",
					BlockOwnerDeletion: false,
					Controller:         true,
				},
				DataLoadInfo: cdataload.DataLoadInfo{
					BackoffLimit:  3,
					Image:         "fluid:v0.0.1",
					TargetDataset: "test-dataset",
					SchedulerName: "scheduler-test",
					TargetPaths: []cdataload.TargetPath{
						{
							Path:     "/test",
							Replicas: 1,
						},
					},
					ImagePullSecrets: []v1.LocalObjectReference{},
					Tolerations: []v1.Toleration{
						{
							Key:      "example-key",
							Operator: v1.TolerationOpExists,
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
					Options: map[string]string{
						// dataload spec options
						"dl-opts-k-1": "dl-opts-v-1",
						// cache info
						"cache-info-k1":    "cache-info-v1",
						"cache-info-k2":    "cache-info-v2",
						"cache-info-k3":    "cache-info-v3",
						"podNames":         "pods-1:pods-2",
						"runtimeName":      "cachefs",
						"timeout":          DefaultDataLoadTimeout,
						"importSource":     "/datasource",
						"metaurlSecret":    "metaurlsecret",
						"metaurlSecretKey": "metaurlsecretkey",
						"loadMetadataOnly": "true",
					},
				},
			},
		},
	}

	for k, item := range testCases {
		engine := CacheFSEngine{
			namespace: "fluid",
			name:      item.runtimeName,
			Log:       fake.NullLogger(),
		}
		got := engine.genDataLoadValue(item.image, item.cacheInfo, item.pods, item.targetDataset, item.dataload, item.value)
		if !reflect.DeepEqual(got, item.want) {
			t.Errorf("case %s:\n\tgot: %v\n\twant:%v", k, got, item.want)
		}
	}
}
