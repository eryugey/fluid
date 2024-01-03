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
	"reflect"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	"k8s.io/apimachinery/pkg/runtime"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/utils"
	"github.com/fluid-cloudnative/fluid/pkg/utils/kubeclient"
	appsv1 "k8s.io/api/apps/v1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mockExecCommandInContainerForTotalFileNums() (stdout string, stderr string, err error) {
	r := `1331167`
	return r, "", nil
}

func mockExecCommandInContainerForUsedStorageBytes() (stdout string, stderr string, err error) {
	r := `CacheFS:test 207300683100160  41460043776 207259223056384   1% /data`
	return r, "", nil
}

func TestCacheFSEngine_TotalStorageBytes(t *testing.T) {
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "fluid",
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"a": "b"},
			},
		},
	}
	var pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-work-0",
			Namespace: "fluid",
			Labels:    map[string]string{"a": "b"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	podList := &corev1.PodList{
		Items: []corev1.Pod{*pod},
	}
	runtimeObjs := []runtime.Object{}
	runtimeObjs = append(runtimeObjs, statefulSet, pod)
	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(appsv1.SchemeGroupVersion, statefulSet)
	scheme.AddKnownTypes(corev1.SchemeGroupVersion, pod)
	scheme.AddKnownTypes(corev1.SchemeGroupVersion, podList)
	fakeClient := fake.NewFakeClientWithScheme(scheme, runtimeObjs...)
	type fields struct {
		runtime   *datav1alpha1.CacheFSRuntime
		name      string
		namespace string
	}
	tests := []struct {
		name      string
		fields    fields
		wantValue int64
		wantErr   bool
	}{
		{
			name: "test",
			fields: fields{
				name:      "test",
				namespace: "fluid",
				runtime: &datav1alpha1.CacheFSRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test",
						Namespace: "fluid",
					},
				},
			},
			wantValue: 41460043776,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := CacheFSEngine{
				runtime:   tt.fields.runtime,
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
				Client:    fakeClient,
				Log:       fake.NullLogger(),
			}
			patch1 := ApplyFunc(kubeclient.ExecCommandInContainer, func(podName string, containerName string, namespace string, cmd []string) (string, string, error) {
				stdout, stderr, err := mockExecCommandInContainerForUsedStorageBytes()
				return stdout, stderr, err
			})
			defer patch1.Reset()
			gotValue, err := e.TotalStorageBytes()
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheFSEngine.TotalStorageBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotValue != tt.wantValue {
				t.Errorf("CacheFSEngine.TotalStorageBytes() = %v, want %v", gotValue, tt.wantValue)
			}
		})
	}
}

func TestCacheFSEngine_TotalFileNums(t *testing.T) {
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "fluid",
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"a": "b"},
			},
		},
	}
	var pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker-0",
			Namespace: "fluid",
			Labels:    map[string]string{"a": "b"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	podList := &corev1.PodList{
		Items: []corev1.Pod{*pod},
	}
	runtimeObjs := []runtime.Object{}
	runtimeObjs = append(runtimeObjs, statefulSet, pod)
	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(appsv1.SchemeGroupVersion, statefulSet)
	scheme.AddKnownTypes(corev1.SchemeGroupVersion, pod)
	scheme.AddKnownTypes(corev1.SchemeGroupVersion, podList)
	fakeClient := fake.NewFakeClientWithScheme(scheme, runtimeObjs...)
	type fields struct {
		runtime   *datav1alpha1.CacheFSRuntime
		name      string
		namespace string
	}
	tests := []struct {
		name      string
		fields    fields
		wantValue int64
		wantErr   bool
	}{
		{
			name: "test",
			fields: fields{
				name:      "test",
				namespace: "fluid",
				runtime: &datav1alpha1.CacheFSRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name: "spark",
					},
				},
			},
			wantValue: 1331167,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := CacheFSEngine{
				runtime:   tt.fields.runtime,
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
				Log:       fake.NullLogger(),
				Client:    fakeClient,
			}
			patch1 := ApplyFunc(kubeclient.ExecCommandInContainer, func(podName string, containerName string, namespace string, cmd []string) (string, string, error) {
				stdout, stderr, err := mockExecCommandInContainerForTotalFileNums()
				return stdout, stderr, err
			})
			defer patch1.Reset()
			gotValue, err := e.TotalFileNums()
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheFSEngine.TotalFileNums() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotValue != tt.wantValue {
				t.Errorf("CacheFSEngine.TotalFileNums() = %v, want %v", gotValue, tt.wantValue)
			}
		})
	}
}

func TestCacheFSEngine_FreeStorageBytes(t *testing.T) {
	type fields struct{}
	tests := []struct {
		name    string
		fields  fields
		want    int64
		wantErr bool
	}{
		{
			name:    "test",
			fields:  fields{},
			want:    0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheFSEngine{}
			got, err := c.FreeStorageBytes()
			if (err != nil) != tt.wantErr {
				t.Errorf("FreeStorageBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FreeStorageBytes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCacheFSEngine_PrepareUFS(t *testing.T) {
	type fields struct{}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "test",
			fields:  fields{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheFSEngine{}
			if err := c.PrepareUFS(); (err != nil) != tt.wantErr {
				t.Errorf("PrepareUFS() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCacheFSEngine_ShouldCheckUFS(t *testing.T) {
	type fields struct {
	}
	tests := []struct {
		name       string
		fields     fields
		wantShould bool
		wantErr    bool
	}{
		{
			name:       "test",
			fields:     fields{},
			wantShould: false,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheFSEngine{}
			gotShould, err := c.ShouldCheckUFS()
			if (err != nil) != tt.wantErr {
				t.Errorf("ShouldCheckUFS() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotShould != tt.wantShould {
				t.Errorf("ShouldCheckUFS() gotShould = %v, want %v", gotShould, tt.wantShould)
			}
		})
	}
}

func TestCacheFSEngine_ShouldUpdateUFS(t *testing.T) {
	type fields struct{}
	tests := []struct {
		name            string
		fields          fields
		wantUfsToUpdate *utils.UFSToUpdate
	}{
		{
			name:            "test",
			fields:          fields{},
			wantUfsToUpdate: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheFSEngine{}
			if gotUfsToUpdate := c.ShouldUpdateUFS(); !reflect.DeepEqual(gotUfsToUpdate, tt.wantUfsToUpdate) {
				t.Errorf("ShouldUpdateUFS() = %v, want %v", gotUfsToUpdate, tt.wantUfsToUpdate)
			}
		})
	}
}

func TestCacheFSEngine_UpdateOnUFSChange(t *testing.T) {
	type fields struct{}
	type args struct {
		ufsToUpdate *utils.UFSToUpdate
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantReady bool
		wantErr   bool
	}{
		{
			name:      "test",
			fields:    fields{},
			args:      args{},
			wantReady: true,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheFSEngine{}
			gotReady, err := c.UpdateOnUFSChange(tt.args.ufsToUpdate)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateOnUFSChange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotReady != tt.wantReady {
				t.Errorf("UpdateOnUFSChange() gotReady = %v, want %v", gotReady, tt.wantReady)
			}
		})
	}
}

func TestCacheFSEngine_UsedStorageBytes(t *testing.T) {
	statefulset := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker",
			Namespace: "cachefs",
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"a": "b"},
			},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:      1,
			ReadyReplicas: 1,
		},
	}

	var pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-worker-0",
			Namespace: "cachefs",
			Labels:    map[string]string{"a": "b"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	podList := &corev1.PodList{
		Items: []corev1.Pod{*pod},
	}
	runtimeObjs := []runtime.Object{}
	runtimeObjs = append(runtimeObjs, statefulset, pod)
	scheme := runtime.NewScheme()
	scheme.AddKnownTypes(appsv1.SchemeGroupVersion, statefulset)
	scheme.AddKnownTypes(corev1.SchemeGroupVersion, pod)
	scheme.AddKnownTypes(corev1.SchemeGroupVersion, podList)
	fakeClient := fake.NewFakeClientWithScheme(scheme, runtimeObjs...)
	type fields struct {
		runtime   *datav1alpha1.CacheFSRuntime
		name      string
		namespace string
	}
	tests := []struct {
		name      string
		fields    fields
		wantValue int64
		wantErr   bool
	}{
		{
			name: "test",
			fields: fields{
				name:      "test",
				namespace: "cachefs",
				runtime: &datav1alpha1.CacheFSRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Name: "spark",
					},
				},
			},
			wantValue: 41460043776,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheFSEngine{
				runtime:   tt.fields.runtime,
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
				Log:       fake.NullLogger(),
				Client:    fakeClient,
			}
			patch1 := ApplyFunc(kubeclient.ExecCommandInContainer, func(podName string, containerName string, namespace string, cmd []string) (string, string, error) {
				stdout, stderr, err := mockExecCommandInContainerForUsedStorageBytes()
				return stdout, stderr, err
			})
			defer patch1.Reset()
			gotValue, err := c.UsedStorageBytes()
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheFSEngine.UsedStorageBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotValue != tt.wantValue {
				t.Errorf("CacheFSEngine.UsedStorageBytes() = %v, want %v", gotValue, tt.wantValue)
			}
		})
	}
}
