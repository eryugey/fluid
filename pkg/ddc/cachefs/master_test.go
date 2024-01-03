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
	"testing"

	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
)

func TestCacheFSEngine_ShouldSetupMaster(t *testing.T) {
	cachefsruntimeInputs := []datav1alpha1.CacheFSRuntime{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test0",
				Namespace: "fluid",
			},
			Status: datav1alpha1.RuntimeStatus{
				MasterPhase: datav1alpha1.RuntimePhaseNotReady,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test1",
				Namespace: "fluid",
			},
			Status: datav1alpha1.RuntimeStatus{
				MasterPhase: datav1alpha1.RuntimePhaseNone,
			},
		},
	}
	testObjs := []runtime.Object{}
	for _, cachefsruntime := range cachefsruntimeInputs {
		testObjs = append(testObjs, cachefsruntime.DeepCopy())
	}
	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)

	engines := []CacheFSEngine{
		{
			name:      "test0",
			namespace: "fluid",
			Client:    client,
		},
		{
			name:      "test1",
			namespace: "fluid",
			Client:    client,
		},
	}

	var testCases = []struct {
		engine         CacheFSEngine
		expectedResult bool
	}{
		{
			engine:         engines[0],
			expectedResult: false,
		},
		{
			engine:         engines[1],
			expectedResult: true,
		},
	}

	for _, test := range testCases {
		if should, err := test.engine.ShouldSetupMaster(); should != test.expectedResult {
			t.Errorf("ShouldSetupMaster() got %v, want %v, err %s", should, test.expectedResult, err)
			return
		}
	}
}

func TestCacheFSEngine_SetupMaster(t *testing.T) {
	stsInputs := []v1.StatefulSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-master",
				Namespace: "fluid",
			},
			Status: v1.StatefulSetStatus{
				Replicas:      1,
				ReadyReplicas: 1,
			},
		},
	}

	testObjs := []runtime.Object{}
	for _, sts := range stsInputs {
		testObjs = append(testObjs, sts.DeepCopy())
	}

	cachefsruntimeInputs := []datav1alpha1.CacheFSRuntime{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "fluid",
			},
		},
	}
	for _, cachefsruntime := range cachefsruntimeInputs {
		testObjs = append(testObjs, cachefsruntime.DeepCopy())
	}
	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)

	engines := []CacheFSEngine{
		{
			name:      "test",
			namespace: "fluid",
			Client:    client,
			Log:       fake.NullLogger(),
		},
	}

	var testCases = []struct {
		engine            CacheFSEngine
		wantedStatefulSet v1.StatefulSet
	}{
		{
			engine:            engines[0],
			wantedStatefulSet: stsInputs[0],
		},
	}

	for _, test := range testCases {
		if err := test.engine.SetupMaster(); err != nil {
			t.Errorf("SetupMaster() err %v", err)
			return
		}
		cachefsruntime, err := test.engine.getRuntime()
		if err != nil {
			t.Errorf("fail to get the runtime")
			return
		}
		if cachefsruntime.Status.MasterPhase == datav1alpha1.RuntimePhaseNone {
			t.Errorf("fail to update the runtime")
			return
		}
	}
}

func TestCacheFSEngine_CheckMasterReady(t *testing.T) {
	statefulsetInputs := []v1.StatefulSet{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "spark-master",
				Namespace: "fluid",
			},
			Status: v1.StatefulSetStatus{
				ReadyReplicas: 1,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "hbase-master",
				Namespace: "fluid",
			},
			Status: v1.StatefulSetStatus{
				ReadyReplicas: 0,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-master",
				Namespace: "fluid",
			},
			Status: v1.StatefulSetStatus{
				ReadyReplicas: 0,
			},
		},
	}
	testObjs := []runtime.Object{}
	for _, statefulset := range statefulsetInputs {
		testObjs = append(testObjs, statefulset.DeepCopy())
	}

	cachefsruntimeInputs := []datav1alpha1.CacheFSRuntime{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "spark",
				Namespace: "fluid",
			},
			Spec: datav1alpha1.CacheFSRuntimeSpec{
				Master: datav1alpha1.CacheFSMasterSpec{},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "hbase",
				Namespace: "fluid",
			},
			Spec: datav1alpha1.CacheFSRuntimeSpec{
				Master: datav1alpha1.CacheFSMasterSpec{},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "fluid",
			},
			Spec: datav1alpha1.CacheFSRuntimeSpec{
				Master: datav1alpha1.CacheFSMasterSpec{
					ExternalEndpoint: datav1alpha1.EndpointSpec{
						MetaUrl: "redis://host:6379/1",
					},
				},
			},
		},
	}
	for _, cachefsruntime := range cachefsruntimeInputs {
		testObjs = append(testObjs, cachefsruntime.DeepCopy())
	}
	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)

	engines := []CacheFSEngine{
		{
			name:      "spark",
			namespace: "fluid",
			Client:    client,
			Log:       fake.NullLogger(),
		},
		{
			name:      "hbase",
			namespace: "fluid",
			Client:    client,
			Log:       fake.NullLogger(),
		},
		{
			name:      "test",
			namespace: "fluid",
			Client:    client,
			Log:       fake.NullLogger(),
		},
	}

	var testCases = []struct {
		engine         CacheFSEngine
		expectedResult bool
	}{
		{
			engine:         engines[0],
			expectedResult: true,
		},
		{
			engine:         engines[1],
			expectedResult: false,
		},
		{
			engine:         engines[2],
			expectedResult: true,
		},
	}

	for _, test := range testCases {
		if ready, err := test.engine.CheckMasterReady(); ready != test.expectedResult {
			t.Errorf("CheckMasterReady() got ready %v, want %v, err %s", ready, test.expectedResult, err)
			return
		}
		if !test.expectedResult {
			continue
		}
		_, err := test.engine.getRuntime()
		if err != nil {
			t.Errorf("fail to get runtime %v", err)
			return
		}
	}
}
