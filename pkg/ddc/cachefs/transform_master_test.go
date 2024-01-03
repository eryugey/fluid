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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
)

func TestTransformMaster(t *testing.T) {
	client := fake.NewFakeClientWithScheme(testScheme)
	engine := CacheFSEngine{
		name:      "test",
		namespace: "fluid",
		Client:    client,
		Log:       fake.NullLogger(),
		runtime: &datav1alpha1.CacheFSRuntime{
			Spec: datav1alpha1.CacheFSRuntimeSpec{
				Fuse:   datav1alpha1.CacheFSFuseSpec{},
				Master: datav1alpha1.CacheFSMasterSpec{},
			},
		},
	}
	memResource := resource.MustParse("1Gi")

	var tests = []struct {
		name         string
		runtime      *datav1alpha1.CacheFSRuntime
		cachefsValue *CacheFS
		wantOptions  map[string]string
		wantPort     string
		wantErr      bool
	}{
		{
			name: "master-not-enabled",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse:   datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{},
				}},
			cachefsValue: &CacheFS{
				Master: Master{
					Enabled: false,
					Options: map[string]string{},
				},
			},
			wantOptions: nil,
			wantPort:    "",
			wantErr:     false,
		},
		{
			name: "default-master",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse:   datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{},
				}},
			cachefsValue: &CacheFS{
				Master: Master{
					Enabled: true,
				},
			},
			wantOptions: map[string]string{
				"port":           "6379",
				"protected-mode": "no",
			},
			wantPort: "6379",
			wantErr:  false,
		},
		{
			name: "set-master-port",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{
						Options: map[string]string{
							"port": "7777",
						},
					},
				}},
			cachefsValue: &CacheFS{
				Master: Master{
					Enabled: true,
				},
			},
			wantOptions: map[string]string{
				"port":           "7777",
				"protected-mode": "no",
			},
			wantPort: "7777",
			wantErr:  false,
		},
		{
			name: "set-protect-mode",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{
						Options: map[string]string{
							"protected-mode": "yes",
						},
					},
				}},
			cachefsValue: &CacheFS{
				Master: Master{
					Enabled: true,
				},
			},
			wantOptions: map[string]string{
				"port":           "6379",
				"protected-mode": "no",
			},
			wantPort: "6379",
			wantErr:  false,
		},
		{
			name: "set-maxmemory",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{
						Options: map[string]string{
							"maxmemory": "1m",
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: memResource,
							},
						},
					},
				}},
			cachefsValue: &CacheFS{
				Master: Master{
					Enabled: true,
				},
			},
			wantOptions: map[string]string{
				"port":           "6379",
				"protected-mode": "no",
				"maxmemory":      "1m",
			},
			wantPort: "6379",
			wantErr:  false,
		},
	}
	for _, test := range tests {
		err := engine.transformMaster(test.runtime, test.cachefsValue)
		if (err != nil) && !test.wantErr {
			t.Errorf("Got err %v", err)
		}
		got := test.cachefsValue.Master.Options
		if !isMapEqual(got, test.wantOptions) {
			t.Errorf("transformMaster() %s:\ngot  = %v\nwant %v", test.name, got, test.wantOptions)
		}
		gotPort := test.cachefsValue.Master.Options["port"]
		if gotPort != test.wantPort {
			t.Errorf("transformMaster() %s: got port %s, want port %s", test.name, gotPort, test.wantPort)
		}
	}
}
