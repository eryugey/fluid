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
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/fluid-cloudnative/fluid/pkg/common"
	"github.com/fluid-cloudnative/fluid/pkg/ddc/base"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
)

func TestTransformFuse(t *testing.T) {
	cachefsSecret1 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test1",
			Namespace: "fluid",
		},
		Data: map[string][]byte{
			"metaurl":    []byte(base64.StdEncoding.EncodeToString([]byte("test"))),
			"access-key": []byte(base64.StdEncoding.EncodeToString([]byte("test"))),
			"secret-key": []byte(base64.StdEncoding.EncodeToString([]byte("test"))),
		},
	}
	cachefsSecret2 := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test2",
			Namespace: "fluid",
		},
		Data: map[string][]byte{
			"access-key": []byte(base64.StdEncoding.EncodeToString([]byte("test"))),
			"secret-key": []byte(base64.StdEncoding.EncodeToString([]byte("test"))),
		},
	}
	testObjs := []runtime.Object{}
	testObjs = append(testObjs, (*cachefsSecret1).DeepCopy(), cachefsSecret2.DeepCopy())

	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)
	engine := CacheFSEngine{
		name:      "test",
		namespace: "fluid",
		Client:    client,
		Log:       fake.NullLogger(),
		runtime:   &datav1alpha1.CacheFSRuntime{},
	}

	var tests = []struct {
		name         string
		runtime      *datav1alpha1.CacheFSRuntime
		dataset      *datav1alpha1.Dataset
		cachefsValue *CacheFS
		expect       string
		wantErr      bool
	}{
		{
			name: "test-secret-right",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{
						ExternalEndpoint: datav1alpha1.EndpointSpec{
							MetaUrl: "redis://host/1",
						},
					},
				}},
			dataset: &datav1alpha1.Dataset{
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{{
						MountPoint: "cachefs:///mnt/test",
						Name:       "test1",
						Options: map[string]string{
							"storage": "test1",
							"bucket":  "test1",
						},
						EncryptOptions: []datav1alpha1.EncryptOption{
							{
								Name: "access-key",
								ValueFrom: datav1alpha1.EncryptOptionSource{
									SecretKeyRef: datav1alpha1.SecretKeySelector{
										Name: "test1",
										Key:  "access-key",
									}},
							}, {
								Name: "secret-key",
								ValueFrom: datav1alpha1.EncryptOptionSource{
									SecretKeyRef: datav1alpha1.SecretKeySelector{
										Name: "test1",
										Key:  "secret-key",
									}},
							}, {
								Name: "metaurl",
								ValueFrom: datav1alpha1.EncryptOptionSource{
									SecretKeyRef: datav1alpha1.SecretKeySelector{
										Name: "test1",
										Key:  "metaurl",
									}},
							}},
					}},
				}},
			cachefsValue: &CacheFS{
				Worker: Worker{},
			},
			expect:  "",
			wantErr: false,
		},
		{
			name: "test-secret-wrong-1",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{
						ExternalEndpoint: datav1alpha1.EndpointSpec{
							MetaUrl: "redis://host/1",
						},
					},
				},
			},
			dataset: &datav1alpha1.Dataset{
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{{
						MountPoint: "cachefs:///mnt/test",
						Name:       "test2",
						EncryptOptions: []datav1alpha1.EncryptOption{{
							Name: "metaurl",
							ValueFrom: datav1alpha1.EncryptOptionSource{
								SecretKeyRef: datav1alpha1.SecretKeySelector{
									Name: "test1",
									Key:  "metaurl",
								},
							},
						}},
					}},
				}}, cachefsValue: &CacheFS{}, expect: "", wantErr: true,
		},
		{
			name: "test-secret-wrong-2",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{
						ExternalEndpoint: datav1alpha1.EndpointSpec{
							MetaUrl: "redis://host/1",
						},
					},
				}},
			dataset: &datav1alpha1.Dataset{
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{{
						MountPoint: "cachefs:///mnt/test",
						Name:       "test3",
						EncryptOptions: []datav1alpha1.EncryptOption{{
							Name: "metaurl",
							ValueFrom: datav1alpha1.EncryptOptionSource{
								SecretKeyRef: datav1alpha1.SecretKeySelector{
									Name: "test1",
									Key:  "metaurl",
								},
							},
						}},
					}}}},
			cachefsValue: &CacheFS{},
			expect:       "",
			wantErr:      true,
		},
		{
			name: "test-options",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{
						ExternalEndpoint: datav1alpha1.EndpointSpec{
							MetaUrl: "redis://host/1",
						},
					},
					TieredStore: datav1alpha1.TieredStore{
						Levels: []datav1alpha1.Level{{
							MediumType: "MEM",
							Path:       "/data",
							Low:        "0.7",
						}},
					},
				}},
			dataset: &datav1alpha1.Dataset{
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{{
						MountPoint: "cachefs:///mnt/test",
						Name:       "test2",
						Options:    map[string]string{"debug": ""},
						EncryptOptions: []datav1alpha1.EncryptOption{{
							Name: "metaurl",
							ValueFrom: datav1alpha1.EncryptOptionSource{
								SecretKeyRef: datav1alpha1.SecretKeySelector{
									Name: "test1",
									Key:  "metaurl",
								},
							},
						}},
					}}}},
			cachefsValue: &CacheFS{},
			expect:       "",
			wantErr:      false,
		},
		{
			name:    "test-no-mount",
			runtime: &datav1alpha1.CacheFSRuntime{},
			dataset: &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-no-mount",
					Namespace: "fluid",
				},
				Spec: datav1alpha1.DatasetSpec{},
			},
			cachefsValue: &CacheFS{},
			expect:       "",
			wantErr:      true,
		},
		{
			name:    "test-no-secret",
			runtime: &datav1alpha1.CacheFSRuntime{},
			dataset: &datav1alpha1.Dataset{
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{{
						MountPoint: "cachefs:///mnt/test",
						Name:       "test2",
						EncryptOptions: []datav1alpha1.EncryptOption{{
							Name: "metaurl",
							ValueFrom: datav1alpha1.EncryptOptionSource{
								SecretKeyRef: datav1alpha1.SecretKeySelector{
									Name: "not-exist",
									Key:  "metaurl",
								},
							},
						}},
					}},
				}}, cachefsValue: &CacheFS{}, expect: "", wantErr: true,
		},
		{
			name:    "test-no-metaurl",
			runtime: &datav1alpha1.CacheFSRuntime{},
			dataset: &datav1alpha1.Dataset{
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{{
						MountPoint: "cachefs:///mnt/test",
						Name:       "test",
						EncryptOptions: []datav1alpha1.EncryptOption{{
							Name: "metaurl",
							ValueFrom: datav1alpha1.EncryptOptionSource{
								SecretKeyRef: datav1alpha1.SecretKeySelector{
									Name: "no-metaurl",
									Key:  "metaurl",
								},
							},
						}},
					}},
				}}, cachefsValue: &CacheFS{}, expect: "", wantErr: true,
		},
		{
			name: "test-tiredstore",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{
						Options: map[string]string{"verbose": ""},
					},
					Master: datav1alpha1.CacheFSMasterSpec{
						ExternalEndpoint: datav1alpha1.EndpointSpec{
							MetaUrl: "redis://host/1",
						},
					},
					TieredStore: datav1alpha1.TieredStore{
						Levels: []datav1alpha1.Level{{
							MediumType: "SSD",
							Path:       "/data",
							Low:        "0.7",
							Quota:      resource.NewQuantity(10, resource.BinarySI),
						}},
					},
				}},
			dataset: &datav1alpha1.Dataset{
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{{
						MountPoint: "cachefs:///mnt/test",
						Name:       "test",
						Options:    map[string]string{"debug": ""},
						EncryptOptions: []datav1alpha1.EncryptOption{{
							Name: "metaurl",
							ValueFrom: datav1alpha1.EncryptOptionSource{
								SecretKeyRef: datav1alpha1.SecretKeySelector{
									Name: "test1",
									Key:  "metaurl",
								},
							},
						}},
					}}}},
			cachefsValue: &CacheFS{},
			expect:       "",
			wantErr:      false,
		}, {
			name: "test-secret3-right",
			runtime: &datav1alpha1.CacheFSRuntime{
				Spec: datav1alpha1.CacheFSRuntimeSpec{
					Fuse: datav1alpha1.CacheFSFuseSpec{},
					Master: datav1alpha1.CacheFSMasterSpec{
						ExternalEndpoint: datav1alpha1.EndpointSpec{
							MetaUrl: "redis://host/1",
						},
					},
				}},
			dataset: &datav1alpha1.Dataset{
				Spec: datav1alpha1.DatasetSpec{
					Mounts: []datav1alpha1.Mount{{
						MountPoint: "cachefs:///mnt/test",
						Name:       "test1",
						Options: map[string]string{
							"storage": "test1",
							"bucket":  "test1",
						},
						EncryptOptions: []datav1alpha1.EncryptOption{
							{
								Name: "access-key",
								ValueFrom: datav1alpha1.EncryptOptionSource{
									SecretKeyRef: datav1alpha1.SecretKeySelector{
										Name: "test3",
										Key:  "access-key",
									}},
							}, {
								Name: "secret-key",
								ValueFrom: datav1alpha1.EncryptOptionSource{
									SecretKeyRef: datav1alpha1.SecretKeySelector{
										Name: "test3",
										Key:  "secret-key",
									}},
							}, {
								Name: "metaurl",
								ValueFrom: datav1alpha1.EncryptOptionSource{
									SecretKeyRef: datav1alpha1.SecretKeySelector{
										Name: "test3",
										Key:  "metaurl",
									}},
							}},
					}},
				}},
			cachefsValue: &CacheFS{
				Worker: Worker{},
			},
			expect:  "",
			wantErr: false,
		},
	}
	for _, test := range tests {
		err := engine.transformFuse(test.runtime, test.dataset, test.cachefsValue)
		if (err != nil) && !test.wantErr {
			t.Errorf("Got err %v", err)
		}
	}
}

func TestCacheFSEngine_genValue(t *testing.T) {
	engine := CacheFSEngine{
		name:      "testengine",
		namespace: "fluid",
		Log:       fake.NullLogger(),
		runtime: &datav1alpha1.CacheFSRuntime{
			Spec: datav1alpha1.CacheFSRuntimeSpec{
				Fuse: datav1alpha1.CacheFSFuseSpec{},
			},
		},
	}
	type args struct {
		mount                datav1alpha1.Mount
		tiredStoreLevel      *datav1alpha1.Level
		value                *CacheFS
		master               *datav1alpha1.CacheFSMasterSpec
		sharedOptions        map[string]string
		sharedEncryptOptions []datav1alpha1.EncryptOption
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		wantSource  string
		wantConfigs Configs
		wantOptions map[string]string
	}{
		{
			name: "test",
			args: args{
				sharedOptions: map[string]string{"a": "b"},
				sharedEncryptOptions: []datav1alpha1.EncryptOption{{
					Name: "encrypt-name",
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "secretName",
						Key:  "secretKey",
					}}},
				},
				mount: datav1alpha1.Mount{
					MountPoint: "pvc://pvcname",
					Options:    map[string]string{"a": "c", "kb": "vb"},
					Name:       "testmount",
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: "SSD",
					Path:       "/dev",
				},
				value: &CacheFS{
					FullnameOverride: "testfullname",
					Fuse:             Fuse{},
					Worker:           Worker{},
				},
				master: &datav1alpha1.CacheFSMasterSpec{
					ExternalEndpoint: datav1alpha1.EndpointSpec{
						MetaUrl: "redis://host/1",
					},
				},
			},
			wantErr:    false,
			wantSource: "redis://host/1",
			wantConfigs: Configs{
				Name:             "testmount",
				MetaUrlSecret:    "",
				MetaUrlSecretKey: "",
				FormatCmd:        "",
				EncryptEnvOptions: []EncryptEnvOption{
					{
						Name:             "encrypt-name",
						EnvName:          "encrypt_name",
						SecretKeyRefName: "secretName",
						SecretKeyRefKey:  "secretKey",
					},
				},
			},
			wantOptions: map[string]string{
				"a":  "c",
				"kb": "vb",
			},
		}, {
			name: "mount-encrypt-options",
			args: args{
				sharedOptions: map[string]string{"a": "b"},
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///test",
					Options:    map[string]string{"aa": "c", "kb": "vb"},
					Name:       "dataset-mount-encrypt-options",
					EncryptOptions: []datav1alpha1.EncryptOption{{
						Name: "encrypt-name",
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "secretName",
							Key:  "secretKey",
						}}},
					},
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: "SSD",
					Path:       "/dev",
				},
				value: &CacheFS{
					FullnameOverride: "testfullname",
					Fuse:             Fuse{},
					Worker:           Worker{},
				},
				master: &datav1alpha1.CacheFSMasterSpec{
					ExternalEndpoint: datav1alpha1.EndpointSpec{
						MetaUrl: "redis://host/1",
					},
				},
			},
			wantErr:    false,
			wantSource: "redis://host/1",
			wantConfigs: Configs{
				Name:             "dataset-mount-encrypt-options",
				MetaUrlSecret:    "",
				MetaUrlSecretKey: "",
				FormatCmd:        "",
				EncryptEnvOptions: []EncryptEnvOption{
					{
						Name:             "encrypt-name",
						EnvName:          "encrypt_name",
						SecretKeyRefName: "secretName",
						SecretKeyRefKey:  "secretKey",
					},
				},
			},
			wantOptions: map[string]string{
				"a":  "b",
				"aa": "c",
				"kb": "vb",
			},
		}, {
			name: "metaurl-from-secret",
			args: args{
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///test",
					Name:       "metaurl-from-secret",
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: "SSD",
					Path:       "/dev",
				},
				value: &CacheFS{
					FullnameOverride: "testfullname",
					Fuse:             Fuse{},
					Worker:           Worker{},
				},
				master: &datav1alpha1.CacheFSMasterSpec{
					ExternalEndpoint: datav1alpha1.EndpointSpec{
						MetaUrl: "redis://host/1",
						EncryptOptions: []datav1alpha1.EncryptOption{{
							Name: "metaurl",
							ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
								Name: "secretName",
								Key:  "secretKey",
							}}},
						},
					},
				},
			},
			wantErr:    false,
			wantSource: "${METAURL}",
			wantConfigs: Configs{
				Name:              "metaurl-from-secret",
				MetaUrlSecret:     "secretName",
				MetaUrlSecretKey:  "secretKey",
				FormatCmd:         "",
				EncryptEnvOptions: []EncryptEnvOption{},
			},
			wantOptions: map[string]string{},
		}, {
			name: "no-source",
			args: args{
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///test",
					Name:       "no-source",
					Options:    map[string]string{"a": "c"},
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: "SSD",
					Path:       "/dev",
				},
				value: &CacheFS{
					FullnameOverride: "no-source",
					Fuse:             Fuse{},
					Worker:           Worker{},
				},
				master: &datav1alpha1.CacheFSMasterSpec{},
			},
			wantErr:    false,
			wantSource: "redis://testengine-master-0.testengine-master-svc.fluid:6379/1",
			wantConfigs: Configs{
				Name:              "no-source",
				MetaUrlSecret:     "",
				MetaUrlSecretKey:  "",
				FormatCmd:         "",
				EncryptEnvOptions: []EncryptEnvOption{},
			},
			wantOptions: map[string]string{
				"a": "c",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt, err := engine.genValue(tt.args.mount, tt.args.tiredStoreLevel, tt.args.value, tt.args.sharedOptions, tt.args.sharedEncryptOptions, tt.args.master)
			if (err != nil) != tt.wantErr {
				t.Errorf("testcase %s: genValue() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			}
			if tt.args.value.Source != tt.wantSource {
				t.Errorf("test case %s:\nwant source: %s\ngot  source: %s", tt.name, tt.args.value.Source, tt.wantSource)
			}
			if !reflect.DeepEqual(tt.args.value.Configs, tt.wantConfigs) {
				t.Errorf("test case %s:\nwant Configs: %v\ngot  Configs: %v", tt.name, tt.args.value.Configs, tt.wantConfigs)
			}
			if len(opt) != len(tt.wantOptions) {
				t.Errorf("testcase %s: genValue() got = %v, wantOptions %v", tt.name, opt, tt.wantOptions)
			}
			for k, v := range opt {
				if v1, ok := tt.wantOptions[k]; !ok {
					t.Errorf("testcase %s: CachefsEngine.genValue() should has key: %v", tt.name, k)
				} else {
					if v1 != v {
						t.Errorf("testcase %s: CachefsEngine.genValue()  key: %v value: %v, get value: %v", tt.name, k, v1, v)
					} else {
						delete(tt.wantOptions, k)
					}
				}
			}
		})
	}
}

func TestCacheFSEngine_genMount(t *testing.T) {
	type fields struct {
		name      string
		namespace string
		Log       logr.Logger
	}
	type args struct {
		value   *CacheFS
		mount   datav1alpha1.Mount
		options map[string]string
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantErr         bool
		wantFuseCommand string
		wantFuseStatCmd string
	}{
		{
			name: "test",
			fields: fields{
				name:      "test",
				namespace: "fluid",
				Log:       fake.NullLogger(),
			},
			args: args{
				value: &CacheFS{
					FullnameOverride: "test",
					Source:           "redis://127.0.0.1:6379",
					Configs: Configs{
						Name:          "test",
						MetaUrlSecret: "test",
					},
					Fuse: Fuse{
						MountPath:     "/test",
						HostMountPath: "/test",
					},
					Worker: Worker{
						MountPath: "/test-worker",
					},
				},
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///",
				},
			},
			wantErr:         false,
			wantFuseCommand: "/bin/mount.cachefs redis://127.0.0.1:6379 /test -o cache-group=fluid-test,no-sharing,metrics=0.0.0.0:9567,no-bgjob,backup-meta=0",
			wantFuseStatCmd: "stat -c %i /test",
		},
		{
			name: "test-options",
			fields: fields{
				name:      "test",
				namespace: "fluid",
				Log:       fake.NullLogger(),
			},
			args: args{
				value: &CacheFS{
					FullnameOverride: "test",
					Source:           "redis://127.0.0.1:6379",
					Configs: Configs{
						Name:          "test",
						MetaUrlSecret: "test",
					},
					Fuse: Fuse{
						MountPath:     "/test",
						HostMountPath: "/test",
					},
					Worker: Worker{
						MountPath: "/test-worker",
					},
				},
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///",
				},
				options: map[string]string{"verbose": ""},
			},
			wantErr:         false,
			wantFuseCommand: "/bin/mount.cachefs redis://127.0.0.1:6379 /test -o verbose,cache-group=fluid-test,no-sharing,metrics=0.0.0.0:9567,no-bgjob,backup-meta=0",
			wantFuseStatCmd: "stat -c %i /test",
		},
		{
			name: "test-pvc",
			fields: fields{
				name:      "test",
				namespace: "fluid",
				Log:       fake.NullLogger(),
			},
			args: args{
				value: &CacheFS{
					FullnameOverride: "test",
					Source:           "redis://127.0.0.1:6379",
					Configs: Configs{
						Name:          "test",
						MetaUrlSecret: "test",
					},
					Fuse: Fuse{
						MountPath:     "/test",
						HostMountPath: "/test",
					},
					Worker: Worker{
						MountPath: "/test-worker",
					},
				},
				mount: datav1alpha1.Mount{
					MountPoint: "pvc://pvcname",
				},
				options: map[string]string{"verbose": ""},
			},
			wantErr:         false,
			wantFuseCommand: "/bin/mount.cachefs redis://127.0.0.1:6379 /test -o verbose,cache-group=fluid-test,no-sharing,metrics=0.0.0.0:9567,no-bgjob,backup-meta=0,source=/tmp",
			wantFuseStatCmd: "stat -c %i /test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataset := &datav1alpha1.Dataset{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.fields.name,
					Namespace: tt.fields.namespace,
				},
			}
			client := fake.NewFakeClientWithScheme(testScheme, dataset)
			c := &CacheFSEngine{
				name:      tt.fields.name,
				namespace: tt.fields.namespace,
				Log:       tt.fields.Log,
				Client:    client,
			}
			if err := c.genFuseMount(tt.args.value, tt.args.options, tt.args.mount); (err != nil) != tt.wantErr {
				t.Errorf("genMount() \nerror = %v\nwantErr = %v", err, tt.wantErr)
			}
			if len(tt.args.value.Fuse.Command) != len(tt.wantFuseCommand) ||
				tt.args.value.Fuse.StatCmd != tt.wantFuseStatCmd {
				t.Errorf("genMount():\ncmd  = %v\nwant = %v\n", tt.args.value.Fuse.Command, tt.wantFuseCommand)
				t.Errorf("\nStatCmd = %v\nwant    = %v", tt.args.value.Fuse.StatCmd, tt.wantFuseStatCmd)
			}
		})
	}
}

func TestCacheFSEngine_genFormatCmd(t *testing.T) {
	type args struct {
		value  *CacheFS
		engine *CacheFSEngine
	}
	tests := []struct {
		name          string
		args          args
		wantFormatCmd string
	}{
		{
			name: "test",
			args: args{
				value: &CacheFS{
					FullnameOverride: "test",
					Source:           "redis://127.0.0.1:6379",
					Configs: Configs{
						Name: "test",
					},
					Fuse: Fuse{
						MountPath:     "/test",
						HostMountPath: "/test",
					},
				},
				engine: &CacheFSEngine{
					runtime: &datav1alpha1.CacheFSRuntime{
						Spec: datav1alpha1.CacheFSRuntimeSpec{},
					},
				},
			},
			wantFormatCmd: "/bin/cachefs format --no-update redis://127.0.0.1:6379 test",
		},
		{
			name: "test-configs",
			args: args{
				value: &CacheFS{
					FullnameOverride: "test-configs",
					Source:           "redis://127.0.0.1:6379",
					Configs: Configs{
						Name: "test-configs",
					},
					Fuse: Fuse{
						MountPath:     "/test",
						HostMountPath: "/test",
					},
				},
				engine: &CacheFSEngine{
					runtime: &datav1alpha1.CacheFSRuntime{
						Spec: datav1alpha1.CacheFSRuntimeSpec{
							Configs: map[string]string{
								"force": "",
							},
						},
					},
				},
			},
			wantFormatCmd: "/bin/cachefs format --no-update --force redis://127.0.0.1:6379 test-configs",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.args.engine
			c.genFormatCmd(tt.args.value, c.runtime.Spec.Configs)
			if tt.args.value.Configs.FormatCmd != tt.wantFormatCmd {
				t.Errorf("genMount() got = %v, want %v", tt.args.value.Configs.FormatCmd, tt.wantFormatCmd)
			}
		})
	}
}

func Test_genOption(t *testing.T) {
	type args struct {
		optionMap map[string]string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "test",
			args: args{
				optionMap: map[string]string{"a": "b", "c": ""},
			},
			want: []string{"a=b", "c"},
		},
		{
			name: "test-empty",
			args: args{
				optionMap: nil,
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := genArgs(tt.args.optionMap)
			if !isSliceEqual(got, tt.want) {
				t.Errorf("genOption() = %v, want %v", got, tt.want)
			}
		})
	}
}

func isSliceEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}

	diff := make(map[string]int, len(got))
	for _, v := range got {
		diff[v]++
	}
	for _, v := range want {
		if _, ok := diff[v]; !ok {
			return false
		}
		diff[v] -= 1
		if diff[v] == 0 {
			delete(diff, v)
		}
	}
	return len(diff) == 0
}

func isMapEqual(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}

	for k, v := range got {
		if want[k] != v {
			return false
		}
	}
	return true
}

func TestClientVersion_LessThan(t *testing.T) {
	type fields struct {
		Major int
		Minor int
		Patch int
		Tag   string
	}
	type args struct {
		other *ClientVersion
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "less",
			fields: fields{
				Major: 1,
				Minor: 0,
				Patch: 0,
				Tag:   "",
			},
			args: args{
				other: &ClientVersion{
					Major: 1,
					Minor: 0,
					Patch: 1,
					Tag:   "",
				},
			},
			want: true,
		},
		{
			name: "more",
			fields: fields{
				Major: 1,
				Minor: 0,
				Patch: 0,
				Tag:   "",
			},
			args: args{
				other: &ClientVersion{
					Major: 0,
					Minor: 1,
					Patch: 0,
					Tag:   "",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &ClientVersion{
				Major: tt.fields.Major,
				Minor: tt.fields.Minor,
				Patch: tt.fields.Patch,
				Tag:   tt.fields.Tag,
			}
			if got := v.LessThan(tt.args.other); got != tt.want {
				t.Errorf("LessThan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCacheFSEngine_genMountOptions(t *testing.T) {
	result := resource.MustParse("20Gi")
	type args struct {
		mount           datav1alpha1.Mount
		tiredStoreLevel *datav1alpha1.Level
	}
	tests := []struct {
		name        string
		args        args
		wantOptions map[string]string
		wantErr     bool
	}{
		{
			name: "cachefs",
			args: args{
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///test",
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: common.SSD,
					VolumeType: common.VolumeTypeHostPath,
					Path:       "/abc",
					Quota:      &result,
				},
			},
			wantOptions: map[string]string{
				"subdir":     "/test",
				"cache-dir":  "/abc",
				"cache-size": "20480",
			},
			wantErr: false,
		},
		{
			name: "pvc",
			args: args{
				mount: datav1alpha1.Mount{
					MountPoint: "pvc://testpvc",
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: common.SSD,
					VolumeType: common.VolumeTypeHostPath,
					Path:       "/abc",
					Quota:      &result,
				},
			},
			wantOptions: map[string]string{
				"cache-dir":  "/abc",
				"cache-size": "20480",
				"source":     "/datasource",
			},
			wantErr: false,
		},
		{
			name: "pvc with subdir",
			args: args{
				mount: datav1alpha1.Mount{
					MountPoint: "pvc://testpvc/subdir",
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: common.SSD,
					VolumeType: common.VolumeTypeHostPath,
					Path:       "/abc",
					Quota:      &result,
				},
			},
			wantOptions: map[string]string{
				"cache-dir":  "/abc",
				"cache-size": "20480",
				"source":     "/datasource/subdir",
			},
			wantErr: false,
		},
		{
			name: "local",
			args: args{
				mount: datav1alpha1.Mount{
					MountPoint: "local:///host/path",
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: common.SSD,
					VolumeType: common.VolumeTypeHostPath,
					Path:       "/abc",
					Quota:      &result,
				},
			},
			wantOptions: map[string]string{
				"cache-dir":  "/abc",
				"cache-size": "20480",
				"source":     "/datasource",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CacheFSEngine{}
			gotOptions, err := c.genMountOptions(tt.args.mount, tt.args.tiredStoreLevel)
			if (err != nil) != tt.wantErr {
				t.Errorf("genMountOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !isMapEqual(gotOptions, tt.wantOptions) {
				t.Errorf("genMountOptions() gotOptions = %v, want %v", gotOptions, tt.wantOptions)
			}
		})
	}
}

func TestCacheFSEngine_genReadonlyOptions(t *testing.T) {
	testRuntimeInfo, err := base.BuildRuntimeInfo("testdataset", "default", "cachefs", datav1alpha1.TieredStore{})
	if err != nil {
		t.Errorf("failed to create test runtimeInfo: %s", err)
	}
	roRuntimeInfo, err := base.BuildRuntimeInfo("rodataset", "default", "cachefs", datav1alpha1.TieredStore{})
	if err != nil {
		t.Errorf("failed to create ro runtimeInfo: %s", err)
	}
	rwRuntimeInfo, err := base.BuildRuntimeInfo("rwdataset", "default", "cachefs", datav1alpha1.TieredStore{})
	if err != nil {
		t.Errorf("failed to create rw runtimeInfo: %s", err)
	}

	testDataset := &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testdataset",
			Namespace: "default",
		},
		Spec: datav1alpha1.DatasetSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteMany,
			},
		},
	}
	roDataset := &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rodataset",
			Namespace: "default",
		},
		Spec: datav1alpha1.DatasetSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadOnlyMany,
			},
		},
	}
	rwDataset := &datav1alpha1.Dataset{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rwdataset",
			Namespace: "default",
		},
		Spec: datav1alpha1.DatasetSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteMany,
			},
		},
	}

	tests := []struct {
		name    string
		info    base.RuntimeInfoInterface
		dataset *datav1alpha1.Dataset
		ro      bool
	}{
		{
			name:    "test",
			info:    testRuntimeInfo,
			dataset: testDataset,
			ro:      false,
		},
		{
			name:    "ro dataset",
			info:    roRuntimeInfo,
			dataset: roDataset,
			ro:      true,
		},
		{
			name:    "rw dataset",
			info:    rwRuntimeInfo,
			dataset: rwDataset,
			ro:      false,
		},
	}
	for _, tc := range tests {
		info := tc.info
		info.SetupWithDataset(tc.dataset)
		engine := &CacheFSEngine{Log: fake.NullLogger(), runtimeInfo: info}
		engine.name = info.GetName()
		engine.namespace = info.GetNamespace()
		s := runtime.NewScheme()
		s.AddKnownTypes(datav1alpha1.GroupVersion, tc.dataset)
		client := fake.NewFakeClientWithScheme(s, tc.dataset)
		engine.Client = client

		options := make(map[string]string)
		readonly := engine.genReadonlyOptions(options)
		if readonly != tc.ro {
			t.Errorf("%s: genReadonlyOptions() got = %v, want %v", tc.name, readonly, tc.ro)
		}
		_, ok := options["ro"]
		if !ok && tc.ro {
			t.Errorf("%s: genReadonlyOptions() didn't set ro options: %v", tc.name, options)
		}
		if ok && !tc.ro {
			t.Errorf("%s: genReadonlyOptions() set ro options unexpectly: %v", tc.name, options)
		}
	}
}
