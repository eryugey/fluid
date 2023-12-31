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
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/fluid-cloudnative/fluid/pkg/common"
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
		runtime: &datav1alpha1.CacheFSRuntime{
			Spec: datav1alpha1.CacheFSRuntimeSpec{
				Fuse: datav1alpha1.CacheFSFuseSpec{},
			},
		},
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
				Spec: datav1alpha1.CacheFSRuntimeSpec{Fuse: datav1alpha1.CacheFSFuseSpec{}}},
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
	cachefsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-genValue",
			Namespace: "fluid",
		},
		Data: map[string][]byte{
			"metaurl":    []byte(base64.StdEncoding.EncodeToString([]byte("test"))),
			"access-key": []byte(base64.StdEncoding.EncodeToString([]byte("test"))),
			"secret-key": []byte(base64.StdEncoding.EncodeToString([]byte("test"))),
		},
	}

	testObjs := []runtime.Object{}
	testObjs = append(testObjs, cachefsSecret.DeepCopy())

	client := fake.NewFakeClientWithScheme(testScheme, testObjs...)
	engine := CacheFSEngine{
		name:      "test",
		namespace: "fluid",
		Client:    client,
		Log:       fake.NullLogger(),
		runtime: &datav1alpha1.CacheFSRuntime{
			Spec: datav1alpha1.CacheFSRuntimeSpec{
				Fuse: datav1alpha1.CacheFSFuseSpec{},
			},
		},
	}
	type fields struct {
		runtime     *datav1alpha1.CacheFSRuntime
		name        string
		namespace   string
		runtimeType string
	}
	type args struct {
		mount                datav1alpha1.Mount
		tiredStoreLevel      *datav1alpha1.Level
		value                *CacheFS
		sharedOptions        map[string]string
		sharedEncryptOptions []datav1alpha1.EncryptOption
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		wantValue   *CacheFS
		wantOptions map[string]string
	}{
		{
			name: "test",
			fields: fields{
				name:        "test",
				namespace:   "fluid",
				runtimeType: common.CacheFSRuntime,
			},
			args: args{
				sharedOptions: map[string]string{"a": "b"},
				sharedEncryptOptions: []datav1alpha1.EncryptOption{{
					Name: CacheFSMetaUrl,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test",
						Key:  "access-key",
					}}}, {
					Name: CacheFSAccessKey,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test",
						Key:  "secret-key",
					}}}, {
					Name: CacheFSSecretKey,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test",
						Key:  "metaurl",
					}}},
				},
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///test",
					Options:    map[string]string{"a": "c"},
					Name:       "test",
					EncryptOptions: []datav1alpha1.EncryptOption{{
						Name: CacheFSMetaUrl,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test",
							Key:  "metaurl",
						}}}, {
						Name: CacheFSAccessKey,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test",
							Key:  "access-key",
						}}}, {
						Name: CacheFSSecretKey,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test",
							Key:  "secret-key",
						}}},
					},
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: "SSD",
					Path:       "/dev",
				},
				value: &CacheFS{
					FullnameOverride: "test",
					Fuse:             Fuse{},
					Worker:           Worker{},
				},
			},
			wantErr: false,
			wantOptions: map[string]string{
				"a": "c",
			},
		}, {
			name: "test-mirror-buckets",
			fields: fields{
				name:        "test-mirror-buckets",
				namespace:   "fluid",
				runtimeType: common.CacheFSRuntime,
			},
			args: args{
				sharedOptions: map[string]string{"a": "b"},
				sharedEncryptOptions: []datav1alpha1.EncryptOption{{
					Name: CacheFSMetaUrl,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test-mirror-buckets",
						Key:  "access-key",
					}}}, {
					Name: CacheFSAccessKey,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test-mirror-buckets",
						Key:  "secret-key",
					}}}, {
					Name: CacheFSSecretKey,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test-mirror-buckets",
						Key:  "metaurl",
					}}},
				},
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///test",
					Options:    map[string]string{"a": "c"},
					Name:       "test-mirror-buckets",
					EncryptOptions: []datav1alpha1.EncryptOption{{
						Name: CacheFSMetaUrl,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test-mirror-buckets",
							Key:  "metaurl",
						}}}, {
						Name: CacheFSAccessKey,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test-mirror-buckets",
							Key:  "access-key",
						}}}, {
						Name: CacheFSSecretKey,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test-mirror-buckets",
							Key:  "secret-key",
						}}},
					},
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: "SSD",
					Path:       "/dev",
				},
				value: &CacheFS{
					FullnameOverride: "test-mirror-buckets",
					Fuse:             Fuse{},
					Worker:           Worker{},
				},
			},
			wantErr: false,
			wantOptions: map[string]string{
				"a": "c",
			},
		}, {
			name: "test-shared-mirror-buckets",
			fields: fields{
				name:        "test-mirror-buckets",
				namespace:   "fluid",
				runtimeType: common.CacheFSRuntime,
			},
			args: args{
				sharedOptions: map[string]string{"a": "b"},
				sharedEncryptOptions: []datav1alpha1.EncryptOption{{
					Name: CacheFSMetaUrl,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test-mirror-buckets",
						Key:  "access-key",
					}}}, {
					Name: CacheFSAccessKey,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test-mirror-buckets",
						Key:  "secret-key",
					}}}, {
					Name: CacheFSSecretKey,
					ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
						Name: "test-mirror-buckets",
						Key:  "metaurl",
					}}},
				},
				mount: datav1alpha1.Mount{
					MountPoint: "cachefs:///test",
					Options:    map[string]string{"a": "c"},
					Name:       "test-mirror-buckets",
					EncryptOptions: []datav1alpha1.EncryptOption{{
						Name: CacheFSMetaUrl,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test-mirror-buckets",
							Key:  "metaurl",
						}}}, {
						Name: CacheFSAccessKey,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test-mirror-buckets",
							Key:  "access-key",
						}}}, {
						Name: CacheFSSecretKey,
						ValueFrom: datav1alpha1.EncryptOptionSource{SecretKeyRef: datav1alpha1.SecretKeySelector{
							Name: "test-mirror-buckets",
							Key:  "secret-key",
						}}},
					},
				},
				tiredStoreLevel: &datav1alpha1.Level{
					MediumType: "SSD",
					Path:       "/dev",
				},
				value: &CacheFS{
					FullnameOverride: "test-mirror-buckets",
					Fuse:             Fuse{},
					Worker:           Worker{},
				},
			},
			wantErr: false,
			wantOptions: map[string]string{
				"a": "c",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt, err := engine.genValue(tt.args.mount, tt.args.tiredStoreLevel, tt.args.value, tt.args.sharedOptions, tt.args.sharedEncryptOptions)
			if (err != nil) != tt.wantErr {
				t.Errorf("testcase %s: genValue() error = %v, wantErr %v", tt.name, err, tt.wantErr)
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
						Name:            "test",
						AccessKeySecret: "test",
						SecretKeySecret: "test",
						Bucket:          "http://127.0.0.1:9000/minio/test",
						MetaUrlSecret:   "test",
						Storage:         "minio",
					},
					Fuse: Fuse{
						SubPath:       "/",
						MountPath:     "/test",
						HostMountPath: "/test",
					},
					Worker: Worker{
						MountPath: "/test-worker",
					},
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
						Name:            "test",
						AccessKeySecret: "test",
						SecretKeySecret: "test",
						Bucket:          "http://127.0.0.1:9000/minio/test",
						MetaUrlSecret:   "test",
						Storage:         "minio",
					},
					Fuse: Fuse{
						SubPath:       "/",
						MountPath:     "/test",
						HostMountPath: "/test",
					},
					Worker: Worker{
						MountPath: "/test-worker",
					},
				},
				options: map[string]string{"verbose": ""},
			},
			wantErr:         false,
			wantFuseCommand: "/bin/mount.cachefs redis://127.0.0.1:6379 /test -o verbose,cache-group=fluid-test,no-sharing,metrics=0.0.0.0:9567,no-bgjob,backup-meta=0",
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
			if err := c.genFuseMount(tt.args.value, tt.args.options, false); (err != nil) != tt.wantErr {
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
		value   *CacheFS
		options map[string]string
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
						Name:            "test",
						AccessKeySecret: "test",
						SecretKeySecret: "test",
						Bucket:          "http://127.0.0.1:9000/minio/test",
						MetaUrlSecret:   "test",
						Storage:         "minio",
					},
					Fuse: Fuse{
						SubPath:       "/",
						MountPath:     "/test",
						HostMountPath: "/test",
					},
				}, options: map[string]string{},
			},
			wantFormatCmd: "/bin/cachefs format --access-key=${ACCESS_KEY} --secret-key=${SECRET_KEY} --storage=minio --bucket=http://127.0.0.1:9000/minio/test redis://127.0.0.1:6379 test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CacheFSEngine{
				runtime: &datav1alpha1.CacheFSRuntime{
					Spec: datav1alpha1.CacheFSRuntimeSpec{},
				},
			}
			c.genFormatCmd(tt.args.value, c.runtime.Spec.Configs, tt.args.options)
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

func TestParseImageTag(t *testing.T) {
	type args struct {
		imageTag string
	}
	tests := []struct {
		name    string
		args    args
		want    *ClientVersion
		want1   *ClientVersion
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				imageTag: "v1.0.4-4.9.0",
			},
			want: &ClientVersion{
				Major: 1,
				Minor: 0,
				Patch: 4,
				Tag:   "",
			},
			want1: &ClientVersion{
				Major: 4,
				Minor: 9,
				Patch: 0,
				Tag:   "",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := ParseImageTag(tt.args.imageTag)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseImageTag() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseImageTag() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseImageTag() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
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
			name: "test1",
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
