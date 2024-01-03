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

	"github.com/brahma-adshonor/gohook"
	"github.com/fluid-cloudnative/fluid/pkg/utils/fake"

	"github.com/fluid-cloudnative/fluid/pkg/ddc/cachefs/operations"
)

func mockCacheFSMetric() string {
	return `cachefs_blockcache_blocks 9708
cachefs_blockcache_bytes 40757435762
cachefs_blockcache_drops 0
cachefs_blockcache_evicts 0
cachefs_blockcache_hit_bytes 40717671794
cachefs_blockcache_hits 9708
cachefs_blockcache_miss 0
cachefs_blockcache_miss_bytes 0
cachefs_blockcache_read_hist_seconds_total 9709
cachefs_blockcache_read_hist_seconds_sum 973.9802880359989
cachefs_blockcache_write_bytes 0
cachefs_blockcache_write_hist_seconds_total 0
cachefs_blockcache_write_hist_seconds_sum 0
cachefs_blockcache_writes 0
cachefs_compact_size_histogram_bytes_total 0
cachefs_compact_size_histogram_bytes_sum 0
cachefs_cpu_usage 101.787289
cachefs_fuse_open_handlers 1
cachefs_fuse_ops_durations_histogram_seconds_total 395082
cachefs_fuse_ops_durations_histogram_seconds_sum 438.98124123499315
cachefs_fuse_read_size_bytes_total 310652
cachefs_fuse_read_size_bytes_sum 40717671794
cachefs_fuse_written_size_bytes_total 0
cachefs_fuse_written_size_bytes_sum 0
cachefs_go_goroutines 46
cachefs_go_info_go1.16.15 1
cachefs_go_memstats_alloc_bytes 20049512
cachefs_go_memstats_alloc_bytes_total 373878120
cachefs_go_memstats_buck_hash_sys_bytes 1481768
cachefs_go_memstats_frees_total 6271987
cachefs_go_memstats_gc_cpu_fraction 0.000018793731834382145
cachefs_go_memstats_gc_sys_bytes 11826520
cachefs_go_memstats_heap_alloc_bytes 20049512
cachefs_go_memstats_heap_idle_bytes 174145536
cachefs_go_memstats_heap_inuse_bytes 25149440
cachefs_go_memstats_heap_objects 43126
cachefs_go_memstats_heap_released_bytes 171704320
cachefs_go_memstats_heap_sys_bytes 199294976
cachefs_go_memstats_last_gc_time_seconds 1651914570.9444923
cachefs_go_memstats_lookups_total 0
cachefs_go_memstats_mallocs_total 6315113
cachefs_go_memstats_mcache_inuse_bytes 16800
cachefs_go_memstats_mcache_sys_bytes 32768
cachefs_go_memstats_mspan_inuse_bytes 320416
cachefs_go_memstats_mspan_sys_bytes 1277952
cachefs_go_memstats_next_gc_bytes 40455344
cachefs_go_memstats_other_sys_bytes 3066536
cachefs_go_memstats_stack_inuse_bytes 2031616
cachefs_go_memstats_stack_sys_bytes 2031616
cachefs_go_memstats_sys_bytes 219012136
cachefs_go_threads 31
cachefs_memory 82145280
cachefs_meta_ops_durations_histogram_seconds_total 85488
cachefs_meta_ops_durations_histogram_seconds_sum 26.298036121000194
cachefs_object_request_errors 0
cachefs_process_cpu_seconds_total 101.77
cachefs_process_max_fds 1048576
cachefs_process_open_fds 14
cachefs_process_resident_memory_bytes 82145280
cachefs_process_start_time_seconds 1651910490.62
cachefs_process_virtual_memory_bytes 3328643072
cachefs_process_virtual_memory_max_bytes -1
cachefs_staging_block_bytes 0
cachefs_staging_blocks 0
cachefs_store_cache_size_bytes 0
cachefs_transaction_durations_histogram_seconds_total 138
cachefs_transaction_durations_histogram_seconds_sum 0.17865633699999994
cachefs_transaction_restart 0
cachefs_uptime 4096.290097335
cachefs_used_buffer_size_bytes 0
cachefs_used_inodes 1
cachefs_used_space 40717672448`
}

func TestCacheFSEngine_parseMetric(t *testing.T) {
	type args struct {
		metrics string
	}
	tests := []struct {
		name          string
		args          args
		wantPodMetric fuseMetrics
	}{
		{
			name: "test",
			args: args{
				metrics: mockCacheFSMetric(),
			},
			wantPodMetric: fuseMetrics{
				blockCacheBytes:     40757435762,
				blockCacheHits:      9708,
				blockCacheMiss:      0,
				blockCacheHitsBytes: 40717671794,
				blockCacheMissBytes: 0,
				usedSpace:           0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := CacheFSEngine{}
			if gotPodMetric := j.parseMetric(tt.args.metrics); !reflect.DeepEqual(gotPodMetric, tt.wantPodMetric) {
				t.Errorf("name = %s, parseMetric() = %v, want %v", tt.name, gotPodMetric, tt.wantPodMetric)
			}
		})
	}
}

func TestCacheFSEngine_getPodMetrics(t *testing.T) {
	GetMetricCommon := func(a operations.CacheFileUtils, cachefsPath string) (metric string, err error) {
		return mockCacheFSMetric(), nil
	}
	err := gohook.Hook(operations.CacheFileUtils.GetMetric, GetMetricCommon, nil)
	if err != nil {
		t.Fatal(err.Error())
	}
	j := CacheFSEngine{
		Log: fake.NullLogger(),
	}

	gotMetrics, err := j.GetPodMetrics("test", "test")
	if err != nil {
		t.Errorf("getPodMetrics() error = %v", err)
		return
	}
	if gotMetrics != mockCacheFSMetric() {
		t.Errorf("getPodMetrics() gotMetrics = %v, want %v", gotMetrics, mockCacheFSMetric())
	}
}
