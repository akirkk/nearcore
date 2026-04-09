"""
Test case classes for release tests on forknet.
"""
import copy

from .base import TestSetup, NodeHardware
from mirror import CommandContext, run_env_cmd


class DynamicResharding(TestSetup):

    def __init__(self, args):
        super().__init__(args)
        self.start_height = 180121999  # 2_10_release
        self.args.start_height = self.start_height
        self.node_hardware_config = NodeHardware.SameConfig(
            num_chunk_producer_seats=20, num_chunk_validator_seats=20)
        self.epoch_len = 20000  # ~3h
        self.has_state_dumper = False
        self.genesis_protocol_version = 84
        self.has_archival = True
        self.regions = "europe-west4,asia-east1,us-west1"
        self.neard_binary_url = "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore/Linux-x86_64/wiezzel/dynamic-resharding-test-v2/d47754e9888098aca99562c5ee161a8e70682a40/release/neard"

    def amend_configs_before_test_start(self):
        super().amend_configs_before_test_start()
        # Enable jemalloc heap profiling. Tracking is always on but dumps
        # are only written every ~4GiB of cumulative allocation
        env_args = copy.deepcopy(self.args)
        env_args.key_value = [
            "MALLOC_CONF=prof:true,lg_prof_interval:32,prof_prefix:/tmp/heap"
        ]
        env_args.clear_all = False
        run_env_cmd(CommandContext(env_args))
