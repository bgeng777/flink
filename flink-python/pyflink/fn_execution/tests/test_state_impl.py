################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.fn_execution.coders import CharCoder, IntCoder
from pyflink.fn_execution.state_impl import InternalSynchronousMapRuntimeState
from pyflink.testing.test_case_utils import PyFlinkTestCase


class _EmptyMapStateHandler(object):
    """Remote map state with no entries. Cache misses fall through to this handler."""

    def blocking_get(self, state_key, map_key, map_key_encoder, map_value_decoder):
        return False, None


class InternalSynchronousMapRuntimeStateTests(PyFlinkTestCase):
    """Tests for process-mode map state, including MapState.put_all (FLINK-41106)."""

    def _new_map_state(self, key_coder, value_coder):
        # A large write-cache limit keeps put_all in memory. The handler only
        # answers cache misses, which are absent keys.
        return InternalSynchronousMapRuntimeState(
            _EmptyMapStateHandler(), b'map-state', key_coder, value_coder, 1000)

    def test_put_all_integer_keys(self):
        state = self._new_map_state(IntCoder(), IntCoder())

        state.put_all({2: 200, 3: 300})

        self.assertEqual(200, state.get(2))
        self.assertEqual(300, state.get(3))
        self.assertIsNone(state.get(1))

    def test_put_all_multi_character_string_keys(self):
        state = self._new_map_state(CharCoder(), CharCoder())

        state.put_all({'ab': 'wanted', 'cd': 'also'})

        self.assertEqual('wanted', state.get('ab'))
        self.assertEqual('also', state.get('cd'))
        # The key must not be unpacked into characters, and the value must be kept.
        self.assertIsNone(state.get('a'))
        self.assertIsNone(state.get('b'))
        self.assertIsNone(state.get('c'))
