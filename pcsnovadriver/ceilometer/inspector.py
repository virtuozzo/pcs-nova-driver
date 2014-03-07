# Copyright (c) 2013-2014 Parallels, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from ceilometer.compute.virt import inspector as virt_inspector
import netsnmp

ROOT = '.1.3.6.1.4.1.26171.1.1'
KEYS = {
    '55': {
        '15': 'uuid',
        '2': 'name',
        '9': 'cpu_number',
        '12': 'cpu_system',
        '13': 'cpu_user'},
    '56': {
        '1': 'name',
        '4': 'read_requests',
        '5': 'write_requests',
        '6': 'read_bytes',
        '7': 'write_bytes'},
    '57': {
        '1': 'name',
        '2': 'in_bytes',
        '3': 'out_bytes',
        '4': 'in_packets',
        '5': 'out_packets',
        '6': 'mac'}}


class Dao:
    def __init__(self):
        self.m_session = netsnmp.Session(Version=2,
                                         DestHost='localhost',
                                         Community='public',
                                         UseNumeric=1)
        self.m_session.UseLongNames = 1

    @staticmethod
    def _parse(response_):
        o = '.'.join([response_.tag.split(ROOT + '.', 1)[1], response_.iid])
        t, _, c, r = o.split('.', 3)
        x = r.split('.')
        return t, c, ''.join(chr(int(x[i + 1])) for i in xrange(0, int(x[0])))

    def query_table_range(self, table_, range_):
        output = []
        o = '.'.join([ROOT, str(table_)])
        q = netsnmp.VarList(netsnmp.Varbind(o))
        self.m_session.walk(q)
        for v in q:
            t, c, u = self._parse(v)
            if not(range_ == u and c in KEYS[t]):
                continue
            k = KEYS[t][c]
            if 0 == len(output) or k in output[-1]:
                output.append(dict())
            output[-1][k] = v.val

        return output

    def query_table_column(self, table_, column_, dst_):
        o = '.'.join([ROOT, str(table_), '1', str(column_)])
        q = netsnmp.VarList(netsnmp.Varbind(o))
        self.m_session.walk(q)
        for v in q:
            t, c, u = self._parse(v)
            dst_.setdefault(u, dict())[KEYS[t][c]] = v.val

    def query_table_cell(self, table_, column_, name_, dst_):
        n = '.'.join(map(str, [len(name_)] + list(bytearray(name_, 'utf-8'))))
        o = '.'.join([ROOT, str(table_), '1', str(column_), n])
        q = netsnmp.VarList(netsnmp.Varbind(o))
        self.m_session.get(q)
        for v in q:
            t, c, _ = self._parse(v)
            dst_[KEYS[t][c]] = v.val


class Ve:
    def __init__(self, dao_, name_):
        d = dict()
        dao_.query_table_column(55, 2, d)
        self.m_dao = dao_
        self.m_veid = next(k for k in d.iterkeys() if d[k]['name'] == name_)

    def get_cpus(self):
        output = dict()
        self.m_dao.query_table_cell(55, 9, self.m_veid, output)
        self.m_dao.query_table_cell(55, 12, self.m_veid, output)
        self.m_dao.query_table_cell(55, 13, self.m_veid, output)
        return output

    def get_disks(self):
        return self.m_dao.query_table_range(56, self.m_veid)

    def get_vnics(self):
        return self.m_dao.query_table_range(57, self.m_veid)


class ParallelsInspector(virt_inspector.Inspector):
    def inspect_instances(self):
        d = dict()
        self.m_dao.query_table_column(55, 2, d)
        self.m_dao.query_table_column(55, 15, d)
        for x in d.values():
            yield virt_inspector.Instance(name=x['name'],
                                          UUID=x['uuid'])

    def inspect_cpus(self, instance_name_):
        d = Ve(self.m_dao, instance_name_).get_cpus()
        t = float(d['cpu_system']) + float(d['cpu_user'])
        return virt_inspector.CPUStats(number=int(d['cpu_number']), time=t)

    def inspect_vnics(self, instance_name_):
        for x in Ve(self.m_dao, instance_name_).get_vnics():
            a = virt_inspector.Interface(name=x['name'],
                                         mac=x['mac'],
                                         fref=None,
                                         parameters=None)
            b = virt_inspector.InterfaceStats(
                                           rx_bytes=long(x['in_bytes']),
                                           rx_packets=long(x['in_packets']),
                                           tx_bytes=long(x['out_bytes']),
                                           tx_packets=long(x['out_packets']))
            yield (a, b)

    def inspect_disks(self, instance_name_):
        for x in Ve(self.m_dao, instance_name_).get_disks():
            a = virt_inspector.Disk(device=x['name'])
            b = virt_inspector.InterfaceStats(
                                    read_requests=long(x['read_requests']),
                                    read_bytes=long(x['read_bytes']),
                                    write_requests=long(x['write_requests']),
                                    write_bytes=long(x['write_bytes']),
                                    errors=0)
            yield (a, b)

    def __init__(self):
        self.m_dao = Dao()
