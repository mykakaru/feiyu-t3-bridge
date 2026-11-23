import sys as _sys
from pathlib import Path as _Path
_BRIDGE_DIR = str(_Path(__file__).resolve().parent.parent / 'bridge')
if _BRIDGE_DIR not in _sys.path:
    _sys.path.insert(0, _BRIDGE_DIR)
import unittest,time,tempfile,json
from pathlib import Path
from datetime import datetime,timezone
from watchdog import classify,transition,atomic,read_json
class Test(unittest.TestCase):
 def test_classify(self):
  now=time.time();s={'state':'authenticated','updated_at':datetime.fromtimestamp(now,timezone.utc).isoformat()}
  self.assertEqual(classify(s,now),'healthy');self.assertEqual(classify(s,now+16),'dead_or_stale')
  s['state']='reconnecting';self.assertEqual(classify(s,now),'disconnected')
  self.assertEqual(classify(s,now,True),'paused')
  s['error_type']='WSAuthFailureError';self.assertEqual(classify(s,now),'blocked')
 def test_once_and_recovery(self):
  s={};self.assertEqual(transition(s,'dead_or_stale',100),[])
  a=transition(s,'dead_or_stale',121);self.assertEqual(a[0][0],'down')
  for n in range(122,200):self.assertEqual(transition(s,'disconnected',n),[])
  b=transition(s,'healthy',201);self.assertEqual(b,[('recovered',a[0][1])]);self.assertEqual(transition(s,'healthy',202),[])
 def test_brief_and_pause(self):
  s={};transition(s,'disconnected',100);self.assertEqual(transition(s,'healthy',105),[])
  transition(s,'paused',200);self.assertEqual(transition(s,'paused',300),[])
 def test_durable(self):
  p=Path(tempfile.mkdtemp())/'state.json';s={};transition(s,'disconnected',100);transition(s,'disconnected',121);atomic(p,s)
  r=read_json(p);self.assertEqual(transition(r,'disconnected',200),[]);self.assertEqual(transition(r,'healthy',201)[0][0],'recovered')
class PrecisionTest(unittest.TestCase):
 def test_micro_future_and_true_future(self):
  from watchdog import classify
  n=1794790000.0
  def state(offset):return {'state':'authenticated','updated_at':datetime.fromtimestamp(n+offset,timezone.utc).isoformat()}
  self.assertEqual(classify(state(0.000001),n),'healthy')
  self.assertEqual(classify(state(0.01),n),'dead_or_stale')
  self.assertEqual(classify(state(-15),n),'dead_or_stale')
  self.assertEqual(classify({},n),'dead_or_stale')
 def test_clock_sample_after_read(self):
  from unittest.mock import patch
  from watchdog import sample_state
  order=[]
  with patch('watchdog.read_json',side_effect=lambda p:order.append('read') or {}),patch('watchdog.time.time',side_effect=lambda:order.append('clock') or 1):
   self.assertEqual(sample_state(Path('.')),({},1))
  self.assertEqual(order,['read','clock'])
 def test_real_roundtrip_1000(self):
  from watchdog import classify
  for _ in range(1000):
   n=time.time();s={'state':'authenticated','updated_at':datetime.fromtimestamp(n,timezone.utc).isoformat()}
   self.assertEqual(classify(s,n),'healthy')
class StateWriteTest(unittest.TestCase):
 def test_transient_lock_and_persistent_heartbeat_failure(self):
  import errno,os
  from unittest.mock import patch
  from watchdog import heartbeat
  locked=PermissionError(errno.EACCES,'fixture file in use')
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/'watchdog-state.json';atomic(p,{'generation':1});actual=os.replace
   calls=[]
   def retry(a,b):
    calls.append(1)
    if len(calls)<3:raise locked
    actual(a,b)
   with patch('watchdog.os.replace',side_effect=retry),patch('watchdog.time.sleep'):
    atomic(p,{'generation':2})
   self.assertEqual(len(calls),3);self.assertEqual(read_json(p)['generation'],2)
   saved={'generation':3}
   with patch('watchdog.os.replace',side_effect=locked),patch('watchdog.time.sleep'):
    self.assertFalse(heartbeat(p,saved))
    with self.assertRaises(PermissionError):atomic(p,{'critical_receipt':True})
   self.assertEqual(read_json(p),{'generation':2})
   self.assertTrue(heartbeat(p,saved))
   self.assertEqual(read_json(p)['heartbeat_write_failures'],1)
   self.assertEqual([f.name for f in p.parent.iterdir()],[p.name])
if __name__ == '__main__':
 unittest.main()
