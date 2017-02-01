
#!/usr/bin/python

# Copyright KOLIBERO under one or more contributor license agreements.  
# KOLIBERO licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import argparse
import hashlib
from couchbase.bucket import Bucket

def convert_save():
  print "convert_save"
  targetBucket = Bucket(args.couchbase)
  csv_rows = []
  counter = 0
  with open(args.jsonfile) as jsonfile:
    content = jsonfile.readlines()
    for line in content:
      counter += 1
      jrow = json.loads(line)
      key = str(counter)
      if args.key == "hash":
        hash_object = hashlib.sha1(json.dumps(jrow))
        key = str(hash_object.hexdigest())
      targetBucket.upsert(key=key,value=jrow)
      if counter % 1000 == 0:
        print counter      

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="JSON 2 Couchbase")
  parser.add_argument('--couchbase', default="couchbase://localhost/targetbucket")
  parser.add_argument('--jsonfile', default="source.json")
  parser.add_argument('--key', default="counter")

  args = parser.parse_args()
  print "couchbase:",args.couchbase
  print "source json:",args.jsonfile
  print "key:",args.key,"Possible: counter, hash"

  convert_save()
