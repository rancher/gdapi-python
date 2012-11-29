#!/usr/bin/env python

import requests
import collections
import hashlib
import os
import json
import time

CACHE_DIR = "~/.gdapi"

LIST = "list-"
CREATE = "create-"
UPDATE = "update-"
DELETE = "delete-"
ACTION = "action-"
COMMAND_TYPES = [ LIST, CREATE, UPDATE, DELETE, ACTION ]

GET_METHOD = "GET"
POST_METHOD = "POST"
PUT_METHOD = "PUT"
DELETE_METHOD = "DELETE"

class RestObject:
  def __str__(self):
    if not hasattr(self, "type"):
      return str(self.__dict__)
    data = [("Type", "Id", "Name", "Value")]
    for k, v in self.iteritems():
      if k not in [ "links", "actions", "id", "type"] and not callable(v):
        if v is None:
          v = "null"
        if v is True:
          v = "true"
        if v is False:
          v = "false"
        data.append((self.type, self.id, str(k), str(v)))

    return indent(data, hasHeader=True, prefix='| ', postfix=' |')

  def __repr__(self):
    return repr(self.__dict__)

  def __iter__(self):
    return iter(self.__dict__)

  def __getitem__(self, i):
    return getattr(self, i)

  def __setitem__(self, i, v):
    return setattr(self, i, v)

  def iteritems(self):
    return self.__dict__.iteritems()


class Schema:
  def __init__(self, text, obj):
    self.text = text
    self.types = {}
    for t in obj:
      self.types[t.id] = t
      for old_name in [ "methods", "actions", "fields" ]:
        if hasattr(t, old_name):
          t.__dict__["resource" + old_name.capitalize()] = t.__dict__[old_name]

      t.creatable = False
      try:
        if POST_METHOD in t.collectionMethods:
          t.creatable = True
      except:
        pass

      t.updatable = False
      try:
        if PUT_METHOD in t.resourceMethods:
          t.updatable = True
      except:
        pass

      t.deletable = False
      try:
        if DELETE_METHOD in t.resourceMethods:
          t.deletable = True
      except:
        pass

      t.listable = False
      try:
        if GET_METHOD in t.collectionMethods:
          t.listable = True
      except:
        pass

  def __str__(self):
    return str(self.text)

  def __repr(self):
    return repr(self.text)


class ApiError(Exception):
  def __init__(self, obj):
    self.error = obj
    try:
      super(ApiError, self).__init__(self, obj.message)
    except:
      super(ApiError, self).__init__(self, "API Error")


class Client:
  def __init__(self, accesskey=None, secretkey=None, url=None, cache=None, cachetime=86400):
    self._accesskey = accesskey
    self._secretkey = secretkey
    self._auth = ( self._accesskey, self._secretkey )
    self._url = url
    self._cache = cache
    self._cachetime = cachetime
    self.schema = None

    if not self._cachetime:
      self._cachetime = 60 * 60 * 24 # 24 Hours

    if self.valid():
      self._load_schemas()

  def valid(self):
    return self._accesskey != None and self._secretkey != None and self._url != None

  def object_hook(self, obj):
    if isinstance(obj, list):
      return [ self.object_hook(x) for x in obj ]

    if isinstance(obj, dict):
      result = RestObject()

      for k, v in obj.iteritems():
        setattr(result, k, self.object_hook(v))

      if hasattr(result, "data") and hasattr(result, "type") and result.type == "collection":
        return result.data

      if hasattr(result, "links"):
        for link_name, link in result.links.iteritems():
          cb = lambda link=link: lambda **kw: self._get(link,data=kw)
          setattr(result, link_name, cb())

      if hasattr(result, "actions"):
        for link_name, link in result.actions.iteritems():
          cb = lambda link_name=link_name, result=result: lambda *args, **kw: self.action(result, link_name, *args, **kw)
          setattr(result, link_name, cb())
      return result

    return obj

  def object_pairs_hook(self,pairs):
    ret = collections.OrderedDict()
    for k, v in pairs:
      ret[k] = v
    return self.object_hook(ret)

  def _get(self, url, data=None):
    return self._unmarshall(self._get_raw(url, data=data))

  def _error(self, text):
    raise ApiError(self._unmarshall(text))

  def _get_raw(self, url, data=None):
    #start = time.time()
    r = requests.get(url, auth=self._auth, params=data)
    #delta = time.time() - start
    #print delta, " seconds", url
    if r.status_code < 200 or r.status_code >= 300:
      self._error(r.text)

    return r.text
  
  def _post(self, url, data=None):
    #start = time.time()
    r = requests.post(url, auth=self._auth, data=self._marshall(data))
    #delta = time.time() - start
    #print delta, " seconds", url
    if r.status_code < 200 or r.status_code >= 300:
      self._error(r.text)

    return self._unmarshall(r.text)

  def _put(self, url, data=None):
    #start = time.time()
    r = requests.put(url, auth=self._auth, data=self._marshall(data))
    #delta = time.time() - start
    #print delta, " seconds", url
    if r.status_code < 200 or r.status_code >= 300:
      self._error(r.text)

    return self._unmarshall(r.text)

  def _delete(self, url):
    #start = time.time()
    r = requests.delete(url, auth=self._auth)
    #delta = time.time() - start
    #print delta, " seconds", url
    if r.status_code < 200 or r.status_code >= 300:
      self._error(r.text)

    return self._unmarshall(r.text)

  def _unmarshall(self, text):
    return json.loads(text, object_pairs_hook=self.object_pairs_hook)

  def _marshall(self, obj):
    if obj is None:
      return None
    return json.dumps(self._to_dict(obj))

  def _load_schemas(self):
    if self.schema:
      return

    schematext = self._get_cached_schema()

    if not schematext:
      schematext = self._get_raw(self._url)
      self._cache_schema(schematext)

    schema = Schema(schematext, self._unmarshall(schematext))

    self._bind_methods(schema)
    self.schema = schema

  def by_id(self, type, id):
    url = self.schema.types[type].links.collection
    if url.endswith("/"):
      url = url + id
    else:
      url = "/".join([url, id])
    return self._get(url)

  def update_by_id(self, type, id, *args, **kw):
    url = self.schema.types[type].links.collection
    if url.endswith("/"):
      url = url + id
    else:
      url = "/".join([url, id])

    return self._put(url, data=self._to_dict(*args, **kw))

  def update(self, obj, *args, **kw):
    url = obj.links.self

    for k, v in self._to_dict(*args, **kw):
      setattr(obj, k, v)

    return self._put(url, data=obj)

  def list(self, type, **kw):
    collection_url = self.schema.types[type].links.collection
    return self._get(collection_url, data=kw)

  def create(self, type, *args, **kw):
    collection_url = self.schema.types[type].links.collection
    return self._post(collection_url, data=self._to_dict(*args,**kw))

  def delete(self, *args):
    for i in args:
      if isinstance(i, RestObject):
        self._delete(i.links.self)

  def action(self, obj, name, *args, **kw):
    url = obj.actions[name]
    return self._post(url, data=self._to_dict(*args,**kw))

  def _to_dict(self, *args, **kw):
    ret = {}

    for i in args:
      if isinstance(i, dict):
        for k, v in i.iteritems():
          ret[k] = v

      if isinstance(i, RestObject):
        for k,v  in i.__dict__.iteritems():
          if not k.startswith("_") and not isinstance(v, RestObject) and not callable(v):
            ret[k] = v

    for k, v in kw.iteritems():
      ret[k] = v

    return ret
  
  def _bind_methods(self, schema):
    bindings = [ 
        ("list", "collectionMethods", GET_METHOD, self.list),
        ("by_id", "collectionMethods", GET_METHOD, self.by_id),
        ("update_by_id", "resourceMethods", PUT_METHOD, self.update_by_id),
        ("create", "collectionMethods", POST_METHOD, self.create)
      ]

    for type_name, type in schema.types.iteritems():
      for method_name, type_collection, test_method, m in bindings:
        # double lambda for lexical binding hack
        cb = lambda type_name=type_name, method=m: lambda *args,**kw: method(type_name, *args, **kw)
        if test_method in type[type_collection]:
          setattr(self, "_".join([method_name, type_name]), cb())

  def _get_schema_hash(self):
    h = hashlib.new("sha1")
    h.update(self._url)
    h.update(self._accesskey)
    return h.hexdigest()

  def _get_cached_schema_file_name(self):
    if not self._cache:
      return None

    h = self._get_schema_hash()

    cachedir = os.path.expanduser(CACHE_DIR)
    if not cachedir:
      return None

    if not os.path.exists(cachedir):
      os.mkdir(cachedir)

    return os.path.join(cachedir, "schema-" + h + ".json")

  def _cache_schema(self, text):
    cachedschema = self._get_cached_schema_file_name()

    if not cachedschema:
      return None

    with open(cachedschema, "w") as f:
      f.write(text)

  def _get_cached_schema(self):
    cachedschema = self._get_cached_schema_file_name()

    if not cachedschema:
      return None

    if os.path.exists(cachedschema):
      modtime = os.path.getmtime(cachedschema)
      if time.time() - modtime < self._cachetime:
        with open(cachedschema) as f:
          data = f.read()
        return data

    return None

  def _all_commands(self, lst=True, update=True, remove=True, create=True):
    for type, schema in self.schema.types.iteritems():
      if schema.listable:
        yield LIST + type
      if schema.creatable:
        yield CREATE + type
      if schema.updatable:
        yield UPDATE + type
      if schema.deletable:
        yield DELETE + type

      if hasattr(schema, "resourceActions"):
        for k in schema.resourceActions:
          yield ACTION + "-".join([type, k]) 

  def _find_match_command(self, text):
    return _find_match(text, self._all_commands());

  def _decompose_command(self, command):
    if not command or command not in self._all_commands():
      return (None, None, None)

    command_type = filter(lambda x: command and command.startswith(x), COMMAND_TYPES)
    if not len(command_type):
      return (None, None, None)

    command_type = command_type[0]
    type_name = command[len(command_type):]
    action_name = None

    if command_type == ACTION:
      idx = type_name.find("-")
      if idx != -1:
        action_name = type_name[idx+1:]
        type_name = type_name[:idx]

    return (command_type, type_name, action_name)

  def _possible_args(self, command_type, type_name, action_name):
    result = []
    type_def = self.schema.types[type_name]
    
    if command_type == ACTION: 
      result.append("id")
      action_def = type_def.resourceActions[action_name]
      if hasattr(action_def, "input"):
        type_def = self.schema.types[action_def.input]

    if command_type == LIST and type_def.listable:
      try:
        for name, filter in type_def.collectionFilters.iteritems():
          result.append(name)
          for m in filter.modifiers:
            if m != "eq":
              result.append(name + "_" + m)
      except:
        pass

    try:
      for name, field in type_def.resourceFields.iteritems():
        if ( ( command_type == CREATE and type_def.creatable ) or command_type == ACTION ) and hasattr(field, "create") and field.create:
          result.append(name)
        if command_type == UPDATE and type_def.updatable and hasattr(field, "update") and field.update:
          result.append(name)
    except:
      pass

    if command_type == DELETE and type_def.deletable:
      result.append("id")

    if command_type == UPDATE and type_def.updatable:
      result.append("id")

    return result

  def _is_list_type(self, type_name, field_name):
    try:
      return self.schema.types[type_name].resourceFields[field_name].type.startswith("array")
    except:
      return False

  def _find_match_args(self, command, args, key, index):
    command_type, type_name, action_name = self._decompose_command(command)
    if not command_type:
      return []

    if index is None:
      possible_args = map(lambda x: x.lower(), self._possible_args(command_type, type_name, action_name))

      for arg in args.keys():
        if arg != key and not self._is_list_type(type_name, arg) and arg in possible_args:
          possible_args.remove(arg)

      return _find_match(key, possible_args)
    else:
      match = _find_match(key, self._possible_args(command_type, type_name, action_name))
      if len(match) == 1 or key in match:
        if key in match:
          match_val = key
        else:
          match_val = match[0]

        val = args[key]
        matches = None
        if len(val):
          val = val[-1]
        else:
          val = None

        try:
          if command_type == ACTION:
            action_def = self.schema.types[type_name].resourceActions[action_name].input
            field = self.schema.types[action_def].resourceFields[match_val]
          else:
            field = self.schema.types[type_name].resourceFields[match_val]
        except:
          # The field may not exist
          return []

        if ( field.type.startswith("reference") or field.type.startswith("array[reference") ) and hasattr(field, "referenceCollection"):
          matches = [ x.id for x in self._get(field.referenceCollection) ]

        if field.type == "enum":
          matches = field.options

        if key == "id":
          matches = [ x.id for x in self.list(type_name) ]

        if matches is None:
          matches = []
          for obj in self.list(type_name):
            if hasattr(obj, key):
              matches.append(obj[key])

        if val is None:
          return matches
        else:
          return _find_match(val, matches)

      return []

  def complete(self, index, words):
    if index == 0:
      return self._find_match_command(words[0])

    command = words[0]
    opts, args, key, index = _parse_args(words[1:], index - 1)
    matches = self._find_match_args(command, args, key, index)
    if index is None:
      return map(lambda x: ("--" + x).lower() + "=", matches)
    else:
      return matches

  def _run(self, cmd, args):
    if cmd not in self._all_commands():
      return

    command_type, type_name, action_name = self._decompose_command(cmd) 
    possible_args_map = {}
    for i in self._possible_args(command_type, type_name, action_name):
      possible_args_map[i.lower()] = i;
    new_args = {}

    for k, v in args.iteritems():
      k_l = k.lower();
      if possible_args_map.has_key(k_l):
        k = possible_args_map[k_l]

      if self._is_list_type(type_name, k):
        new_args[k] = v
      else:
        if len(v):
          new_args[k] = v[0]
        else:
          new_args[k] = None

    #print command_type, type_name, possible_args_map, new_args
    #print "\n\n"

    if command_type == LIST:
      for i in self.list(type_name, **new_args):
        print i

    if command_type == CREATE:
      print self.create(type_name, **new_args)

    if command_type == DELETE:
      obj = self.by_id(type_name, new_args["id"])
      if obj:
        self.delete(obj)
        print obj

    if command_type == UPDATE:
      print self.update_by_id(type_name, new_args["id"], new_args)


    if command_type == ACTION:
      obj = self.by_id(type_name, new_args["id"])
      if obj:
        print self.action(obj, action_name, new_args)




## {{{ http://code.activestate.com/recipes/267662/ (r7)
import cStringIO,operator

def indent(rows, hasHeader=False, headerChar='-', delim=' | ', justify='left',
           separateRows=False, prefix='', postfix='', wrapfunc=lambda x:x):
    """Indents a table by column.
       - rows: A sequence of sequences of items, one sequence per row.
       - hasHeader: True if the first row consists of the columns' names.
       - headerChar: Character to be used for the row separator line
         (if hasHeader==True or separateRows==True).
       - delim: The column delimiter.
       - justify: Determines how are data justified in their column. 
         Valid values are 'left','right' and 'center'.
       - separateRows: True if rows are to be separated by a line
         of 'headerChar's.
       - prefix: A string prepended to each printed row.
       - postfix: A string appended to each printed row.
       - wrapfunc: A function f(text) for wrapping text; each element in
         the table is first wrapped by this function."""
    # closure for breaking logical rows to physical, using wrapfunc
    def rowWrapper(row):
        newRows = [wrapfunc(item).split('\n') for item in row]
        return [[substr or '' for substr in item] for item in map(None,*newRows)]
    # break each logical row into one or more physical ones
    logicalRows = [rowWrapper(row) for row in rows]
    # columns of physical rows
    columns = map(None,*reduce(operator.add,logicalRows))
    # get the maximum of each column by the string length of its items
    maxWidths = [max([len(str(item)) for item in column]) for column in columns]
    rowSeparator = headerChar * (len(prefix) + len(postfix) + sum(maxWidths) + \
                                 len(delim)*(len(maxWidths)-1))
    # select the appropriate justify method
    justify = {'center':str.center, 'right':str.rjust, 'left':str.ljust}[justify.lower()]
    output=cStringIO.StringIO()
    if separateRows: print >> output, rowSeparator
    for physicalRows in logicalRows:
        for row in physicalRows:
            print >> output, \
                prefix \
                + delim.join([justify(str(item),width) for (item,width) in zip(row,maxWidths)]) \
                + postfix
        if separateRows or hasHeader: print >> output, rowSeparator; hasHeader=False
    return output.getvalue()
#End ## {{{ http://code.activestate.com/recipes/267662/ (r7)
    
def _find_match(text, strings):
  result = [];
  if not text or len(text.strip()) == 0:
    return strings

  for test in strings:
    if test.lower().startswith(text.lower()):
      result.append(test)

  return result


def _parse_args(words, index):
  found_break = False
  opts = {}
  args = {}
  cmap = opts
  key = None
  cindex = 0
  result_index = None
  result_key = None
  for word in words:
    if word == "--" and cindex != index:
      found_break = True
      cmap = args
    if word.startswith("--"):
      key = word[2:]
      val = None
      idx = key.find("=")
      if idx != -1:
        val = key[idx+1:]
        key = key[:idx]
      key = key.lower()
      if cindex == index:
        result_key = key
      if not key in cmap:
        cmap[key] = []
      if val != None and len(val) > 0:
        cmap[key].append(val)
        result_index = len(cmap[key]) - 1
    elif key:
      if cindex == index:
        result_key = key
        result_index = len(cmap[key]) - 1
        if result_index < 0:
          result_index = 0
      if word != "=":
        cmap[key].append(word)
        key = None

    cindex = cindex + 1

  if not found_break:
    args = opts

  return (opts, args, result_key, result_index)


def _parse_client_options(cmd, words, index = None):
  keys = [ "access-key", "secret-key", "url", "cache", "cache-time" ]
  opts, args, key, index = _parse_args(words, index)
  parsed_opts = {}
  to_remove = []

  env_prefix = os.path.basename(cmd.replace("-","_"))
  for i in [ ".py", "-cli", "-tool", "-util" ]:
    env_prefix = env_prefix.replace(i, "")
  env_prefix = env_prefix.upper()

  for long_key in keys:
    short_key = long_key.replace("-","")
    added = False
    for i in [ short_key, long_key ]:
      if opts.has_key(i) and not parsed_opts.has_key(short_key):
        added = True
        to_remove.append(i)
        parsed_opts[short_key] = opts[i][0]

    if not added:
      env_suffix = long_key.replace("-","_").upper()
      env_key = env_prefix + env_suffix
      for env_key in [ env_prefix + env_suffix, "GDAPI_" + env_suffix ]:
        if os.environ.has_key(env_key):
          parsed_opts[short_key] = os.environ[env_key]
          break


  for i in to_remove:
    del opts[i]

  if parsed_opts.has_key("cache"):
    parsed_opts["cache"] = parsed_opts["cache"].lower() in ['true', '1', 't', 'y', 'yes']

  if parsed_opts.has_key("cachetime"):
    parsed_opts["cachetime"] = int(parsed_opts["cachetime"])

  return (parsed_opts, args, key, index)
  

def _run_completion(argv):
    index = int(argv[2]) - 1
    words = argv[4:]
    opts = _parse_client_options(argv[3], words)[0]
    if not opts.has_key("cache"):
      opts["cache"] = True
    #try:
    client = Client(**opts)
    if client.valid():
      for word in client.complete(index, words):
        print word


def _run_client(argv):
    cmd = argv[1]
    opts, args, key, index = _parse_client_options(argv[0], argv[1:])
    #print cmd, opts, args, key, index
    client = Client(**opts)

    if client.valid():
      try:
        client._run(cmd, args)
      except ApiError, e:
        try:
          print e.error
        except:
          print "Error", e
        sys.exit(1)
    else:
      print "Must specify accesskey, secretkey, and url"

  
def _run_cli(opts = None):
  # This doesn't really work
  return
  if opts is None:
    opts = _parse_client_options("", [])[0]

  client = Client(**opts)
  if client.valid():
    import code
    import rlcompleter
    import readline
    readline.parse_and_bind("tab: complete")
    console = code.InteractiveConsole(locals = { "client" : client})
    console.interact()
  
if __name__ == '__main__':
  import sys
  if len(sys.argv) > 1:
    if sys.argv[1] == "_complete":
      _run_completion(sys.argv)
    else:
      _run_client(sys.argv)
  else:
    _run_cli()
