% Module conn_replica

<div class='index'>

* [Enum `ServerType`](#enum-servertype)
* [Struct `ReplicaSetConnection`](#struct-replicasetconnection)
* [Implementation ` of ::std::clone::Clone for ServerType`](#implementation-of-stdcloneclone-for-servertype) - Automatically derived.
* [Implementation ` of ::std::cmp::Eq for ServerType`](#implementation-of-stdcmpeq-for-servertype) - Automatically derived.
* [Implementation ` of Clone for ReplicaSetData`](#implementation-of-clone-for-replicasetdata)
* [Implementation ` for ReplicaSetData`](#implementation-for-replicasetdata)
* [Implementation ` of Connection for ReplicaSetConnection`](#implementation-of-connection-for-replicasetconnection)
* [Implementation ` of ::std::clone::Clone for NodeData`](#implementation-of-stdcloneclone-for-nodedata) - Automatically derived.
* [Implementation ` of Ord for NodeData`](#implementation-of-ord-for-nodedata)
* [Implementation ` of Eq for NodeData`](#implementation-of-eq-for-nodedata)
* [Implementation ` for NodeData`](#implementation-for-nodedata)
* [Implementation ` of Eq for ReplicaSetData`](#implementation-of-eq-for-replicasetdata)
* [Implementation ` for ReplicaSetConnection`](#implementation-for-replicasetconnection)

</div>

## Enum `ServerType`

#### Variants


* `PRIMARY = 0`

* `SECONDARY = 1`

* `OTHER = 2`

## Struct `ReplicaSetConnection`

~~~ {.rust}
pub struct ReplicaSetConnection {
    seed: Cell<~ARC<~[(~str, uint)]>>,
    state: Cell<ReplicaSetData>,
    write_to: Cell<NodeConnection>,
    read_from: Cell<NodeConnection>,
    priv port_state: Cell<Port<ReplicaSetData>>,
    priv chan_reconn: Cell<Chan<bool>>,
    read_pref: Cell<READ_PREFERENCE>,
    read_pref_changed: Cell<bool>,
    timeout: Cell<u64>,
}
~~~

## Implementation of `::std::clone::Clone` for `ServerType`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> ServerType
~~~

## Implementation of `::std::cmp::Eq` for `ServerType`

Automatically derived.

### Method `eq`

~~~ {.rust}
fn eq(&self, __arg_0: &ServerType) -> ::bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, __arg_0: &ServerType) -> ::bool
~~~

## Implementation of `Clone` for `ReplicaSetData`

### Method `clone`

~~~ {.rust}
fn clone(&self) -> ReplicaSetData
~~~

## Implementation for `ReplicaSetData`

### Method `new`

~~~ {.rust}
fn new(pri: Option<NodeData>, sec: PriorityQueue<NodeData>,
       err: Option<MongoErr>) -> ReplicaSetData
~~~

## Implementation of `Connection` for `ReplicaSetConnection`

### Method `connect`

~~~ {.rust}
fn connect(&self) -> Result<(), MongoErr>
~~~

### Method `disconnect`

~~~ {.rust}
fn disconnect(&self) -> Result<(), MongoErr>
~~~

### Method `reconnect`

~~~ {.rust}
fn reconnect(&self) -> Result<(), MongoErr>
~~~

### Method `send`

~~~ {.rust}
fn send(&self, data: &[u8], read: bool) -> Result<(), MongoErr>
~~~

### Method `recv`

~~~ {.rust}
fn recv(&self, buf: &mut ~[u8], read: bool) -> Result<uint, MongoErr>
~~~

### Method `set_timeout`

~~~ {.rust}
fn set_timeout(&self, timeout: u64) -> u64
~~~

### Method `get_timeout`

~~~ {.rust}
fn get_timeout(&self) -> u64
~~~

## Implementation of `::std::clone::Clone` for `NodeData`

Automatically derived.

### Method `clone`

~~~ {.rust}
fn clone(&self) -> NodeData
~~~

## Implementation of `Ord` for `NodeData`

### Method `lt`

~~~ {.rust}
fn lt(&self, other: &NodeData) -> bool
~~~

### Method `le`

~~~ {.rust}
fn le(&self, other: &NodeData) -> bool
~~~

### Method `gt`

~~~ {.rust}
fn gt(&self, other: &NodeData) -> bool
~~~

### Method `ge`

~~~ {.rust}
fn ge(&self, other: &NodeData) -> bool
~~~

## Implementation of `Eq` for `NodeData`

### Method `eq`

~~~ {.rust}
fn eq(&self, other: &NodeData) -> bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, other: &NodeData) -> bool
~~~

## Implementation for `NodeData`

### Method `new`

~~~ {.rust}
fn new(ip: ~str, port: uint, typ: ServerType, ping: Option<u64>,
       tagset: Option<TagSet>) -> NodeData
~~~

## Implementation of `Eq` for `ReplicaSetData`

### Method `eq`

~~~ {.rust}
fn eq(&self, other: &ReplicaSetData) -> bool
~~~

### Method `ne`

~~~ {.rust}
fn ne(&self, other: &ReplicaSetData) -> bool
~~~

## Implementation for `ReplicaSetConnection`

### Method `new`

~~~ {.rust}
fn new(seed: &[(~str, uint)]) -> ReplicaSetConnection
~~~

### Method `refresh`

~~~ {.rust}
fn refresh(&self) -> Result<(), MongoErr>
~~~

Refreshes replica set connection data.

Refreshes by fishing out latest state sent from reconnection task
to main task and checking if the write_to or read_from servers
need to be updated (due to primaries/secondaries, their tags, or the
read preference having changed). Connection must be connected
while calling refresh.

Called before every send or recv.

#### Returns

() on success, MongoErr on failure

