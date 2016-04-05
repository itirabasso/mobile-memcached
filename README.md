# mobile-cache
Distributed cached sharded over redis. According Mobile team requisites.

##Features
* Bassed on redis
* Data sharding support
* CLS support
* building mock mode at TEST env. (See [redis-mock](https://github.com/faeldt/redis-mock))
 
## Usage
```javascript
var MobileCache = require('mobile-cache');
var cache = new MobileCache({servers:['127.0.0.1:6380']});
cache.on('error',function(error){
    console.log("Something whent wrong. "+error);
)};

cache.set(key,value[,TTL],callback);
cache.get(key,callback);
cache.remove(key,callback);
cache.quit();

```
## Configure
```javascript
{
 name: 'cache name',
 namespace: 'This is the namespace object from CLS',
 servers: ['127.0.0.1:6380','127.0.0.1:6381'....],
 clientOptions: 'for redis configuration, see [node_redis](https://github.com/mranney/node_redis#overloading)'
}
```
## Events
See [node_redis](https://github.com/mranney/node_redis#connection-events) documentacion.
