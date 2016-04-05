var http = require('http');
var MeliCache = require('../index.js');

var cache;
function createCache() {
	console.log('creating a new cache');
	var conf = {
		'type': 'remote',
		'name': 'items',
		'servers': ['127.0.0.1:11211']
	};

	cache = new MeliCache(conf);
}

createCache();

var hostname = '127.0.0.1';
var port = 1337;

http.createServer(function(req, res) {
	cache.get('tincho', function(error, value) {
		if (error) {
			res.writeHead(200, {
				'Content-Type': 'text/plain'
			});
			res.end(error.toString());
		} else {
			res.writeHead(200, {
				'Content-Type': 'application/json'
			});
			res.end(JSON.stringify(value));
		}
	});
}).listen(port, hostname, function() {
	console.log('Server running at http://%s:%s/', hostname, port);
});