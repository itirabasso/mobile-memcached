'use strict';
var debug = require('debug')('mobile-memcached::cache');
var Client = require('./lib/client.js');

function createClient(server, options) {
	var clientOptions = options.clientOptions || {};
		
	var fields = server.split(/:/);
	
	clientOptions.host = fields[0];
	clientOptions.port = parseInt(fields[1], 10);
	clientOptions.name = options.name || 'default';

	return new Client(clientOptions);
}

function handle_callback(client, callback) {
	return function(error, response) {
		if (error) {
			debug('client is down %j, reconnecting name: %s', error, client.server_name);
			client.reconnect();
		}
		return callback(error, response);
	};
}

var MeliCache = module.exports = function MeliCache(options) {

	var self = {};
	var clients = {};
	
	// if (options.namespace) {
	// 	var patchRedis = require('cls-memcached');
	// 	patchMemcached(options.namespace);
	// }

	if (options === undefined) {
		options = {
			'type': 'remote',
			'name': 'items',
			'servers': ['localhost:11211']
		};
	}

	// options.servers.forEach(function(server) {
	// 	clients[server] = createClient(server, options);
	// });

	// var servers = {};
	// for (var key in clients) {
	// 	servers[key] = 1;
	// }

	// self.ring = new hashring(servers);
	
	var client = new Client(options);

	self.get = function(key, callback) {
		client.get(key, handle_callback(client, callback));
	};

	self.del = function(key, callback) {
		client.del(key, handle_callback(client, callback));
	};

	self.set = function(key, value, ttl, callback) {
		client.set(key, value, ttl, handle_callback(client, callback));
	};

	//retrocompatibility
	self.remove = self.del;

	self.quit = function() {
		client.end()
		// options.servers.forEach(function(server) {
		// 	clients[server].end();
		// });
	};

	self.on = function(event, listener) {
		client.on(event, listener);
		// options.servers.forEach(function(server) {
		// 	var client = clients[server];
		// 	client.on(event, listener);
		// });
	};

	self.on('error', function(error, server) {
		debug('error: %j', error);
		client.reconnect();
		// clients[server] && clients[server].reconnect();
	});

	return self;
}