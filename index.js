'use strict';
var debug = require('debug')('mobile-memcached::cache');
var asyncu = require('async-utils');

var NOT_READY_ERROR = 'the client is not ready';
var STATUS = {
	CONNECTING: 'connecting',
	READY: 'ready'
};

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
	
	// if (options.namespace) {
	// 	var patchMemcached = require('cls-memcached');
	// 	patchMemcached(options.namespace);
	// }

	if (options === undefined) {
		options = {
			'server_name': 'default',
			'servers': ['localhost:11211']
		};
	}
	
	var memcached = require('memcached');
	self.jsdog = require('jsdog-meli').configure();

	// FIXME : Remapear opciones (y agregar las disponibles para memcached)
	self.client_options = options;
	self.client_options.client_timeout = options.client_timeout || 50;//timeout to assume connection is gone
	self.client_options.socket_keepalive = options.socket_keepalive || true;//self documented
	self.client_options.enable_offline_queue = options.enable_offline_queue || true;//whether if the client should queue commands while offline
	self.client_options.no_ready_check = options.no_ready_check || false;//wait for a info command response to check if the server is ready
	self.client_options.socket_nodelay = options.socket_nodelay || true;//disables nagle's algorithm
	
	debug('creating memcached client with options: %j', self.client_options);
	
	self.memcached_client = new memcached(self.client_options.servers);
	self.server_name = self.client_options.name;
	// self.memcached_client.connect()
	// self.memcached_client.on('ready', function() {
	// 	debug('server %s connected and ready', self.server_name);
	// 	self.connection_status = STATUS.READY;
	// })
	self.connection_status = STATUS.READY;
	debug('server %s connected and ready', self.server_name);

	self.get = function(key, callback) {
		var self = this;
		var start = new Date();
		
		debug('getting key %s from server %s', key, self.server_name);

		self.memcached_client.get(key, asyncu.fuse(self.client_options.client_timeout, 
			handle_callback(self.memcached_client, function(error, value) {
			
			var total = new Date() - start;
			var opResult = error && 'fail' || 'success';

			self.jsdog.recordCompoundMetric('application.mobile.api.cache.time', total, [
				'result:'+ opResult,
				'method:get',
				'cache:' + self.client_options.name,
				'server:' + self.server_name
			]);
			
			if (error) {
				debug('error while getting key %s. %j',key, error);
				return callback(error);
			}
			
			var result = value ? 'hit' : 'miss';
			self.jsdog.recordCompoundMetric('application.mobile.api.cache.result', 1, [
				'result:' + result,
				'method:get',
				'cache:' + self.client_options.name,
				'server:' + self.server_name
			]);
			
			debug('successful get key %s [result: %s] value: %s', key, result, value);

			return callback(undefined, JSON.parse(value));
		})));

	};

	self.del = function(key, callback) {
		var self = this;

		if (!self.connection_status === STATUS.READY) {
			return callback(NOT_READY_ERROR);
		}
		
		var start = new Date();

		debug('removing key %s from server %s', key, self.server_name);
		
		self.memcached_client.del(key, asyncu.fuse(self.client_options.client_timeout,
			handle_callback(self.memcached_client, function(error, value) {
			
			var total = new Date() - start;
			var opResult = error && 'fail' || 'success';

			self.jsdog.recordCompoundMetric('application.mobile.api.cache.time', total, [
				'result:' + opResult,
				'method:remove',
				'cache:' + self.client_options.name,
				'server:' + self.server_name
			]);

			if (error) {
				return callback(error);
			}

			return callback();
		})));
	};

	self.set = function(key, value, ttl, callback) {
		var self = this;
		if (!self.connection_status === STATUS.READY) {
			return callback(NOT_READY_ERROR);
		}

		var start = new Date();

		if (typeof ttl === 'function') {
			callback = ttl;
			ttl = 0;
		}

		debug('setting key %s in server %s', key, self.server_name);
		self.memcached_client.set(key, JSON.stringify(value), ttl, asyncu.fuse(self.client_options.client_timeout,
			handle_callback(self.memcached_client, function(error) {
			var total = new Date() - start;
			var opResult = error && 'fail' || 'success';

			self.jsdog.recordCompoundMetric('application.mobile.api.cache.time', total, [
				'result:' + opResult,
				'method:set',
				'cache:' + self.client_options.name,
				'server:' + self.server_name
			]);

			if (error) {
				return callback(error);
			}

			return callback();
		})));

	};

	//retrocompatibility
	self.remove = self.del;

	self.quit = function() {
		this.memcached_client.end()
	};

	self.on = function(event, listener) {
		var self = this;
		self.memcached_client.on(event, function() {
			var args = Array.prototype.slice.call(arguments).concat(self.server_name);
			listener.apply(undefined, args);
		});
	};

	self.on('remove', function(details) {
		debug('removing the server from our consistent hashing: %j', details);
	}

	self.on('failure', function(error, server) {
		debug('error: %j', error);
		self.memcached_client.reconnect();
	});

	self.on('reconnecting', function(details) {
		debug("Total downtime caused by server %s : %s ms", details.server, details.totalDownTime);
	});

	return self;
}