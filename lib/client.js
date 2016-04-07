var debug = require('debug')('mobile-memcached::client');
var asyncu = require('async-utils');

var NOT_READY_ERROR = 'the client is not ready';
var STATUS = {
	CONNECTING: 'connecting',
	READY: 'ready'
};

var Client = module.exports = function Client(clientOptions) {
	var self = this;
	
	if (!(self instanceof Client)) return new Client(clientOptions);

	clientOptions = clientOptions || {};

	self.server_name = 'unkwnown';
	
	//defaults
	clientOptions.client_timeout = clientOptions.client_timeout || 50;//timeout to assume connection is gone
	clientOptions.socket_keepalive = clientOptions.socket_keepalive || true;//self documented
	clientOptions.enable_offline_queue = clientOptions.enable_offline_queue || true;//whether if the client should queue commands while offline
	clientOptions.no_ready_check = clientOptions.no_ready_check || false;//wait for a info command response to check if the server is ready
	clientOptions.socket_nodelay = clientOptions.socket_nodelay || true;//disables nagle's algorithm
	
	self.client_options = clientOptions;

	self.connect();
}

Client.prototype.connect = function() {
	var self = this;
	var memcached = require('memcached');
	self.jsdog = require('jsdog-meli').configure();

	var clientOptions = self.client_options;
	
	debug('creating memcached client with options: %j', clientOptions);
	
	// FIXME : Add server name
	self.memcached_client = new memcached(clientOptions.host + ":" + clientOptions.port);
	self.server_name = 'unkwnown';
	self.connection_status = STATUS.CONNECTING;

	self.memcached_client.on('ready', function() {
		debug('server %s connected and ready', self.server_name);
		self.connection_status = STATUS.READY;
	})
	debug('server %s connected and ready', self.server_name);

}

Client.prototype.get = function(key, callback) {
	var self = this;

	if (!self.connection_status === STATUS.READY) {
		return callback(NOT_READY_ERROR);
	}

	var start = new Date();
	
	debug('getting key %s from server %s', key, self.server_name);
	
	self.memcached_client.get(key, asyncu.fuse(self.client_options.client_timeout, function(error, value) {
		
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
	}));
}

Client.prototype.del = function(key, callback) {
	var self = this;

	if (!self.connection_status === STATUS.READY) {
		return callback(NOT_READY_ERROR);
	}
	
	var start = new Date();

	debug('removing key %s from server %s', name, key, self.server_name);
	
	self.memcached_client.del(key, asyncu.fuse(self.client_options.client_timeout, function(error, value) {
		
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
	}));
};

//retrocompatibility
Client.prototype.remove = Client.prototype.del;

Client.prototype.set = function(key, value, ttl, callback) {
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
	self.memcached_client.set(key, JSON.stringify(value), ttl, asyncu.fuse(self.client_options.client_timeout, function(error) {
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
	}));
};

Client.prototype.expire = function(key, ttl, callback) {
	var self = this;
	if (!self.connection_status === STATUS.READY) {
		return callback(NOT_READY_ERROR);
	}
	debug('setting expire for key %s in server %s', key, self.server_name);
	self.memcached_client.touch(key, ttl, asyncu.fuse(self.client_options.client_timeout, callback));
};

Client.prototype.quit = function() {
	debug('quit client %s', this.server_name);
	require('jsdog-meli').stop();
	this.memcached_client.end();
};

Client.prototype.reconnect = function() {
	var self = this;
	if (self.connection_status === STATUS.CONNECTING) {
		debug('already trying to reconnect');
		return;
	}
	debug('reconnect client %s', this.server_name);
	this.memcached_client.quit();
	this.ready = false;
	this.connect();
};

Client.prototype.on = function(event, listener) {
	var self = this;
	self.memcached_client.on(event, function() {
		var args = Array.prototype.slice.call(arguments).concat(self.server_name);
		listener.apply(undefined, args);
	});
};
