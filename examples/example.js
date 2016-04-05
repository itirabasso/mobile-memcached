var MeliCache = require('../index.js');

var conf = {
	'type': 'remote',
	'name': 'items',
	'servers': ['127.0.0.1:11211']
};

var cache = new MeliCache(conf);

setTimeout(function() {
	cache.set('tincho', 'gonzalez', 10, function(error, response) {
		if (error) {
			console.log('error en el set');
			return console.log(error);
		}

		console.log('set ok');
		
		cache.get('tincho', function(error, response) {
			if (error) {
				console.log('error en el get');
				return console.log(error);
			}
			
			console.log('get ok');	
			
			console.log(response);
		});
	});
}, 100);
