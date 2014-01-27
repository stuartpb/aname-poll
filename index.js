var scoreRange = require('./lib/redis-scripts/score-range-to-hash.js');
var zahd = require('./lib/redis-scripts/zadd-hdel.js');
var hshd = require('./lib/redis-scripts/hset-hdel.js');
var hmz = require('./lib/redis-scripts/hmovez.js');
var queryDomain = require('./lib/query-domain.js');

module.exports = function dnsmonctor(cfg, process) {
  var db = redis.createClient(cfg.redis.port, cfg.redis.hostname,
    {no_ready_check: true});
  if (cfg.redis.password) db.auth(cfg.redis.password);

  function finishProcessing(domain, err) {
    db.eval(hmz,2,'processing_domains','querying_domains', domain);
  }

  function finishQuerying(domain, err, records) {
    var expiration = Infinity;
    
    for (var i = 0; i < res.length; i ++) {
      expiration = Math.min(expiration, records[i].ttl * 1000);
    }

    //TODO: save records, check if modified

    if (process) {
      db.eval(hshd,2,'processing_domains','querying_domains',
        domain, expiration, function(err, res) {
          process(domain,records,finishProcessing);
        });
    } else {
      db.eval(zahd,2,'expiring_domains','querying_domains',
        expiration, domain);
    }
  }

  return function() {
    db.eval(scoreRange,2,'expiring_domains','querying_domains',
      '-inf', Date.now(), function (err, res) {
        for (var i = 1; i < res.length; i += 2) {
          queryDomain(res[i], finishQuerying.bind(null, res[i]));
        }
      });
  }
};
