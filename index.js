var redis = require('redis');
var scoreRange = require('./lib/redis-scripts/score-range-to-hash.js');
var zahd = require('./lib/redis-scripts/zadd-hdel.js');
var hshd = require('./lib/redis-scripts/hset-hdel.js');
var hmz = require('./lib/redis-scripts/hmovez.js');
var queryDomain = require('getaaaaarr');
var equal = require('./lib/arrset.js').equal;

module.exports = function dnsmonctor(cfg, cb) {
  // The function to process newly-retrieved records with (if any).
  var process = cfg.process;

  var db = redis.createClient(cfg.redis.port, cfg.redis.hostname,
    {no_ready_check: true});
  if (cfg.redis.password) db.auth(cfg.redis.password);

  // Callback wrapper for final callbacks so as to not leak retvals
  function next(err){if (err) return cb(err); else cb();}

  // Handle the record response from a query
  function finishQuerying(domain, err, records) {

    // Expire these records in 5 minutes
    // TODO: Actually get the record TTLs when querying-
    // right now we CBA to do that due to the shortcomings of dns.resolve
    // and various issues involved in implementing our own resolver
    // using native-dns
    var expiration = Date.now() + 300000;
    records = records.map(function(record){record.ttl = 300; return record});

    // If we process new records
    if (process) {

      // Set the new records and get the old ones
      db.getset('records:' + domain, JSON.stringify(records),
        function(err, oldrecords) { if (err) return cb(err);

        // If the old ones aren't the same as the new ones
        if (!equal(records,JSON.parse(oldrecords))) {

          // Mark this domain as processing
          db.eval(hshd,2,'processing_domains','querying_domains',
            domain, expiration, function(err, res) {
              if (err) return cb(err);

              // Process the new records
              process(domain,records,finishProcessing.bind(null, domain));
            });
        // If the old ones were the same,
        // just advance as if we weren't processing
        } else completeQuery();
      });
    } else {
      db.set('records:' + domain, JSON.stringify(records), completeQuery);
    }

    // Mark that we've set the records and finish
    function completeQuery(err, res) {
      if (err) return cb(err);
      db.eval(zahd,2,'expiring_domains','querying_domains',
        expiration, domain, next);
    }

    // Mark that we've finished processing records
    function finishProcessing(domain, err) {
      if (err) return cb(err);
      db.eval(hmz,2,'processing_domains','querying_domains', domain, next);
    }
  }

  return function() {
    // Get all the domains whose records expired some time before now
    db.eval(scoreRange,2,'expiring_domains','querying_domains',
      '-inf', Date.now(), function (err, res) {
        // Query each of these domains
        for (var i = 1; i < res.length; i += 2) {
          queryDomain(res[i], finishQuerying.bind(null, res[i]));
        }
      });
  };
};
