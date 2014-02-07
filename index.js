var redis = require('redis');
var scoreRange = require('./lib/redis-scripts/score-range-to-hash.js')();
var zahd = require('./lib/redis-scripts/zadd-hdel.js')();
var hshd = require('./lib/redis-scripts/hset-hdel.js')();
var hmz = require('./lib/redis-scripts/hmovez.js')();
var queryDomain = require('getaaaaarr');
var equal = require('./lib/arrset.js').equal;

module.exports = function dnsmonctor(cfg, cb) {
  // The function to process newly-retrieved records with (if any).
  var process = cfg.process;
  // The function to process record retrival failures with (if any).
  var notfoundHandler = cfg.notfound;

  // TTL in seconds to set for records
  var ttl = cfg.ttl === 0 ? 0 : cfg.ttl || 300;

  var db = redis.createClient(cfg.redis.port, cfg.redis.hostname,
    {no_ready_check: true});
  if (cfg.redis.password) db.auth(cfg.redis.password);

  // Callback wrapper for final callbacks so as to not leak retvals
  function next(err){if (err) return cb(err); else cb();}

  // Handle the record response from a query
  function finishQuerying(domain, err, records) {
    // Set time until our next query
    var expiration = Date.now() + ttl * 1000;

    // If there was an error querying the domain
    if (err) {
      // If the domain was not found
      if (err.code == 'ENOTFOUND') {
        // If we have a function to handle missing domains
        if (notfoundHandler) {
          // Call it with a callback that lets it decide how to proceed
          notfoundHandler(domain, function(remove) {
            // Remove the domain from circulation if requested
            if (remove) db.hdel('querying_domains', domain, next);
            // Otherwise, keep it in circulation
            else completeQuery();
          });
        // If there's no function to handle missing domains
        } else {
          // Just return the domain to the query queue
          completeQuery();
        }
      // If it was some other kind of error
      } else {
        // Spit it up
        return cb(err);
      }
    }

    // Set TTLs on the records
    records = records.map(function(record){record.ttl = ttl; return record});

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
              process(domain,records,finishProcessing);
            });
        // If the old ones were the same,
        // just advance as if we weren't processing
        } else completeQuery();
      });
    } else {
      db.set('records:' + domain, JSON.stringify(records), completeQuery);
    }

    // Mark that we've set the records (if any) and finish
    function completeQuery(err) {
      if (err) return cb(err);
      db.eval(zahd,2,'expiring_domains','querying_domains',
        expiration, domain, next);
    }

    // Mark that we've finished processing records
    function finishProcessing(err) {
      if (err) return cb(err);
      db.eval(hmz,2,'processing_domains','querying_domains', domain, next);
    }
  }

  return function() {
    // Get all the domains whose records expired some time before now
    db.eval(scoreRange,2,'expiring_domains','querying_domains',
      '-inf', Date.now(), function (err, res) {
        if (err) return cb(err);
        // Query each of these domains
        for (var i = 1; i < res.length; i += 2) {
          queryDomain(res[i], finishQuerying.bind(null, res[i]));
        }
      });
  };
};
