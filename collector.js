var
  async = require('async'),
  r = require('rethinkdb'),
  fs = require('fs'),
  request = require('request'),

  config = require('./config.json');

var last_timestamp = null;
try {
  last_timestamp = require('/opt/joola/joola.collect/last_timestamp.json').timestamp;
  last_timestamp = new Date(last_timestamp);
  console.log('Found last used timestamp, ' + last_timestamp.toISOString());
}
catch (ex) {
  last_timestamp = new Date();
  last_timestamp.setMonth(last_timestamp.getMonth() - 12);
}

var enddate = new Date();

r.connect(config.rethinkdb, function (err, conn) {
  if (err)
    throw err;
  console.log('Connected to RethinkDB');
  r.dbList().run(conn, function (err, list) {
    if (err)
      throw err;

    async.mapLimit(list, 3, function (item, callback) {
      var result = {
        timestamp: enddate,
        uid: null,
        reads: 0,
        writes: 0,
        simple: 0,
        total: 0
      };

      r.db(item).table('_metadata').pluck('uid_app').limit(1).run(conn, function (err, cursor) {
        if (err)
          return callback(err);

        cursor.each(function (a, row) {
          result.uid = row.uid_app;
        });

        r.db(item).table('_stats_reads').filter(function (row) {
          return row('timestamp').gt(r.ISO8601(last_timestamp.toISOString())).and(row('timestamp').le(r.ISO8601(enddate.toISOString())))
        }).sum('readCount').run(conn, function (err, results) {
          if (err && err.toString().indexOf('does not exist') === -1)
            return callback(err);
          if (results)
            result.reads = results;
          r.db(item).table('_stats_writes').filter(function (row) {
            return row('timestamp').gt(r.ISO8601(last_timestamp.toISOString())).and(row('timestamp').le(r.ISO8601(enddate.toISOString())))
          }).sum('writeCount').run(conn, function (err, results) {
            if (err && err.toString().indexOf('does not exist') === -1)
              return callback(err);
            if (results)
              result.writes = results;

            result.total = result.writes + result.reads + result.simple;
            if (result.total > 0 && result.uid !== 'joola-stats-223')
              return callback(null, result);
            else
              return callback(null);
          });
        });
      });
    }, function (err, results) {
      if (err)
        throw err;
      var _results = [];
      results.forEach(function (r) {
        if (r)
          _results.push(r);
      });
      results = _results;
      console.log('All done', results);
      r.db('joola_stats_223').table('workspace_app_usage').insert(results).run(conn, function (err) {
        if (err)
          throw err;
        console.log('Closing connection.');
        saveTimestamp(enddate);
        conn.close();

        async.mapLimit(results, 3, function (usage, callback) {
          var postOptions = {
            url: config.cnc.engine + '/api/applications/appusage?APIToken=' + config.cnc.apitoken,
            headers: {
              'Content-Type': 'application/json'
            },
            method: "POST",
            json: usage,
            rejectUnauthorized: false,
            requestCert: true,
            agent: false
          };

          request(postOptions, function (error, response, body) {
            //if (error || response.statusCode !== 200)
            //return callback(new Error('failed to post to cnc [' + response.statusCode + ']: ' + body));

            return callback(null);
          });
        }, function (err, results) {
          if (err)
            throw err;
          console.log('Done.');
        });
      });
    });
  });
});

function saveTimestamp(ts) {
  var outputFilename = '/opt/joola/joola.collect/last_timestamp.json';

  fs.writeFileSync(outputFilename, JSON.stringify({timestamp: ts}, null, 4));
  console.log("JSON saved to " + outputFilename);
}
