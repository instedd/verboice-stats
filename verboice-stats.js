// web.js
var express = require("express");
var logfmt = require("logfmt");
var http = require('http');
var fs = require('fs');
var AWS = require('aws-sdk'); 
var async = require('async');
var app = express();

var bucket = 'verboice-stats.instedd.org';
var localStatsPath = './stats.json';

AWS.config.update({region: 'us-west-1'});

var s3 = new AWS.S3(); 

app.use(logfmt.requestLogger());

downloadStats();


app.get('/', function(req, res) {
  res.send(compileStats());
});

var server = app.listen(3000, function() {
    console.log('Listening on port %d', server.address().port);
});


function downloadStats(){

	s3.listObjects({Bucket: bucket}, function(err, data){
		if (err) 
			console.log(err, err.stack); 
  		else
  		{
  			var remote_files = [];
  			remote_files.push(data.Contents);
  			console.log("Found " + remote_files[0].length + " files in bucket");

  			//todo: check for data.isTruncated

			var writer = fs.createWriteStream(localStatsPath, {flags: 'w', encoding: 'utf8'});
  			
  			//todo: batch in chunks
  			async.map(remote_files[0], getFile, function (err, result) {
  				if(!err) 
  				{
			    	writer.end(JSON.stringify(result));
			    	console.log('Finished');
			  	} 
			  	else 
			  	{
			    	console.log('Error: ' + err);
			  	}
			});
  		}
  	});
}

function getFile(remote_file, callback) {
	var key = remote_file.Key;
	console.log("fetching: " + key);

	s3.getObject({Bucket: bucket, Key: key}, function(err, data){
		if (!err)
		{
			var stat = JSON.parse(data.Body);
			console.log("adding data for: " + stat.timestamp);
			callback(null, stat);
		}
  		else
  		{	
  			console.log(err, err.stack); 
  			callback(err, null);
		}
	});
}

function compileStats()
{
	var rawStatsFile = fs.readFileSync(localStatsPath,'utf8');

	var rawStats = JSON.parse(rawStatsFile);

	var lastDate = new Date("January 1, 2000");
	var totalCalls = 0; //todo: replace with initial number of calls

	for (var i = rawStats.length - 1; i >= 0; i--) 
	{
		var statDate = new Date(rawStats[i].timestamp);
		console.log("compiling stats from " + statDate);

		if(statDate > lastDate)
			lastDate = statDate;

		totalCalls += rawStats[i].call_count;
	};

	var result = totalCalls + " calls made until " + lastDate; 
	console.log(result);

	return result;

}

