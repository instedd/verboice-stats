// web.js
var logfmt = require("logfmt");
var http = require('http');
var fs = require('fs');
var AWS = require('aws-sdk'); 
var async = require('async');

var bucket = 'verboice-stats.instedd.org';
var localStatsPath = './stats.json';

AWS.config.update({region: 'us-east-1'});

var s3 = new AWS.S3(); 

downloadStats();

function downloadStats(){

	s3.listObjects({Bucket: bucket}, function(err, data){
		if (err) 
			return console.log(err, err.stack); 
  		
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

