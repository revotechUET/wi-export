'use strict';
let crypto = require('crypto');
const LEN = 8;

function createPath(basePath, hashString, fileName) {
	//console.log("HASHSTRING : " + hashString);
	let md5sum = crypto.createHash('md5');
	md5sum.update(hashString);
	let hash = md5sum.digest('hex');
	let dirs = [];

	while (hash.length > 0) {
		hash = createDirSync(basePath, hash, dirs);
	}
	return basePath + '/' + dirs.join('/') + '/' + fileName;
}

function getHashPath(hashString) {
	let md5sum = crypto.createHash('md5');
	md5sum.update(hashString);
	let hash = md5sum.digest('hex');
	let dirs = [];

	while (hash.length > 0) {
		dirs.push(hash.substr(0, LEN));
		hash = hash.substr(LEN);
	}
	return dirs.join('/') + '/';
}


module.exports.getHashPath = getHashPath;
module.exports.createPath = createPath;

