let async = require('async');
let las2Writer = require('./source/las2/writer');
let las3Writer = require('./source/las3/writer');
let csvWriter = require('./source/csv/writer');

//from inventory
module.exports.exportLas2FromInventory = function (well, datasetObjs, exportPath, s3, curveModel, username, callback) {
    async.map(datasetObjs, function (item, cb) {
        let idDataset = item.idDataset;
        let idCurves = item.idCurves;
        las2Writer.writeAll(exportPath, null, well, idDataset, idCurves, username, s3, curveModel, null, function(err, rs) {
            console.log('las2Writer.writeAll callback called');
            if(err) {
                cb(err);
            } else {
                cb(null, rs);
            }
        });
    }, function (err, result) {
        console.log('complete', result);
        if (err) {
            callback(err);
        } else {
            callback(null, result);
        }
    });
}
module.exports.exportLas3FromInventory = function (well, datasetObjs, exportPath, s3, curveModel, username, callback) {
    las3Writer.writeAll(exportPath, null, well, datasetObjs, username, s3, curveModel, null, callback);
}
module.exports.exportCsvFromInventory = function (well, datasetObjs, exportPath, s3, curveModel, username, callback) {
    async.map(datasetObjs, function (item, cb) {
        let idDataset = item.idDataset;
        let idCurves = item.idCurves;
        csvWriter.writeAll(exportPath, null, well, idDataset, idCurves, username, s3, curveModel, null, function(err, rs) {
            if(err) {
                cb(err);
            } else {
                cb(null, rs);
            }
        });
    }, function (err, result) {
        console.log('complete', result);
        if (err) {
            callback(err);
        } else {
            callback(null, result);
        }
    });
};


//from project
module.exports.exportLas2FromProject = function (project, datasetObjs, exportPath, curveBasePath, username, callback) {
    async.map(datasetObjs, function (item, cb) {
        let idDataset = item.idDataset;
        let idCurves = item.idCurves;
        las2Writer.writeAll(exportPath, project, null, idDataset, idCurves, username, null, null, curveBasePath, function(err, rs) {
            console.log('las2Writer.writeAll callback called');
            if(err) {
                cb(err);
            } else {
                cb(null, rs);
            }
        });
    }, function cb (err, result) {
        if (err) {
            callback(err);
        } else {
            console.log('complete', result);
            if (err) {
                callback(err);
            } else {
                callback(null, result);
            }
        }
    })
}
module.exports.exportLas3FromProject = function (project, datasetObjs, exportPath, curveBasePath, username, callback) {
    las3Writer.writeAll(exportPath, project, null, datasetObjs, username, null, null, curveBasePath, callback);
}
module.exports.exportCsvFromProject = function (project, datasetObjs, exportPath, curveBasePath, username, callback) {
    async.map(datasetObjs, function (item, cb) {
        let idDataset = item.idDataset;
        let idCurves = item.idCurves;
        csvWriter.writeAll(exportPath, project, null, idDataset, idCurves, username, null, null, curveBasePath, function(err, rs) {
            console.log('csvWriter.writeAll callback called');
            if(err) {
                cb(err);
            } else {
                cb(null, rs);
            }
        });
    }, function cb (err, result) {
        if (err) {
            callback(err);
        } else {
            console.log('complete', result);
            if (err) {
                callback(err);
            } else {
                callback(null, result);
            }
        }
    })
}