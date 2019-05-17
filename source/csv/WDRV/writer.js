let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
var csv = require('fast-csv');
let space = require('../../space');
const MDCurve = '__MD';
let hashDir = require('../../hash-dir');
let _unitTable = null;
let _wellUnit;

module.exports.setUnitTable = setUnitTable;
function setUnitTable (unitTable) {
    _unitTable = unitTable;
}


function convertUnit(value, fromUnit, desUnit) {
    //todo
    if(!_unitTable) return value;
    let unitTable = _unitTable;
    let fromRate = unitTable[fromUnit];
    let desRate = unitTable[desUnit];
    if (fromRate && desRate) {
        
        return value * desRate / fromRate;
    }
    return value;
}

function writeHeader(csvStream, well, idCurves) {
	console.log(idCurves);
    console.log('---------idCurves', idCurves);
    //let headerArr = ['$Csv :WELL ', 'Dataset'];
    //headerArr.push(well.name)
    //csvStream.write(headerArr);
    //csvStream.write([]);

    let columnArr = ['WELL', 'Dataset', 'Depth'];
    let unitArr = ['.', '.', _wellUnit || 'M'];
    async.eachOfSeries(well.datasets, function (dataset, index, nextDataset) {
        // async.eachOfSeries(dataset.curves, function (curve, idx, nextCurve) {
        //     if(idCurves.find(function (id) {return id == curve.idCurve}) && curve.name != MDCurve) {
        //         console.log('curve', curve.name, curve.unit);
				// if (curve.type === 'ARRAY') {
					// for (let i = 0; i < curve.dimension; i++) {
						// columnArr.push(normalizeName(curve.name) + '_' + i);
						// unitArr.push(curve.unit);
					// }
				// } else{
					// columnArr.push(normalizeName(curve.name));
					// unitArr.push(curve.unit);
				// } 
        //         // columnArr.push(normalizeName(curve.name));
        //         // unitArr.push(curve.unit);
        //     }
        //     nextCurve();
        // }, function (err) {
        //     nextDataset();
        // })
        async.eachOfSeries(idCurves, function (idCurve, idx, nextCurve) {
			let curve = dataset.curves.find(function (curve) {return curve.idCurve == idCurve});
            if(curve && curve.name != MDCurve) {
                console.log('curve', curve.name, curve.unit);
				if (curve.type === 'ARRAY') {
					for (let i = 0; i < curve.dimension; i++) {
						columnArr.push(normalizeName(curve.name) + '_' + i);
						unitArr.push(curve.unit);
					}
				} else{
					columnArr.push(normalizeName(curve.name));
					unitArr.push(curve.unit);
				} 
                // columnArr.push(normalizeName(curve.name));
                // unitArr.push(curve.unit);
            }
            nextCurve();
        }, function (err) {
            nextDataset();
        })
    }, function (err) {
        if (err) {
            console.log('writeHeader err', err);
        }
        csvStream.write(columnArr);
        csvStream.write(unitArr);
    })
}

function normalizeName(name) {
    let newName = name.replace(/[&\/\\#,+()$~%.'":*?<>{}\|]+/g,' ')
                        .trim()
                        .replace(/\s+/g,'_');
    return newName;
}

async function writeDataset(csvStream, writeStream, project, well, dataset, idCurves, numOfPreCurve, s3, curveModel, curveBasePath, callback) {

    let fromUnit = dataset.unit || 'M';
    if(project) {
        fromUnit = 'M';
    }
    let top = Number.parseFloat(convertUnit(Number.parseFloat(dataset.top), fromUnit, _wellUnit).toFixed(4));
    let bottom = Number.parseFloat(convertUnit(Number.parseFloat(dataset.bottom), fromUnit, _wellUnit).toFixed(4));
    let step = Number.parseFloat(convertUnit(Number.parseFloat(dataset.step), fromUnit, _wellUnit).toFixed(4));
    let readStreams = [];

	console.log(idCurves);
    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) { return curve.idCurve == idCurve });
        if (curve && curve.name != MDCurve) {
            let stream;
            if (!project) { //export from inventory
                let curvePath = await curveModel.getCurveKey(curve.curve_revisions[0]);
                console.log('curvePath=========', curvePath);
                try {
                    stream = await s3.getData(curvePath);
					// stream = await fs.createReadStream('/mnt/B2C64575C6453ABD/well-insight/wi-online-inventory/wi-inventory-data/' + curvePath);
                } catch (e) {
                    console.log('=============NOT FOUND CURVE FROM S3', e);
                    callback(e);
                }
            } else { //export from project
                let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
                stream = fs.createReadStream(curvePath);
            }
            stream = byline.createStream(stream).pause();
			readStreams.push({
				stream: stream,
				curveName: normalizeName(curve.name),
				type: curve.type
			});
        }
    }

    if (readStreams.length === 0 || idCurves.length == 0) {
        console.log('hiuhiu');
        if(step == 0) {
            callback();
        } else
            for (let i = top; i < bottom + step; i += step) {
                let lineArr = generateLineArr(well.name, dataset.name, i.toFixed(4), numOfPreCurve);
                csvStream.write(lineArr);
                if (i >= bottom) {
                    callback();
                }
            }
    } else {
        readStreams[0].stream.resume();
        let lineArr;
        for (let i = 0; i < readStreams.length; i++) {
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].stream.on('data', function (line) {
                readLine++;
                let tokens = line.toString('utf8').split("||");
                let index = tokens.toString().substring(0, tokens.toString().indexOf(" "));
                tokens = tokens.toString().substring(tokens.toString().indexOf(" ") + 1).split(' ');
                let _ = require('lodash');
                // if (!_.isFinite(parseFloat(tokens))) {
                //     // let nullHeader = well.well_headers.find(header => {
                //     //     return header.header == "NULL";
                //     // })
                //     // tokens = nullHeader ? nullHeader.value :  '-999.0000';
                //     tokens = '-9999';
                // }
				if (readStreams[i].type != 'TEXT') {
					if (!_.isFinite(parseFloat(tokens[0]))) {
						tokens = ['-9999'];
					}
				} else {
					tokens = tokens.map((elt, i, arr) => {
						if (elt.includes('"')) {
							return elt.replace(/"/g, '');
						} else return elt;
					})
				}
                if (i === 0) {
                    let depth;
                    if (step == 0) depth = convertUnit(Number(index), 'M', _wellUnit).toFixed(4);
                    else depth = top.toFixed(4);
                    lineArr = generateLineArr(well.name, dataset.name, depth, numOfPreCurve);
                    top += step;
                }
                // lineArr.push(tokens);
				lineArr = [...lineArr, ...tokens];
                if (i !== readStreams.length - 1) {
                    readStreams[i].stream.pause();
                    if (readStreams[i + 1].stream.isPaused()) {
                        readStreams[i + 1].stream.resume();
                    }
                } else {
                    csvStream.write(lineArr, function () {
                        writeLine++;
                        if (readStreams.numLine && readStreams.numLine === writeLine) {
                            callback();
                        }
                    });
                    lineArr = generateLineArr(well.name, dataset.name, numOfPreCurve);
                    readStreams[i].stream.pause();
                    if (readStreams[0].stream.isPaused()) {
                        readStreams[0].stream.resume();
                    }
                }
            })
            readStreams[i].stream.on('end', function () {
                if (!readStreams.numLine) {
                    readStreams.numLine = readLine;
                }
                if (i == readStreams.length - 1 && readLine == 0) {
                    callback('No curve data');
                }
                console.log('END TIME', new Date(), readStreams.numLine);
                if (i != readStreams.length - 1) {
                    console.log('---', i, readStreams.length - 1);
                    readStreams[i + 1].stream.resume();
                }
                if (readStreams.numLine && readStreams.numLine === writeLine) {
                    callback();
                }
            })
        }
    }
}

function writeAll(exportPath, project, well, datasetObjs, username, s3, curveModel, curveBasePath, callback) {
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, s3, curveModel are null
    */
    console.log('WriteAll exportPath ----', exportPath);
    if (!well) { //export from inventory
        well = project.wells[0];
    }

    //create path if not existed
    if (!fs.existsSync(exportPath)) {
        fs.mkdirSync(exportPath);
    }
    let lasFilePath = path.join(exportPath, username);
    if (!fs.existsSync(lasFilePath)) {
        fs.mkdirSync(lasFilePath);
    }

    if(datasetObjs.length > 1) {
        _wellUnit = well.unit || 'M';
    } else {
        let dataset = well.datasets.find(function (dataset) { return dataset.idDataset == datasetObjs[0].idDataset;});
        _wellUnit = dataset.unit || 'M';
    }

    let fileName = well.name + "_" + Date.now() + '.csv'
    fileName = fileName.replace(/\//g, "-");
    lasFilePath = path.join(lasFilePath, fileName);

    var csvStream = csv.createWriteStream({ headers: false });
    let writeStream = fs.createWriteStream(lasFilePath, { flags: 'a' });
    let idCurvesArr = [];
    csvStream.pipe(writeStream);
    writeHeader(csvStream, well, getIdCurvesArr(datasetObjs));

    let numOfPreCurve = 0;
    async.eachOfSeries(datasetObjs, function (obj, index, next) {
        let dataset = well.datasets.find(function (dataset) { return dataset.idDataset == obj.idDataset; });
        writeDataset(csvStream, writeStream, project, well, dataset, obj.idCurves, numOfPreCurve, s3, curveModel, curveBasePath, function (e) {
            if (e) { console.log(e); }
            numOfPreCurve += obj.idCurves.length;
            next();
        });
    }, function (err) {
        if(err) {
            callback(err);
        } else {
            csvStream.end();
            writeStream.on('finish', function () {
                callback(null, {
                    wellName: well.name,
                    fileName: fileName
                })
            })
        }
    })
}

function generateLineArr(wellName, datasetName, depth, numOfPreCurve) {
    let arr = [];
    arr.push(wellName);
    arr.push(datasetName);
    arr.push(depth);
    for (let i = 0; i < numOfPreCurve; i++) {
        arr.push("");
    }
    return arr;
}

function getIdCurvesArr (datasetObjs) {
    let arr = [];
    async.eachOfSeries(datasetObjs, function (obj, index, next) {
        async.eachOfSeries(obj.idCurves, function(id, idx, nextId) {
            arr.push(id);
            nextId();
        }, function () {
            next();
        }, function () {
            console.log('ok');
        })
    })
    return arr;
}

module.exports.writeAll = writeAll;
