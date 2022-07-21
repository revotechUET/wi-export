let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
let space = require('../../space');
var csv = require('fast-csv');
const MDCurve = '__MD';
let hashDir = require('../../hash-dir');
let _unitTable = null;
const _ = require('lodash');

function checkInZoneDepths(depth, zoneDepthIntervals) {
    if (!zoneDepthIntervals.length) return true;
    for (const zoneDepth of zoneDepthIntervals) {
        if ((depth - zoneDepth.start) * (depth - zoneDepth.end) <= 0) {
            return true
        } 
    }
    return false
}

module.exports.setUnitTable = setUnitTable;
function setUnitTable(unitTable) {
    _unitTable = unitTable;
}


function convertUnit(value, fromUnit, desUnit) {
    //todo
    if (!_unitTable) return value;
    let unitTable = _unitTable;
    let fromRate = unitTable[fromUnit];
    let desRate = unitTable[desUnit];
    if (fromRate && desRate) {

        return value * desRate / fromRate;
    }
    return value;
}

function writeHeader(csvStream, well) {
    let headerArr = ['$Csv : ']
    headerArr.push(well.name)
    csvStream.write(headerArr);
    csvStream.write([]);
}

function normalizeName(name) {
    let newName = name.replace(/[&\/\\#,+()$~%.'":*?<>{}\|]+/g,' ')
                        .trim()
                        .replace(/\s+/g,'_');
    return newName;
}

async function writeCurve(lasFilePath, exportPath, fileName, project, well, dataset, idCurves, curveModel, curveBasePath, zoneDepthIntervals, callback) {
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, curveModel are null
    */
    let desUnit = dataset.unit || 'M';
    let fromUnit = dataset.unit || 'M';
    if (project) {  //export from project
        well = project.wells[0];
        well.username = project.createdBy;
        fromUnit = 'M';
    }

    let top = Number.parseFloat(convertUnit(Number.parseFloat(dataset.top), fromUnit, desUnit).toFixed(4));
    let bottom = Number.parseFloat(convertUnit(Number.parseFloat(dataset.bottom), fromUnit, desUnit).toFixed(4));
    let step = Number.parseFloat(convertUnit(Number.parseFloat(dataset.step), fromUnit, desUnit).toFixed(4));
    let readStreams = [];
    var csvStream = csv.createWriteStream({ headers: false });
    let writeStream = fs.createWriteStream(lasFilePath, { flags: 'a' });
    csvStream.pipe(writeStream);
    //writeHeader(csvStream, well);

    let curveNameArr = [];
    let curveUnitArr = [];
    curveNameArr.push('Depth');
    curveUnitArr.push(desUnit);

    console.log(zoneDepthIntervals);
    
    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) { return curve.idCurve == idCurve });
        if (curve && curve.name != MDCurve) {
            let stream;
            let unit = curve.curve_revisions ? curve.curve_revisions[0].unit : curve.unit;
			if (curve.type === 'ARRAY') {
				for (let i = 0; i < curve.dimension; i++) {
					curveNameArr.push(normalizeName(curve.name) + `[${i}]`);
					curveUnitArr.push(unit);
				}
			} else {
				curveNameArr.push(normalizeName(curve.name));
				curveUnitArr.push(unit);
			}
            if (!project) { //export from inventory
                try {
                    stream = await curveModel.getCurveData(curve);
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
        csvStream.write(curveNameArr);
        csvStream.write(curveUnitArr);
        if (step == 0) {
            callback(null, {
                fileName: fileName,
                wellName: well.name,
                datasetName: dataset.name
            })
        } else
            for (let i = top; i < bottom + step; i += step) {
                csvStream.write([i.toFixed(4)]);
                if (i >= bottom) {
                    callback(null, {
                        fileName: fileName,
                        wellName: well.name,
                        datasetName: dataset.name
                    })
                }
            }
    } else {
        readStreams[0].stream.resume();
        csvStream.write(curveNameArr);
        csvStream.write(curveUnitArr);
        let tokenArr = [];
        for (let i = 0; i < readStreams.length; i++) {
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].stream.on('data', function (line) {
                readLine++;
                let tokens = line.toString('utf8').split("||");
                tokens = tokens.toString().replace(/\s\s+/g, ' ');
                let index = tokens.substring(0, tokens.indexOf(" "));
                tokens = tokens.substring(tokens.indexOf(" ") + 1).split(' ');
                // if (!_.isFinite(parseFloat(tokens))) {
                //     // let nullHeader = well.well_headers.find(header => {
                //     //     return header.header == "NULL";
                //     // })
                //     // tokens = nullHeader ? nullHeader.value :  '-999.0000';
                //     tokens = '-9999';
                // }
				if (readStreams[i].type == 'NUMBER') {
                    tokens = [tokens.join(' ')];
					if (!_.isFinite(parseFloat(tokens[0]))) {
						tokens = ['-9999'];
					}
                } else if (readStreams[i].type == 'TEXT') {
                    tokens = [tokens.join(' ')];
                    if (tokens[0].includes('"')) {
						tokens[0] = tokens[0].replace(/"/g, '');
                    }
                } else {
					tokens = tokens.map((elt, i, arr) => {
						if (elt.includes('"')) {
							return elt.replace(/"/g, '');
						} else return elt;
					})
				}
                let depth;
                if (step == 0) {
                    depth = convertUnit(Number(index), 'M', desUnit);
                } else {
                    depth = top;
                }

                if(!checkInZoneDepths(depth, zoneDepthIntervals)) {
                    tokens = ['-9999'];
                }

                if (i == 0) {
                    tokenArr.push(depth.toFixed(4));
                }

                // tokenArr.push(tokens);
				tokenArr = [...tokenArr, ...tokens];
                if (i !== readStreams.length - 1) {
                    readStreams[i].stream.pause();
                    if (readStreams[i + 1].stream.isPaused()) {
                        readStreams[i + 1].stream.resume();
                    }
                } else {
                    top += step;
                    csvStream.write(tokenArr, function () {
                        writeLine++;
                        if (readStreams.numLine && readStreams.numLine === writeLine) {
                            csvStream.end();
                            // writeStream.on('finish', function () {
                            callback(null, {
                                fileName: fileName,
                                wellName: well.name,
                                datasetName: dataset.name
                            });
                            // })
                        }
                    });
                    tokenArr = [];
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
                    //export dataset with curves has no data
                    callback(null, {
                        fileName: fileName,
                        wellName: well.name,
                        datasetName: dataset.name
                    });
                }
                console.log('END TIME', new Date(), readStreams.numLine);
                if (i != readStreams.length - 1) {
                    console.log('---', i, readStreams.length - 1);
                    readStreams[i + 1].stream.resume();
                }
                if (readStreams.numLine && readStreams.numLine === writeLine) {
                    csvStream.end();
                    // writeStream.on('finish', function () {
                    callback(null, {
                        fileName: fileName,
                        wellName: well.name,
                        datasetName: dataset.name
                    });
                    // })
                }
            })
        }
    }
}

function writeAll(exportPath, project, well, idDataset, idCurves, username, curveModel, curveBasePath, zoneDepthIntervals, callback) {
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, curveModel are null
    */
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

    let dataset = well.datasets.find(function (dataset) { return dataset.idDataset == idDataset; });
    if (dataset) {
        let fileName = dataset.name + "_" + well.name + "_" + Date.now() + '.csv'
        fileName = fileName.replace(/\//g, "-");
        lasFilePath = path.join(lasFilePath, fileName);

        writeCurve(lasFilePath, exportPath, fileName, project, well, dataset, idCurves, curveModel, curveBasePath, zoneDepthIntervals, function (err, rs) {
            console.log('writeAll callback called', err, rs);
            if (err) {
                callback(err);
            } else {
                callback(null, rs);
            }
        });
        // }
    } else {
        console.log('no dataset');
        callback(null, null);
    }
}
module.exports.writeAll = writeAll;
