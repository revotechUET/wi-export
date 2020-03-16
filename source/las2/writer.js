let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
let space = require('../space');
let hashDir = require('../hash-dir');
const MDCurve = '__MD';
const WHLEN1 = 19;
const WHLEN2 = 10;
const WHLEN3 = 30;
let _unitTable = null;
const _ = require('lodash');
const NULL_VAL = "-9999";

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

function writeVersion(lasFilePath) {
    fs.appendFileSync(lasFilePath, '~Version\r\n');
    fs.appendFileSync(lasFilePath, 'VERS .        2      : CWLS LAS Version 2.0 \r\n');
    fs.appendFileSync(lasFilePath, 'WRAP .        NO     : One Line per Depth Step \r\n');
}

function getWellUnit(well) {
    // TODO
    let unitHeader = well.well_headers.find(function (header) {
        return header.header == 'UNIT';
    })
    return unitHeader.value;
}

function normalizeName(name) {
    let newName = name.replace(/[&\/\\#,+()$~%.'":*?<>{}\|]+/g,' ').trim().replace(/\s+/g,'_');
    return newName;
}

function writeWellHeader(lasFilePath, well, dataset, from) {
    let wellHeaders = well.well_headers;
    fs.appendFileSync(lasFilePath, '~Well\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, '#\r\n#PARAMETER_NAME    .UNIT     VALUE                         : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    //append start depth, stop depth and step
    let strtHeader;
    let stopHeader;
    let stepHeader;
    let wellUnit = dataset.unit || getWellUnit(well) || 'M';

    strtHeader = space.spaceAfter(WHLEN1, 'STRT') + space.spaceAfter(WHLEN2, '.' + wellUnit);
    let topValue = from == 'inventory' ? dataset.top : convertUnit(Number.parseFloat(dataset.top), 'M', wellUnit);
    strtHeader += space.spaceAfter(WHLEN3, Number.parseFloat(topValue).toFixed(4)) + ": Top Depth";

    stopHeader = space.spaceAfter(WHLEN1, 'STOP') + space.spaceAfter(WHLEN2, '.' + wellUnit);
    let bottomValue = from == 'inventory' ? dataset.bottom : convertUnit(Number.parseFloat(dataset.bottom), 'M', wellUnit);
    stopHeader += space.spaceAfter(WHLEN3, Number.parseFloat(bottomValue).toFixed(4)) + ": Bottom Depth";

    stepHeader = space.spaceAfter(WHLEN1, 'STEP') + space.spaceAfter(WHLEN2, '.' + wellUnit);
    let stepValue = from == 'inventory' ? dataset.step : convertUnit(Number.parseFloat(dataset.step), 'M', wellUnit);
    stepHeader += space.spaceAfter(WHLEN3, Number.parseFloat(stepValue).toFixed(4)) + ": Step";

    totalHeader = space.spaceAfter(WHLEN1, 'TD') + space.spaceAfter(WHLEN2, '.' + wellUnit);
    let totalValue = Number.parseFloat(bottomValue) - Number.parseFloat(topValue);
    console.log('------', topValue, bottomValue, totalValue);
    totalHeader += space.spaceAfter(WHLEN3, Number.parseFloat(totalValue).toFixed(4)) + ": Total Depth";

    fs.appendFileSync(lasFilePath, strtHeader + '\r\n' + stopHeader + '\r\n' + stepHeader + '\r\n' + totalHeader + '\r\n');
    //append other headers

    let nullHeader = space.spaceAfter(WHLEN1, 'NULL') + space.spaceAfter(WHLEN2, '.') + space.spaceAfter(WHLEN3, '-9999') + ": \r\n";
    fs.appendFileSync(lasFilePath, nullHeader);

    // let WELL_header = wellHeaders.find(function (h) { return h.value == 'WELL' });
    // let NULL_header = wellHeaders.find(function (h) { return h.value == 'NULL' });
    // if (!WELL_header) {
    let wellHeader = space.spaceAfter(WHLEN1, 'WELL') + space.spaceAfter(WHLEN2, '.') + space.spaceAfter(WHLEN3, well.name) + ": " + 'WELL NAME' + '\r\n';
    fs.appendFileSync(lasFilePath, wellHeader);
    // }
    // if (!NULL_header) {
    //     let header = space.spaceAfter(14, " " + 'NULL' + '.') + space.spaceAfter(24, '-999.2500') + ": \r\n";
    //     fs.appendFileSync(lasFilePath, header);
    // }
    for (i in wellHeaders) {
        if (wellHeaders[i].header !== 'filename' && wellHeaders[i].header !== 'STRT' && wellHeaders[i].header !== 'STOP' && wellHeaders[i].header !== 'STEP' && wellHeaders[i].header !== 'NULL' && wellHeaders[i].header !== 'WELL'
        && (wellHeaders[i].value || wellHeaders[i].description ||  wellHeaders[i].unit)) {
            let header = space.spaceAfter(WHLEN1, wellHeaders[i].header.toString()) + space.spaceAfter(WHLEN2, '.' + wellHeaders[i].unit) + space.spaceAfter(WHLEN3, wellHeaders[i].value) + ": " + wellHeaders[i].description + '\r\n';
            fs.appendFileSync(lasFilePath, header);
        }
    }
}

async function writeCurve(lasFilePath, exportPath, fileName, project, well, dataset, idCurves, curveModel, curveBasePath, callback) {
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, curveModel are null
    */

    fs.appendFileSync(lasFilePath, '~Parameter\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, '#\r\n#PARAMETER_NAME    .UNIT     VALUE                         : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');

    fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, "SET") + space.spaceAfter(10, '.') + space.spaceAfter(WHLEN3, dataset.name) + ': \r\n');
    if (dataset.dataset_params) {
        for (param of dataset.dataset_params) {
            if(param.mnem != 'SET')
                fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, param.mnem) + space.spaceAfter(WHLEN2, '.' + param.unit) + space.spaceAfter(WHLEN3, param.value) + ": " + param.description + '\r\n');
        }
    }

    fs.appendFileSync(lasFilePath, '~Curve\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#LOGNAME           .UNIT     LOG_ID                        : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, 'DEPTH') + space.spaceAfter(WHLEN2 + WHLEN3, '.' + dataset.unit) + ':\r\n');


    if (!well) {  //export from project
        well = project.wells[0];
        well.username = project.createdBy;
    }

    let fromUnit = dataset.unit || 'M';
    if(project) fromUnit = 'M';

    let top = Number.parseFloat(convertUnit(Number.parseFloat(dataset.top), fromUnit, dataset.unit).toFixed(4));
    let bottom = Number.parseFloat(convertUnit(Number.parseFloat(dataset.bottom), fromUnit, dataset.unit).toFixed(4));
    let step = Number.parseFloat(convertUnit(Number.parseFloat(dataset.step), fromUnit, dataset.unit).toFixed(4));
    let readStreams = [];
    let writeStream = fs.createWriteStream(lasFilePath, { flags: 'a' });
    let curveColumns = '~A        DEPTH';

    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) {
            return curve.idCurve == idCurve
        });
        if (curve && curve.name != MDCurve) {
            const normalizedCurveName = normalizeName(curve.name);
            let stream;
            if (!project) { //export from inventory
                try {
                    stream = await curveModel.getCurveData(curve);
					// stream = await fs.createReadStream('/mnt/B2C64575C6453ABD/well-insight/wi-online-inventory/wi-inventory-data/' + curvePath);
                } catch (e) {
                    console.log('=============NOT FOUND CURVE FROM S3', e);
                    callback(e);
                }
				if (curve.type == 'ARRAY') {
					for (let i = 0; i < curve.dimension; i++) {
						fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, normalizedCurveName + `[${i}]`) + space.spaceAfter(WHLEN2 + WHLEN3, '.' + curve.curve_revisions[0].unit) + ': ' + curve.description + ' {F}\r\n');
					}
				} else {
					fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, normalizedCurveName) + space.spaceAfter(WHLEN2 + WHLEN3, '.' + curve.curve_revisions[0].unit) + ': ' + curve.description + (curve.type == "NUMBER" ? ' {F}':' {S}') + '\r\n');
				}
            } else { //export from project
                let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
                console.log('curvePath', curvePath);
                stream = fs.createReadStream(curvePath);
				if (curve.type == 'ARRAY') {
					for (let i = 0; i < curve.dimension; i++) {
						fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, normalizedCurveName + `[${i}]`) + space.spaceAfter(WHLEN2 + WHLEN3, '.' + curve.unit) + ': ' + curve.description + ' {F}\r\n');
					}
				} else {
					fs.appendFileSync(lasFilePath, space.spaceAfter(WHLEN1, normalizedCurveName) + space.spaceAfter(WHLEN2 + WHLEN3, '.' + curve.unit) + ': ' + curve.description + (curve.type == "NUMBER" ? ' {F}':' {S}') + '\r\n');
				}
            }
            stream = byline.createStream(stream).pause();
            readStreams.push({
                stream: stream,
                type: curve.type
            });

			if (idCurve == 0) {
                curveColumns += space.spaceBefore(15, normalizedCurveName);
			}
			else{
				if (curve.type == 'ARRAY') {
					for (let i = 0; i < curve.dimension; i++) {
						curveColumns += space.spaceBefore(18, normalizedCurveName + `[${i}]`);
					}
				} else {
					curveColumns += space.spaceBefore(18, normalizedCurveName);
				}
			}
        }
    }

    fs.appendFileSync(lasFilePath, curveColumns + '\r\n');
    // fs.appendFileSync(lasFilePath, curveColumns + '\r\n');

    //write curves
    if (readStreams.length === 0 || idCurves.length == 0) {
        if (step == 0) {
            callback(null, {
                fileName: fileName,
                wellName: well.name,
                datasetName: dataset.name
            })
        } else
            for (let i = top; i < bottom + step; i += step) {
                writeStream.write(space.spaceBefore(15, i.toFixed(2)) + '\r\n', function () {
                    if (i >= bottom) {
                        callback(null, {
                            fileName: fileName,
                            wellName: well.name,
                            datasetName: dataset.name
                        })
                    }
                });
            }
    } else {
        readStreams[0].stream.resume();
        for (let i = 0; i < readStreams.length; i++) {
            console.log('~', readStreams[i].stream.isPaused());
        }
        for (let i = 0; i < readStreams.length; i++) {
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].stream.on('data', function (line) {
                readLine++;
                line = line.toString().replace(/\s\s+/g, ' ');
                let index = line.substring(0, line.indexOf(" "));
                let tokens = line.substring(line.indexOf(" ") + 1).split(' ');
                if(readStreams[i].type == "NUMBER"){
                    if (!_.isFinite(parseFloat(tokens[0]))) {
                        // let nullHeader = well.well_headers.find(header => {
                        //     return header.header == "NULL";
                        // })
                        // tokens = nullHeader ? nullHeader.value :  '-999.0000';
                        tokens = NULL_VAL;
                    } else tokens = parseFloat(tokens).toFixed(4);
                }
				else if (readStreams[i].type == 'ARRAY') {
					tokens = tokens.map((elt) => {
						return space.spaceBefore(17, parseFloat(elt).toFixed(4)) + ' ';
					}).join('');
                }
                else {
					tokens = tokens.join(' ');
                    if(tokens == "null"){
                        tokens = NULL_VAL;
                    }
                    else if(!tokens.includes('"')){
                        tokens = '"' + tokens + '"';
                    }
                }
                tokens = space.spaceBefore(17, tokens) + ' ';
                if (i === 0) {
                    index = Number(index);
                    let depth, hasDepth;
                    if (step == 0 || hasDepth) {
                        depth = convertUnit(index, 'M', dataset.unit).toFixed(4);
                        hasDepth = true;
                    } else {
                        depth = top.toFixed(4);
                        top += step;
                    }

                    depth = space.spaceBefore(14, depth) + ' ';
                    tokens = depth + tokens;
                }
                if (i !== readStreams.length - 1) {
                    writeStream.write(tokens, function () {
                    })
                    readStreams[i].stream.pause();
                    if (readStreams[i + 1].stream.isPaused()) {
                        readStreams[i + 1].stream.resume();
                    }
                } else {
                    writeStream.write(tokens + '\r\n', function () {
                        writeLine++;
                        if (readStreams.numLine && readStreams.numLine === writeLine) {
                            callback(null, {
                                fileName: fileName,
                                wellName: well.name,
                                datasetName: dataset.name
                            });
                        }
                    });
                    readStreams[i].stream.pause();
                    if (readStreams[0].stream.isPaused()) {
                        readStreams[0].stream.resume();
                    }
                }
            })
            readStreams[i].stream.on('error', function (err) {
                console.log('-------', error);
                callback(err);
            })
            readStreams[i].stream.on('end', function () {
                if (!readStreams.numLine) {
                    readStreams.numLine = readLine;
                    console.log('numLine', readStreams.numLine);
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
            })
        }
    }
}

function writeAll(exportPath, project, well, idDataset, idCurves, username, curveModel, curveBasePath, callback) {
    /*export from inventory
        project, curveBasePath are null
    */

    /*export from project
        well, curveModel are null
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

    let dataset = well.datasets.find(function (dataset) {
        return dataset.idDataset == idDataset;
    });
    if (dataset) {
        let fileName = dataset.name + "_" + well.name + "_" + Date.now() + '.las'
        fileName = fileName.replace(/\//g, "-");
        lasFilePath = path.join(lasFilePath, fileName);
        let from = project ? 'project' : 'inventory';
        writeVersion(lasFilePath);
        writeWellHeader(lasFilePath, well, dataset, from);
        writeCurve(lasFilePath, exportPath, fileName, project, well, dataset, idCurves, curveModel, curveBasePath, function (err, rs) {
            console.log('writeAll callback called', rs);
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
