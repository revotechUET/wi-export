let path = require('path');
let fs = require('fs');
let byline = require('byline');
let async = require('async');
let space = require('../space');
let hashDir = require('../hash-dir');
const MDCurve = '__MD';
let _unitTable = null;
let _wellTopDepth;
let _wellBottomDepth;
let _wellStep;
let _wellUnit;
let _ = require('lodash');
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
    fs.appendFileSync(lasFilePath, 'VERS  .             3.00                                : CWLS LOG ASCII STANDARD - VERSION 3.00\r\n');
    fs.appendFileSync(lasFilePath, 'WRAP  .             NO                                  : One line per depth step\r\n');
    fs.appendFileSync(lasFilePath, 'DLM   .             COMMA                               : Delimiter character between data columns\r\n');
    fs.appendFileSync(lasFilePath, '#Delimiting character ( SPACE TAB OR COMMA ).\r\n\r\n');
}

function writeWellHeader(lasFilePath, well) {
    let wellHeaders = well.well_headers;
    fs.appendFileSync(lasFilePath, '~Well\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, '#\r\n#PARAMETER_NAME    .UNIT     VALUE                         : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');

    //append start depth, stop depth and step
    let wellUnit = _wellUnit || well.unit || 'M';
    let strtHeader = space.spaceAfter(19, 'STRT') + space.spaceAfter(10, '.' + wellUnit);
    let stopHeader = space.spaceAfter(19, 'STOP') + space.spaceAfter(10, '.' + wellUnit);
    let stepHeader = space.spaceAfter(19, 'STEP') + space.spaceAfter(10, '.' + wellUnit);
    let totalHeader = space.spaceAfter(19, 'TD') + space.spaceAfter(10, '.' + wellUnit);
    strtHeader += space.spaceAfter(30, Number.parseFloat(_wellTopDepth).toFixed(4)) + ": Top Depth";
    stopHeader += space.spaceAfter(30, Number.parseFloat(_wellBottomDepth).toFixed(4)) + ": Bottom Depth";
    stepHeader += space.spaceAfter(30, Number.parseFloat(_wellStep).toFixed(4)) + ": Step";
    totalHeader += space.spaceAfter(30,
        (Number.parseFloat(_wellBottomDepth) -
            Number.parseFloat(_wellTopDepth)).toFixed(4)) + ": Total Depth";

    fs.appendFileSync(lasFilePath, strtHeader + '\r\n' + stopHeader + '\r\n' + stepHeader + '\r\n' + totalHeader + '\r\n');
    //append other headers
    let nullHeader = space.spaceAfter(19, 'NULL') + space.spaceAfter(10, '.') + space.spaceAfter(30, NULL_VAL) + ": NULL VALUE\r\n";
    fs.appendFileSync(lasFilePath, nullHeader);

    let wellHeader = space.spaceAfter(19, 'WELL') + space.spaceAfter(10, '.') + space.spaceAfter(30, well.name) + ": " + 'WELL NAME' + '\r\n';
    fs.appendFileSync(lasFilePath, wellHeader);

    for (i in wellHeaders) {
        if (wellHeaders[i].header !== 'filename' && wellHeaders[i].header !== 'COMPANY' && wellHeaders[i].header !== 'STRT' && wellHeaders[i].header !== 'STOP' && wellHeaders[i].header !== 'STEP' && wellHeaders[i].header != 'NULL' && wellHeaders[i].header != 'WELL'
        && (wellHeaders[i].value || wellHeaders[i].description ||  wellHeaders[i].unit)) {
            let header = space.spaceAfter(19, wellHeaders[i].header.toString()) + space.spaceAfter(10, '.' + wellHeaders[i].unit) + space.spaceAfter(30, wellHeaders[i].value) + ": " + wellHeaders[i].description + '\r\n';
            fs.appendFileSync(lasFilePath, header);
        }
    }
}
function normalizeName(name) {
    let newName = name.replace(/[&\/\\#,+()$~%.'":*?<>{}\|]+/g,' ').trim().replace(/\s+/g,'_');
    return newName;
}
async function writeDataset(lasFilePath, fileName, project, well, dataset, idCurves, curveModel, curveBasePath, zoneDepthIntervals, callback) {
    const lasDatasetName = dataset.name.toUpperCase().replace(/ /g,'.').replace(/_DATA|_PARAMETER|_DEFINITION/g,"");
    fs.appendFileSync(lasFilePath, '\r\n~' + lasDatasetName + '_PARAMETER\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, '#\r\n#PARAMETER_NAME    .UNIT     VALUE                         : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, space.spaceAfter(19, 'SET') + space.spaceAfter(10, '.') + space.spaceAfter(30, lasDatasetName) + ':\r\n');
    if (dataset.dataset_params && dataset.dataset_params.length > 0) {
        for (param of dataset.dataset_params) {
            if(param.mnem != 'SET') {
                let line = space.spaceAfter(19, param.mnem) + space.spaceAfter(10, '.' + param.unit) + space.spaceAfter(30, param.value) + ': ' + param.description + '\r\n';
                fs.appendFileSync(lasFilePath, line);
            }
        }
    }
    fs.appendFileSync(lasFilePath, '\r\n\r\n~' + lasDatasetName + '_DEFINITION\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#LOGNAME           .UNIT     LOG_ID                        : DESCRIPTION\r\n#\r\n');
    fs.appendFileSync(lasFilePath, '#_______________________________________________________________________________\r\n');
    fs.appendFileSync(lasFilePath, space.spaceAfter(19, 'DEPTH') + space.spaceAfter(40, '.' + dataset.unit || 'M')+ ': Depth    {F}\r\n');

    let desUnit = dataset.unit || 'M';
    let fromUnit = dataset.unit || 'M';
    if (project) {
        fromUnit = 'M';
    }
    let top = Number.parseFloat(convertUnit(Number.parseFloat(dataset.top), fromUnit, desUnit).toFixed(4));
    let bottom = Number.parseFloat(convertUnit(Number.parseFloat(dataset.bottom), fromUnit, desUnit).toFixed(4));
    let step = Number.parseFloat(convertUnit(Number.parseFloat(dataset.step), fromUnit, desUnit).toFixed(4));
    let readStreams = [];
    let writeStream = fs.createWriteStream(lasFilePath, {flags: 'a'})

    console.log(zoneDepthIntervals);
    
    let minStartDepth = 9999;
    for (const item of zoneDepthIntervals) {
        if (minStartDepth > Number(item.start)) {
            minStartDepth = Number.parseFloat(item.start);
        }
    }
    if (minStartDepth === 9999) {
        minStartDepth = 0;
    }

    let maxEndDepth = 0;
    for (const item of zoneDepthIntervals) {
        if (maxEndDepth < Number(item.end)) {
            maxEndDepth = Number.parseFloat(item.end);
        }
    }
    if (maxEndDepth === 0) {
        maxEndDepth = 9999;
    }

    console.log(`minStartDepth: ${minStartDepth}, maxEndDepth: ${maxEndDepth}`);
    
    for (idCurve of idCurves) {
        let curve = dataset.curves.find(function (curve) {
            return curve.idCurve == idCurve
        });
        let line = '';
        if (curve && curve.name != MDCurve) {
            let stream;
            let normalizedCurveName = normalizeName(curve.name);
            if (project) { //export from project
				if (curve.type == 'ARRAY') {
					for (let i = 0; i < curve.dimension; i++) {
						line += space.spaceAfter(19, normalizedCurveName + `[${i}]`) + space.spaceAfter(40, '.' + curve.unit) + ': ' + curve.description + ' {F}\r\n';
					}
				} else {
					line = space.spaceAfter(19, normalizedCurveName) + space.spaceAfter(40, '.' + curve.unit) + ': ' + curve.description + (curve.type == "NUMBER"? ' {F}' : ' {S}') + '\r\n';
				}
                let curvePath = await hashDir.createPath(curveBasePath, project.createdBy + project.name + well.name + dataset.name + curve.name, curve.name + '.txt');
                console.log('curvePath', curvePath);
                stream = fs.createReadStream(curvePath);
            } else { //export from inventory
				if (curve.type == 'ARRAY') {
					for (let i = 0; i < curve.dimension; i++) {
						line += space.spaceAfter(19, normalizedCurveName + `[${i}]`) + space.spaceAfter(40, '.' + curve.curve_revisions[0].unit) + ': ' + curve.description + ' {F}\r\n';
					}
				} else {
					line = space.spaceAfter(19, normalizedCurveName) + space.spaceAfter(40, '.' + curve.curve_revisions[0].unit) + ': ' + curve.description + (curve.type == "NUMBER"? ' {F}' : ' {S}') + '\r\n';
				}
                try {
                    stream = await curveModel.getCurveData(curve); 
                } catch (e) {
                    console.log('=============NOT FOUND CURVE FROM S3', e);
                    callback(e);
                }
            }
            stream = byline.createStream(stream).pause();
            readStreams.push({
                stream: stream,
                type: curve.type
            });
            fs.appendFileSync(lasFilePath, line);
        }
    }
    fs.appendFileSync(lasFilePath, '\r\n\r\n' + '~' + lasDatasetName + '_DATA | ' + lasDatasetName + '_DEFINITION\r\n');

    //writeCurves
    if (readStreams.length === 0 || idCurves.length == 0) {
        console.log('hiuhiu', top, bottom, step);
        if(step == 0) {
            callback(null, {
                fileName: fileName,
                wellName: well.name
            })
        } else 
            for (let i = top; i < bottom + step; i += step) {
                writeStream.write(space.spaceBefore(15, i.toFixed(2)) + '\r\n', function () {
                    if (i >= bottom) {
                        callback(null, {
                            fileName: fileName,
                            wellName: well.name
                        })
                    }
                });
            }
    } else {
        readStreams[0].stream.resume();
        let hasDepth;
        for (let i = 0; i < readStreams.length; i++) {
            if (i == 0 && readStreams[i].stream.isPaused()) {
                readStreams[i].stream.resume();
            }
            let readLine = 0;
            let writeLine = 0;
            readStreams[i].stream.on('data', function (line) {
                readLine++;
                line = line.toString().replace(/\s\s+/g, ' ');
                let index = line.substring(0, line.indexOf(" "));
                let tokens = line.substring(line.indexOf(" ") + 1).split(' ');
                // if (tokens == null || tokens == NaN || tokens.substring(0, 4) == 'null' || tokens == 'NaN' || !tokens) {
                if(readStreams[i].type == "NUMBER"){
                    if (!_.isFinite(parseFloat(tokens[0]))) {
                        tokens = NULL_VAL;
                    } 
                    else tokens = parseFloat(tokens).toFixed(4);
				} 
                else if (readStreams[i].type == 'ARRAY') {
					tokens = tokens.map((elt) => {
						return space.spaceBefore(14, parseFloat(elt).toFixed(4)) + ' ';
					}).join(',');
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
                tokens = space.spaceBefore(14, tokens) + ' ';
                index = Number(index);
                let depth;
                if (step == 0 || hasDepth) {
                    depth = convertUnit(index, 'M', desUnit);
                    hasDepth = true;
                } else {
                    depth = top;
                }

                if (depth < minStartDepth || depth > maxEndDepth) {
                    tokens = space.spaceBefore(14, NULL_VAL) + ' ';
                }
                if (i === 0) {
                    depth = space.spaceBefore(15, depth.toFixed(4) + ',') + ' ';
                    tokens = depth + tokens;
                }
                if (i !== readStreams.length - 1) {
                    tokens += ',';
                    writeStream.write(tokens, function () {
                    })
                    readStreams[i].stream.pause();
                    if (readStreams[i + 1].stream.isPaused()) {
                        readStreams[i + 1].stream.resume();
                    }
                } else {
                    top += step;
                    writeStream.write(tokens + '\r\n', function () {
                        writeLine++;
                        if (readStreams.numLine && readStreams.numLine === writeLine) {
                            console.log('number of line', readStreams.numLine, writeLine);
                            readLine = 0;
                            writeLine = 0;
                            readStreams.numLine = "";
                            callback(null, {
                                fileName: fileName,
                                wellName: well.name
                            });
                        }
                    });
                    readStreams[i].stream.pause();
                    if (readStreams[0].stream.isPaused()) {
                        readStreams[0].stream.resume();
                    }
                }
            })
            readStreams[i].stream.on('end', function () {
                console.log('end', i, readLine);
                if (!readStreams.numLine) {
                    readStreams.numLine = readLine;
                }
                if (i == readStreams.length - 1 && readLine == 0) {
                    //export dataset with curves has no data
                    callback(null, {
                        fileName: fileName,
                        wellName: well.name
                    });
                }
                console.log('END TIME', new Date(), readStreams.numLine);
                if (i != readStreams.length - 1) {
                    console.log('resume', i + 1);
                    readStreams[i + 1].stream.resume()
                }
            })
        }
    }
}

function writeAll(exportPath, project, well, datasetObjs, username, curveModel, curveBasePath, callback) {
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

    if(datasetObjs.length > 1) {
        _wellBottomDepth = convertUnit(getWellBottomDepth(well), 'M', well.unit);
        _wellTopDepth = convertUnit(getWellTopDepth(well), 'M', well.unit);
        _wellStep = convertUnit(getWellStep(well), 'M', well.unit);
        _wellUnit = well.unit;
    } else {
        let dataset = well.datasets.find(function (dataset) {
            return dataset.idDataset == datasetObjs[0].idDataset;
        });
        _wellBottomDepth = dataset.bottom;
        _wellTopDepth = dataset.top;
        _wellStep = dataset.step;
        _wellUnit = dataset.unit;
        if (project) {
            _wellTopDepth = convertUnit(_wellTopDepth, 'M', _wellUnit);
            _wellBottomDepth = convertUnit(_wellBottomDepth, 'M', _wellUnit);
            _wellStep = convertUnit(_wellStep, 'M', _wellUnit);
        }
    }
    
    let fileName = well.name + "_" + Date.now() + '.las'
    fileName = fileName.replace(/\//g, "-");
    lasFilePath = path.join(lasFilePath, fileName);
    writeVersion(lasFilePath);
    writeWellHeader(lasFilePath, well);
    async.mapSeries(datasetObjs, function (item, cb) {
        // console.log('callback', cb);
        // console.log('111111', item.idCurves);
        let dataset = well.datasets.find(function (dataset) {
            return dataset.idDataset == item.idDataset;
        });
        let zoneDepthIntervals = item.intervals || [];
        writeDataset(lasFilePath, fileName, project, well, dataset, item.idCurves, curveModel, curveBasePath, zoneDepthIntervals, cb);
    }, function cb(err, rs) {
        console.log('map series callback');
        if (err) {
            callback(err);
        } else {
            console.log('rs', rs);
            callback(null, rs[0]);
        }
    });
}

function getWellTopDepth(well) {
    let datasets = well.datasets;
    return Math.min(...datasets.map(d => +convertUnit(d.top, d.unit, 'M')));
}

function getWellBottomDepth(well) {
    let datasets = well.datasets;
    return Math.max(...datasets.map(d => +convertUnit(d.bottom, d.unit, 'M')));
}

function getWellStep(well) {
    let datasets = well.datasets;
    return datasets[0] ? convertUnit(datasets[0].step, datasets[0].unit, 'M') : 0;
}

module.exports.writeAll = writeAll;
