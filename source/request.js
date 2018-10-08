let request = require('request');
module.exports.getCurveData = async function (idCurve, token, callback) {
    let options = {
        method: "POST",
        url: "http://dev.i2g.cloud/project/well/dataset/curve/getData",
        headers:
        {
            'Authorization': token,
            'Content-Type': 'application/json'
        },
        body: {
            idCurve: idCurve,
        },
        json: true
    };
    
  request(options, function (err, response, body){
      if(err){
          console.log('err');
          callback(err);
      } else {
          console.log('ok');
        callback(null, body);        
      }
  });
}
module.exports.getUnitArr = function (token, callback) {
    let options = {
        method: "POST",
        url: "http://dev.i2g.cloud/family/list-unit",
        headers:
        {
            'Authorization': token,
            'Content-Type': 'application/json'
        },
        body: {
            idUnitGroup: 11
        },
        json: true
    };
    
  request(options, function (err, response, body){
      if(err){
          console.log('err');
          callback(err);
      } else {
          console.log('ok');
        callback(null, body);        
      }
  });
}