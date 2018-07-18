let request = require('request');
module.exports.getCurveData = function (idCurve, token, callback) {
    let options = {
        method: "POST",
        url: "http://dev.sflow.me/project/well/dataset/curve/getData",
        headers:
        {
            Authorization: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6ImhvYW5nIiwicGFzc3dvcmQiOiJjNGNhNDIzOGEwYjkyMzgyMGRjYzUwOWE2Zjc1ODQ5YiIsIndob2FtaSI6Im1haW4tc2VydmljZSIsImlhdCI6MTUyMzg1Nzc1NSwiZXhwIjoxNTI0MDMwNTU1fQ.nWIBIv6IrykE4K3yB-XCnPYtwkiHEyCrGt9o4BBmcPI",
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